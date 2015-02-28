
import multiprocessing
from multiprocessing.managers import SyncManager
from threading import Timer

try:
    from setproctitle import setproctitle
except:
    def setproctitle(blah):
        pass
        
from subprocess import Popen, PIPE, STDOUT
from os import getpid
from random import randint
from uuid import uuid1 as uuid
from pprint import pformat
from types import GeneratorType
from copy import deepcopy, copy

import argparse
import sys
import traceback

import signal
from daemon import DaemonContext
from daemon.daemon import make_default_signal_map, set_signal_handlers
# from lockfile import FileLock
import gc

import logging

from slac_utils.queues import QueueFactory, Queue as StubQueue, END_MARKER
from slac_utils.statistics_feeder import CarbonFeeder, StatisticsMixin
from slac_utils.request import TaskRequest, LogMessage, JobStatusQueue, Task
from slac_utils.string import camel_case_to_underscore
from slac_utils.time import now, sleep, delta

from resource import getrusage, RUSAGE_SELF

# from sys import getsizeof
# from pympler import asizeof
#from guppy import hpy

TASK_RECEIVED = 0
TASK_PROCESSING = 1
TASK_INFO = 2
TASK_COMPLETE = 5
TASK_WARNING = -3
TASK_ERRORED = -5


class NoActionRequired(Exception):
    pass
    
class ResourceLocked(Exception):
    pass

class TimedOut(Exception):
    pass


    
class Worker( multiprocessing.Process ):
    """
    abstraction of a process to have a work queue and results queue
    also supports yield results for multiple out tasks
    worker thread that also adds information on statistics and logging
    """   
    action = 'Doing'

    work_queue_func = None
    work_queue = None
    results_queue_func = None
    results_queue = None
    logging_queue_func = None
    logging_queue = None

    pool = 'default'
    keys = ('#',)
    work_exchange_name = None
    results_exchange_name = None

    working = True
    global_working = None # semaphore
    max_tasks = 0
    done_tasks = 0
    prefetch_tasks = None
        
    proc_name = None
    # name = None
    
    log = None
    
    # how many iterations before doing a garbage colleciton call
    garbage_collect_freq = 10
    memory_multiply_threshold = 4 # if the mem footprint increased this much, then quit worker
    min_mem_footprint = None
    
    # caching
    post_msg_data = {}
    
    # module statistics
    stats = {}
    print_stats_fields = ( 'total_time', )
    report_stats_locally = False
    
    def __init__( self, queue_factory_obj, 
            work_queue_func='work', results_queue_func=None, logging_queue_func=None, 
            pool='default', keys=('#',),
            work_exchange_name=None,
            results_exchange_name=None,
            max_tasks=0, prefetch_tasks=None, working_semaphore=None,
            **kwargs ):

        # if no queue object, don't create multiprocess thread
        if not queue_factory_obj == None:
            # logging.warn("+ worker %s multiprocessing start" % (self))
            multiprocessing.Process.__init__(self)        
            # logging.warn("+ worker %s multiprocessing done" % (self))
            # create queues
            self.queue_factory_obj = queue_factory_obj
            self.work_queue_func = work_queue_func
            self.results_queue_func = results_queue_func
            self.logging_queue_func = logging_queue_func
            # logging.warn("+ worker %s qeues done" % (self))

        self.kwargs = kwargs
        # work
        self.pool = pool
        self.keys = keys
        if max_tasks:
            self.max_tasks = max_tasks
        if prefetch_tasks:
            self.prefetch_tasks = prefetch_tasks
        # results
        self.work_exchange_name = work_exchange_name
        self.results_exchange_name = results_exchange_name

        # semaphore to signal whether we should continue working
        self.working = True
        self.global_working = working_semaphore

        self.log = None
        self.stats = {}
        
        if queue_factory_obj and self.proc_name:
            setproctitle( self.proc_name )

        logging.info("created %s worker: queues %s (ex %s) -> %s (ex %s), log %s, using %s %s, max_tasks %s" % (self, self.work_queue_func, self.work_exchange_name, self.results_queue_func, self.results_exchange_name, self.logging_queue_func, self.pool, self.keys, self.max_tasks ) )

        # testing
        # self.h = hpy()


        
    def setup( self, **kwargs ):
        # set up the queues for input/output and loggin
        self.connection = self.queue_factory_obj.get_connection( **kwargs )
        logging.debug(" using pooled message broker connection: %s" % (self.connection))

        self.work_queue = getattr( self.queue_factory_obj, self.work_queue_func )( connection=self.connection, exchange_name=self.work_exchange_name, pool=self.pool, keys=self.keys )
        logging.debug("  work queue: %s" % (self.work_queue))
        
        self.results_queue = getattr( self.queue_factory_obj, self.results_queue_func )( connection=self.connection, exchange_name=self.results_exchange_name ) \
            if not self.results_queue_func == None else StubQueue()
        logging.debug("  results queue: %s" % (self.results_queue))

        self.logging_queue = getattr( self.queue_factory_obj, self.logging_queue_func )( connection=self.connection ) \
            if not self.logging_queue_func == None else StubQueue()
        logging.debug("  logging queue: %s" % (self.logging_queue))
        self.logger = JobStatusQueue( self.logging_queue, self.log )
    
    # def __del__(self):
    #     pass  
    
    def run( self ):
        """
        this worker will listen on the work queue and '_run' forever for each task that comes in
        """
        self.done_tasks = 0
        try:
            if self.working:
                self.setup( **self.kwargs )
                with self.work_queue:
                    with self.results_queue:
                        with self.logging_queue:
                            # logging.error("%s consume from %s" % (self,self.work_queue) )
                            if self.working:
                                self.work_queue.consume( self._run_loop, limit=self.max_tasks, prefetch=self.prefetch_tasks )
                            # logging.error('%s end consume' % (self,))

        except KeyboardInterrupt, e:
            logging.debug("terminating...")
        except IOError, e:
            logging.error("Connection error %s: %s" % (self.work_queue,e,))
        except NoActionRequired, e:
            pass
        except Exception, e:
            logging.error( "CONSUME ERROR with %s: (%s) %s\n%s" % (self, type(e), e, traceback.format_exc()) )
            
        logging.debug("%s exiting" % (self,))
    
    def pre_msg_process( self, msg, envelope ):
        return msg
    
    def post_msg_process( self ):
        yield None, None
        
    def _run_loop( self, some_msg, envelope, ack_early=True ):
        """
        wrapper function to call _run() on contents of some_msg multiple times if an array
        otherwise just pass it through
        """
        # logging.error('='*80)
        self.post_msg_data = {}
        
        if not envelope:
            raise Exception("error with envelope 1: %s (%s)" % (envelope,msg))
        
        # don't forget to ack in this loop rather than in the _run() loop, use ack_early=None to do this
        stuff = some_msg if isinstance( some_msg, list ) else [ some_msg, ]
        # logging.error( "%s: %s -> %s" % (isinstance( some_msg, list ),type(some_msg),type(stuff) ) )

        # if there are many items in stuff, then we do not want to ack within the _run() loop, but outside. set var ack to None in this case
        ack = ack_early
        bulk = False
        if len(stuff) > 1:
            if ack_early and envelope:
                self.work_queue.task_done( envelope )
            ack = None
            bulk = True
        
        ts = {}
        # pre_msg_process
        ts['start'] = now()
        stuff = self.pre_msg_process( stuff, envelope )
        ts['pre'] = now()

        # run for each datum
        for msg in stuff:
            self._run( msg, envelope, ack_early=ack, bulk=bulk )
        ts['loop'] = now()

        # post_msg_process: finish the bulk results
        for key, results in self.post_msg_process():
            # logging.error("BULK %s: %s" % (key, results))
            if not self.results_queue == None:
                self._enqueue_result( results, key=key )
        ts['end'] = now()

        if ack == None:
            if ack_early == False and envelope:
                self.work_queue.task_done( envelope )

        if bulk:
            self.stats['pre_msg_process_time'] = delta(ts['start'],ts['pre'])
            self.stats['main_loop_time'] = delta(ts['pre'],ts['loop'])
            self.stats['post_msg_process_time'] = delta(ts['loop'],ts['end'])
            self.stats['total_time'] = delta(ts['start'],ts['end'])
            self._report_state( self.log, error=[] )
            self.log = None
        
        # check to make sure we're not signaled to stop working
        # logging.warn("WHISTLE: %s" % (self.global_working))
        if self.global_working:
            try:
                if not self.global_working.is_set():
                    self.working = False
                    raise NoActionRequired('end of work day')
            except:
                pass

        # terminate if we're too tired
        if self.done_tasks > self.garbage_collect_freq:
            self.done_tasks = 0
            gc.collect()
            # if the task is too high utilisation, kill it
            mem = getrusage(RUSAGE_SELF).ru_maxrss 
            # mem = getsizeof(self.__dict__)
            # mem = asizeof.asizeof(self)
            # logging.debug("mem: %s (min %s)" % (mem,self.min_mem_footprint))
            # http://www.smira.ru/wp-content/uploads/2011/08/heapy.html
            # logging.error("%s" % self.h.heap().byrcs[0].referrers.byrcs[0].byvia )
            # logging.error("%s" % self.h.heap().get_rp() )
            if self.min_mem_footprint == None or mem < self.min_mem_footprint:
                self.min_mem_footprint = mem
            # logging.debug("mem: %s * %s" % (self.min_mem_footprint,self.memory_multiply_threshold))
            # exit out
            if self.memory_multiply_threshold == None:
                self.memory_multiply_threshold = 4 # what?!
            if mem > self.min_mem_footprint * self.memory_multiply_threshold:
                # logging.warn("exiting mem: %s > %s" % ( mem,self.min_mem_footprint * self.memory_multiply_threshold  ) )
                self.working = False
                raise Exception('memory footprint too large')
    
    def _run( self, msg, envelope, ack_early=True, bulk=False ):
        """
        actually do the work as defined in method process()
        deal with generator returned objects
        """
        
        if ack_early and envelope:
            self.work_queue.task_done( envelope )

        start_time = now()
        try:

            # logging.warn("WORKER IN %s: recv %s" % (self.__class__.__name__,msg) )
            if msg == END_MARKER:
                raise SyntaxError, "received early termination signal"

            # do statistics

            # dispatch by message type
            # logging.error("0> %s:\t%s" %(self,msg))
            error = None
            if '_meta' in msg and 'context' in msg:
                # create a log message
                self.log = LogMessage( meta=deepcopy(msg['_meta']), context=deepcopy(msg['context']) )

            else:
                self.log = LogMessage()

            t = msg['type'] if 'type' in msg else 'task'
            if t == 'task':

                # tell someone that we're working on it
                self.log._meta['state'] = TASK_PROCESSING
                self.log._meta['from'] = str(self)
                self.logger.info('processing', self.log)
                # do something!
                res = self.process_task( msg, stats=self.stats )
                # logging.error("1> (%s) %s" %(type(res),res,))

                # put results into queue
                if isinstance( res, GeneratorType ):
                    # group by the key defined for each message (so that we can keep the routing keys)
                    out = {}
                    for r in res:
                        if r:
                            # logging.error("R: %s %s" % (type(r),r))
                            key = r._meta['key'] if '_meta' in r and 'key' in r._meta else ''
                            # logging.error("KEY: %s" % (key,))
                            if not key in out:
                                out[key] = []
                            out[key].append(r)
                    for key, array in out.iteritems():
                        # logging.error("OUT: %s\t%s" % (key,len(array)))
                        self._enqueue_result( array, key=key )
                else:
                    self._enqueue_result( res )

            # don't send results of logging to the results queue
            elif t == 'log':
                self.process_log( msg )
                raise NoActionRequired, 'received log message'

            # hmm...
            else:
                raise Exception, 'unknown message type received: %s' % (msg)

            # good...
            self.log._meta['state'] = TASK_COMPLETE

        except NoActionRequired,e:
            pass
        except Exception, err:
            error = err
            t = traceback.format_exc()
            # logging.error("EXCEPTION: (%s) %s\t%s" % ( type(err), err, msg ) )
            # logging.error("EXCEPTION: (%s) %s" % ( type(err), err ) )
            # logging.error("  STACK: " + str(t))
            self.log['_meta']['from'] = str(self)
            self.log['_meta']['state'] = TASK_ERRORED
            self.log['_meta']['stack_trace'] = str(t)

            # TODO: if there is a problem, we don't really want to mark the task as done...
            # so shoudl we just return?
        finally:
            self.post_task( msg )
            if ack_early == False and envelope:
                self.work_queue.task_done( envelope )
            if not envelope:
                logging.error("error with envelope 2: %s" % (envelope,))
            self.done_tasks = self.done_tasks + 1
                
        # report statistics
        self.stats['total_time'] = delta( start_time,now() )
        
        if bulk == False:
            self._report_state( self.log, error=error )
            self.log = None

    def _report_state( self, log, error=[] ):
        log.stats = self.stats
        stats_string = self._print_statistics(self.stats,fields=self.print_stats_fields)
        info = False
        try:
            if log['_meta']['state'] == TASK_COMPLETE:
                info = True
        except:
            pass
        if info:
            self.logger.info( "completed (%s)" % (stats_string,), log, local_too=self.report_stats_locally )
        else:
            self.logger.error( "%s (%s)" % (error,stats_string), log, local_too=self.report_stats_locally )

    def _enqueue_result( self, results, key='' ):
        # put the results into the results queue with the defined key
        # if bulk_enqueue, then we delay sending all the messages until much later
        # for messages that are arrays, this effectively batches up all the result into one output
        if not self.results_queue == None and not results == None:
            if key == '' and '_meta' in results and 'key' in results._meta:
                key = results._meta['key']
            # logging.error("ENQUEUE: %s\t%s" % (,key,results) )
            self.results_queue.put( results, key=key )
                
    def qsize(self):
        logging.debug('asking work queue size')
        return self.work_queue.qsize()

    def _print_statistics( self, stats_dict, fields=('total_time',), decimals=2, join_char=', ' ):
        out = []
        flt = '%.{0}f'.format(decimals)
        for k in fields:
            if k in stats_dict:
                v = stats_dict[k]
                val = v
                try:
                    v = float(v)
                    if v.is_integer():
                        val = '%s' % (int(v))
                    else:
                        val = flt % float(v)
                    if k.endswith('_time'):
                        k = k.replace('_time','')
                        val = val + 's'
                except:
                    pass
                out.append( '%s %s' % (k,val) )
        return join_char.join(out)
    
    def _parse_statistics( self, line, d={} ):
        for f in line.split(r' '):
            g = f.split('=')
            if len(g) == 2:
                d[g[0]] = g[1]
        return d

    def process_task( self, msg, stats={}, **kwargs ):
        raise NotImplementedError, 'process task'
        
    def process_log( self, msg, stats={} ):
        pass

    def post_task( self, msg ):
        pass


class Supervisor( Worker ):
    """
    a supervisor is a worker that listens for jobs coming in. these jobs are then:
    - checked for content
    -- if it's a log message, then process_log() is called
    -- if it's a job
    --- validate_task() is called on the job, and can modify the contents of the job
    ---- if it doesn't raise an exception, then the job is passed onto the supervisor_queue where the workers await
    ---- if validate task does raise an exception, then process_invalid_task() is called to handle any cleanup etc.
    """
    action = 'supervise'

    def prep_task( self, job ):
        pass
    def cleanup_task( self, job ):
        pass
        
    def process_log( self, job ):

        # logging.info("LOG: %s" % (job,))
        if 'state' in job._meta:
            if job._meta['state'] == TASK_COMPLETE:
                self.cleanup_task( job )

        return True
    
    def pre_process_task( self, job, **opts ):
        return True
        
    def process_task( self, job, **opts ):
        """
        do the relevant checks to ensure that this job is valid
        if it is then send to worker team for processing
        """
        try:
            if not self.pre_process_task( job, **opts ):
                return None
            
            # create task object
            for i in ( 'context', 'data', '_meta' ):
                if not i in job:
                    raise DelegatedException, "task request is invalid (no '%s' defined)" % i
            job = Task( meta=job['_meta'], context=job['context'], data=job['data'] )

            for i in ( 'user', 'id' ):
                if not i in job:
                    raise Exception, 'field %s not defined' % i
                # if not job[i]:
                #     raise Exception, 'field %s value is invalid on %s' % (i,job)
            
            job = self.validate_task( job )
            if job:
                self.prep_task( job )
                # clear the state for the next worker
                # logging.error("JOB: %s" % (job,))
                if 'state' in job['_meta']:
                    del job['_meta']['state'] 
                return job

        except Exception, e:
            # logging.error("TRACE %s"%(traceback.format_exc(),))
            self.process_invalid_task( job, e )
            raise e

    def process_invalid_task( self, job, exception ):
        """ what to do if the task is invalid """
        job['_meta']['state'] = TASK_ERRORED
        job['_meta']['reason'] = '(%s) %s' % (type(exception), exception)
        return
    
    def validate_task( self, job ):
        """
        check the job for consistency etc. raise Exception if there is a problem, this will call process_invalid_task()
        """
        return job

class Manager( object ):
    """
    dyanamically handles a pool of Process objects based on the work queue size
    symnopsis
      m = Manager()
      m.setup()
      m.start() #will block until finished

    """
    queue_factory = QueueFactory
    queue_factory_obj = None
    
    work_queue_func = None
    results_queue_func = None
    logging_queue_func = None # to give to supervisor
    
    work_queue = None
    results_queue = None
    logging_queue = None

    pool = 'default'
    keys = ('#',)
    
    worker = Worker
    workers = []
    max_tasks_per_worker = 0
    min_workers = 1
    max_workers = 0

    working = True
    monitor_period = 5
    new_worker_at = 0

    args = None
    options = None

    # list of field names of kwargs for creation of workers  (from init)
    default_worker_kwargs = [ 'pool', 'keys' ]
    worker_kwargs = []

    proc_name = None

    def __init__( self, *args, **kwargs ):

        self.kwargs = kwargs

        # workers
        self.workers = []
    
        if not 'pool' in kwargs:
            kwargs['pool'] = None
    
        # logging.error("INIT MANAGER KWARGS: %s" % (kwargs,))
        for k,v in kwargs.iteritems():
            try:
                h = getattr(self, k)
                if h:
                    # logging.debug(' setting %s = %s' % (k,v))
                    setattr(self, k, v)
            except:
                pass
                
        # routing key for queue that workers should be listening on
        if kwargs['pool'] == None:
            self.pool = str(uuid())
            kwargs['pool'] = self.pool
            logging.debug(" setting pool %s" % (self.pool,))
        logging.debug("creating factory with %s" % (kwargs,))
        self.queue_factory_obj = self.queue_factory( **kwargs )


    def setup( self, **options ):
        if self.proc_name:
            setproctitle( self.proc_name )
    
    def loop_start( self, **options ):
        pass
        
    def loop_end( self, **options ):
        pass
    
    def start( self, *args, **options ):
        
        logging.info( 'Starting %s: pool=%s, keys=%s, with %s to %s workers' % (self.__class__.__name__, self.pool, self.keys, self.min_workers, self.max_workers) )

        options = dict( self.kwargs.items() + options.items() )
        self.setup( **options )

        self.working = True
        while self.working:
            
            try:
                
                self.loop_start( **options )

                # cleanup dead workers
                # logging.debug("workers: %s" % (self.workers))
                for n, p in enumerate( self.workers ):
                    if not p.is_alive():
                        # logging.warn( ' reaping worker ' + str(p) + ', n=' + str(n) )
                        p.terminate()
                        self.workers.pop( n )

                c = len( self.workers )
                # logging.debug("= workers %s (%s->%s)" % (c,self.min_workers,self.max_workers) )

                # too few workers, create more
                if not self.min_workers == 0:
                    needed_workers = self.min_workers - c
                    if not self.max_workers == None and self.max_workers > 0 and c > self.max_workers: needed_workers = 0
                    # logging.error("= workers +%s (min %s, current %s)" % (needed_workers,self.min_workers, c) )
                    if needed_workers > 0:
                        for i in xrange( needed_workers ):
                            try:
                                self.start_worker( **options )
                                if self.proc_name:
                                    setproctitle( self.proc_name )
                            except Exception,e:
                                logging.error("Error starting worker: %s %s with %s"%(type(e),e, options))
                        # logging.warn("= created all workers")
                # determine length of queue by asking random worker
                # n = randint(0,c-1)
                # logging.debug("n: " + str(n))
                #q = self.workers[n].qsize()
                #logging.warn("worker " + str(n) + " says qsize is " + str(q) )
                # # if too many, then start new child upto max_children
                # if not q == None:
                #     if q >= self.new_child_at and c < self.max_workers:
                #         self.create_worker( **options )
                #     # if queue is zero, then terminate a random child
                #     elif q == 0:
                #         p = self.workers.pop( n )
                #         logging.debug("terminating worker " + str(p) + ", n=" + str(n))
                #         p.terminate()

                self.loop_end( **options )
                sleep( self.monitor_period )
                
            except Exception, e:
                logging.error('Fatal Error: %s %s\n%s' % (type(e), str(e), traceback.format_exc()) )
                self.working = False
        
            if self.proc_name:
                setproctitle( self.proc_name )
        
        # clean up workers
        self.terminate()
        return

    def terminate(self, *args, **kwargs):
        self.working = False
        self.terminate_workers()
        return

    def reload(self,*args,**kwargs):
        pass

    def start_worker( self, *args, **kwargs ):
        # logging.warn( '= worker, creating %s' % (kwargs) )
        w = self.create_worker( self.queue_factory_obj, **kwargs )
        w.daemon = False #True
        # logging.warn( '= worker, starting' )
        w.start()
        w.join( 0.2 )
        self.workers.append( w )
        # logging.warn( '= worker adding %s' % (w) )

    def create_worker(self, queue_factory, **kwargs):
        opts = {}
        for k in ( self.default_worker_kwargs + self.worker_kwargs ):
            opts[k] = kwargs[k]
        # logging.warn( '= worker, creating %s' % (opts,) )
        return self.worker( self.queue_factory_obj, 
            work_queue_func=self.work_queue_func, results_queue_func=self.results_queue_func, logging_queue_func=self.logging_queue_func, 
            max_tasks=self.max_tasks_per_worker, 
            **opts )
        
    def terminate_workers( self ):
        logging.info('terminating workers: %s' % (self.workers,))
        # poison pill workers
        while len(self.workers):
            w = self.workers[0]
            if w.is_alive():
                logging.info(" killing worker %s" % (w,))
                w.terminate()
                w.join()
            else:
                self.workers.pop(0)
        logging.info('terminated')

    def restart_workers( self, *args, **options ):
        self.terminate_workers()
        for i in xrange(self.min_workers):
            self.start_worker( *args, **options )
            

class WorkerManager( Manager ):
    """
    a manager that does something; typically a single thing like a worker, but does not need the management
    """
    queue_factory = None
    
    work_queue = None
    results_queue = None
    logging_queue = None

    working = False

    proc_name = None

    def __init__( self, *args, **kwargs ):
        
        # multiprocessing.Process.__init__(self)                
        Manager.__init__(self, *args, **kwargs)
        
        for i in ( 'work_queue_func', 'results_queue_func', 'logging_queue_func' ):
            if not i in kwargs:
                if not getattr( self, i ):
                    setattr( self, i, None )
            else:
                setattr( self, i, kwargs[i] )
        
        logging.debug("creating %s working manager: %s -> %s, %s" %(self.__class__.__name__, self.work_queue_func, self.results_queue_func, kwargs))
        self.kwargs = kwargs

        self.queue_factory_obj = self.queue_factory( **kwargs )
        logging.debug("  factory: %s" % (self.queue_factory_obj,) )
        
        if self.proc_name:
            setproctitle( self.proc_name )
        
    def setup( self, **kwargs ):
        # TODO: where does pool and key come from?
        self.work_queue = getattr( self.queue_factory_obj, self.work_queue_func )( pool=self.pool, keys=self.keys ) \
            if not self.work_queue_func == None else StubQueue()
        logging.debug("  work queue: %s" % (self.work_queue))
        self.results_queue = getattr( self.queue_factory_obj, self.results_queue_func )( auto_delete=True ) \
            if not self.results_queue_func == None else StubQueue()
        logging.debug("  results queue: %s" % (self.results_queue))

        self.logging_queue = getattr( self.queue_factory_obj, self.logging_queue_func )( auto_delete=True ) \
            if not self.logging_queue_func == None else StubQueue()
        logging.debug("  logging queue: %s" % (self.logging_queue))
        self.logger = JobStatusQueue( self.logging_queue )
        

    def run( self ):
        return self.start()
    
    def start( self, *args, **options ):
        options = dict( self.kwargs.items() + options.items() )
        self.setup( **options )
        self.working = True
        while self.working:
            logging.info( 'starting %s' % (self.__class__.__name__,) ) #+ ': pool=' + str(self.pool) + ", key=" + str(self.key) )
            try:
                with self.work_queue:
                    with self.results_queue:
                        with self.logging_queue:
                            self.loop_start( **options )
                            self.process_task( {} )
                            self.loop_end( **options )
            except Exception, e:
                logging.error('error (%s) %s: %s' %(type(e),e, traceback.format_exc() ) )
                sleep(5)
        # self.cleanup()
        return
        # 
        # def cleanup( self ):
        #     pass
    

class FanOutManager( Manager ):
    """
    manager class that delegates job validation and delegation to a single supervisor to manage worker colony
    work_queue_func is the main incoming job queue that the supervisor worker will listen on
    the supervisor shall then validate_task() and put validated jobs into the supervisor_queue_func queue
    workers all listen in on this supervisor_queue_func as their work queue
    when workers finish, they should put results into the results_queue_func queue
    if a logging_queue_func is defined, then workers will communicate to this queue with updates - typically
    this should be a queue to the work_queue_func so that the supervisor is notified.
    the workers should communicate only to their affiliated supervisor, as such, we use an unqiue uuid pool for this workers
    """
    worker = Worker

    work_queue_func = 'work'
    supervisor_queue_func = 'supervised'
    results_queue_func = 'results'
    logging_queue_func = None # to give to supervisor

    # worker settings
    max_tasks_per_worker = 0 # inf
    max_tasks_per_supervisor = 0
    
    # supervisor args for creation (follow through from init)
    min_supervisors = 1
    max_supervisors = 1
    supervisors = []
    supervisor_exchange_name = None   # used to identify tasks to only the supervisors workers
    supervisor = Supervisor
    default_supervisor_kwargs = [ 'host', 'port', 'vhost', 'user', 'password', 'pool', 'keys', 'stats_host', 'stats_port' ]
    supervisor_kwargs = []

    working_semaphore = multiprocessing.Event()

    def setup(self,**kwargs):
        """
        create a unique id for this team based on the classname of this manager, if we do not want this team
        to load balance jobs with others with same name, we shudl have unique supervisor_exchange_name's
        """
        # self.supervisor_exchange_name = "%s.%s" % (self.supervisor_queue_func,uuid())
        self.supervisor_exchange_name = "%s" % (camel_case_to_underscore(self.__class__.__name__,))
        logging.debug("supervisor_exchange_name: %s"%(self.supervisor_exchange_name,))

        # work day has started!
        self.working_semaphore.set()

    def restart( self, *args, **kwargs ):
        self.terminate_supervisors()
        self.terminate_workers()

    def terminate( self, *args, **kwargs ):
        self.working = False
        self.working_semaphore.clear()
        logging.debug("stop all work! %s" %self.working_semaphore.is_set() )
        # # kill all children workers
        # self.terminate_workers()
        # # kill all supervisors
        # self.terminate_supervisors()
    
    def terminate_supervisors( self, *args, **kwargs ):
        logging.info('terminating supervisors: %s' % (self.supervisors))
        for s in self.supervisors:
            s.terminate()            
        for s in self.supervisors:
            s.join()

    def reload( self, *args, **kwargs ):
        self.restart( *args, **kwargs )
        
    def loop_start( self, **options ):
        """ ensure the supervisor is alive """
        # reap dead ones
        k = []
        i = 0
        for s in self.supervisors:
            if not s or not s.is_alive():
                if s:
                    s.terminate()
                k.append( i )
            i = i + 1
            
        for i in reversed(k):
            if self.supervisors[i]:
                self.supervisors.pop(i)
            
        # create new ones
        for i in xrange(0,self.min_supervisors - len(self.supervisors)):
            self.supervisors.append( self.create_supervisor( **options ) )

    def create_supervisor( self, **kwargs ):
        # logging.debug('creating supervisor')
        opts = {
            'work_queue_func':  self.work_queue_func,   # monitors incoming tasks
            'results_queue_func': self.supervisor_queue_func,   # puts job into queue for workers
            # 'logging_queue_func': self.logging_queue_func,
            'max_tasks':         self.max_tasks_per_supervisor,
            'working_semaphore':    self.working_semaphore, # shift whistle
        }
            
        for f in ( self.default_supervisor_kwargs + self.supervisor_kwargs ):
            if f in kwargs:
                opts[f] = kwargs[f]
        # logging.error("OPTS: %s" % (opts,))
        # add exhange name so the worker only communicates with th supervisor via its own exchange
        opts['results_exchange_name'] = self.supervisor_exchange_name
        w = self.supervisor( self.queue_factory_obj,**opts )
        w.daemon = True
        w.start()
        w.join( 0.2 )
        return w

    def create_worker(self,job,**kwargs):
        logging.debug('creating worker')
        opts = {
            'work_queue_func':  self.supervisor_queue_func, # listen for job from supervisor
            'results_queue_func': self.results_queue_func,  # put results into results queue
            'logging_queue_func': self.work_queue_func,     # send logs back to supervisor
            'max_tasks':         self.max_tasks_per_worker,
            'working_semaphore':    self.working_semaphore, # shift whistle
        }
        # REMOVE THIS IF THINGS FAIL!
        for k in ( self.default_worker_kwargs + self.worker_kwargs ):
        # for k in self.worker_kwargs:
            # skip keys as we want everything from teh supervisor
            if not k in ( 'keys', ):
                opts[k] = kwargs[k]
        # modify the exchange that the worker listens on to that of the exchange the supervisor is using
        opts['work_exchange_name'] = self.supervisor_exchange_name
        # logging.error("OPTS: %s" % (opts,))
        return self.worker( self.queue_factory_obj, **opts )

