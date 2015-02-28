from uuid import uuid1 as uuid
from types import GeneratorType

import sys, signal

import argparse
from slac_utils.time import sleep

from slac_utils.queues import PyAmqpLibQueueFactory as QueueFactory
from slac_utils.messages import Message
from slac_utils.env import get_user

import logging


class Task( Message ):
    
    def __init__(self, meta={}, context={}, data={}, type='task' ):

        meta['type'] = type
        if not 'task_id' in meta or meta['task_id'] == None:
            meta['task_id'] = str(uuid())

        # logging.error("USER1: %s" % (meta['user']))
        if not 'user' in meta or meta['user'] == None:
            meta['user'] = get_user()  # deal with remctl
        # logging.error("USER2: %s" % (meta['user']))

        super( Task, self ).__init__( meta, context, data )
        other = {
            'id':   meta['task_id'],
            'user': meta['user'],
            'type': meta['type'],
        }
        self.update( **other )


    

class TaskRequest( Task ):
    """
    object used to request that something 
    """
    context_fields = [ 'device' ]
    optional_context_fields = []
    action_fields = []    
    
    action = None

    def __init__(self, meta={}, context={}, data=[]):

        logging.debug("creating request: %s, %s" % (context,data) )
        super( TaskRequest, self ).__init__( meta=meta, context=context, data=data, type='task')
        self.context = self.set_context( context )
        if len(self.context.keys()) < len(self.context_fields):
            raise Exception, 'not all context fields defined for task request: %s %s' % (self.context.keys(), self.context_fields)

        self.data = self.set_action( data )

    
    def set_context(self, item):
        this = {}
        for i in self.context_fields:
            # logging.debug('checking %s in %s' % (i,item))
            if i in item:
                logging.debug( ' ' + str(i) + ' adding context')
                this[i] = item[i]
            else:
                raise SyntaxError, 'missing context field %s in %s' % (i,item)
        try:
            for i in self.optional_context_fields:
                logging.debug("checking optional context field: %s" % (i,))
                if i in item:
                    logging.debug( ' %s adding context' % (i,) )
                    this[i] = item[i]
        except:
            pass
        logging.debug('setting context %s' %(this,) )
        return this
        
    def set_action(self, item ):
        this = {}
        # logging.error("ACTION FIELDS: " + str(self.action_fields))
        # logging.error("CONTEXT FIELDS: " + str(self.context_fields))
        for i in item:
            if i in self.action_fields:
                # logging.debug(" adding action %s as %s " %(i,item[i]))
                this[i] = item[i]
            else:
                logging.debug(" ignoring action " + str(i) )
                # raise Exception, 'internal error: unsupported action ' + str(i) + ' for request type ' + str(self.__name__)
        logging.debug(" setting actions: %s" %(this))
        return this


class LogMessage( Task ):
    def __init__(self, meta={}, context={}, data={} ):
        super(LogMessage,self).__init__(meta=meta, context=context, data=data, type='log')


class Requestor(object):

    argparser = None
    
    request_timeout = 15
    request_object = TaskRequest
    queue_factory = QueueFactory

    work_queue_func = 'work'
    
    results = []
    
    def __init__(self, argparser=None, settings={} ):

        if argparser == None:
            argparser = argparse.ArgumentParser( description='Generic Requestor Class' )
            argparser.add_argument( '-v', '--verbose', help="verbosity", default=False, action='store_true' )

        if isinstance( argparser, dict ):
            self.kwargs = argparser
        else:
            self.argparser = argparser
            self.args = self.argparser.parse_args()        
            self.kwargs = vars(self.args)

        if 'verbose' in self.kwargs and self.kwargs['verbose']:
            logging.basicConfig(level=logging.DEBUG)
        else:
            logging.basicConfig(level=logging.INFO, format='%(message)s')
    
        self.f = self.queue_factory( host=settings.BROKER_HOST, port=settings.BROKER_PORT, 
                virtual_host=settings.BROKER_VHOST, user=settings.BROKER_USER, password=settings.BROKER_PASSWORD )
        self.connection = self.f.get_connection()
        # logging.error("WORK QUEUE: %s" % (self.work_queue_func,))
        self.q = getattr( self.f, self.work_queue_func )( connection=self.connection )
        self.jobs = []
        self.results = []

        self.parse()

    def __del__( self ):
        try:
            for r in self.results:
                r.delete()
                r.__exit__()
        except:
            pass

    def usage(self, error=None ):
        self.argparser.print_help()
        # print something

    def parse(self):
        """
        create the requests and append to jobs queue
        """
        for a in self.args:
            req = self.request_object( a )
            self.jobs.append( req )
    
    def submit( self, request ):
        # start listening for responses from server
        r = self.f.client( consume=True, keys=[ request.id ], auto_delete=True )
        r.__enter__()
        # kickstart consumer
        r._consumer_start()
        self.results.append( r )
        with self.q as q:
            q.put( request )
            # add delay in case response is too quick
            # sleep( 1 )
    
    def run( self ):
        """
        create requests from the parsed jobs and submit
        """
        if len(self.jobs) == 0:
            raise Exception, 'no jobs defined'
        for item in self.jobs:
            self.submit( item )

    def report(self):
        # see if we get a response soon
        def timeout_handler(signum,frame):
            logging.error("Error: Response timeout")
            sys.exit(1)
        signal.signal( signal.SIGALRM, timeout_handler )
        signal.alarm(self.request_timeout)

        for r in self.results:
            for i in r.get( format=r.format ):
                signal.alarm(0) # cancel
                l = i['level']
                # logging.info("GOT %s: %s" % (l,i) )
                for d in i['data']:
                    # logging.error("L: %s - %s" % (l,d))
                    if l == 'data':
                        yield( d )
                    else:
                        m = str(d)
                        if l == 'warn':
                            m = 'Warning: ' + m
                        elif l == 'error':
                            m = 'Error: ' + m
                        getattr( logging, l )( m )

        return
        

class Request(object):

    request_timeout = 15
    queue_factory = QueueFactory
    q = None
    
    submission_queue = 'work'
    s = None
    result_queue = 'result'
    r = None
    logging_queue = 'logging'
    l = None
    
    def __init__(self, **kwargs ):
        self.f = self.queue_factory( **kwargs )
        # connection polling
        self.connection = self.f.get_connection()
        self.s = getattr( self.f, self.submission_queue )( auto_delete=True )

    def __del__( self ):
        try:
            self.r.delete()
            for r in self.r:
                r.__exit__()
        except:
            pass
    
    def submit( self, request ):
        # start listening for responses from server
        task_id = request._meta['task_id']
        self.r = getattr( self.f, self.result_queue )( connection=self.connection, consume=True, pool='poll-request', keys=[ '#.task_id.%s.#' % (task_id,) ], auto_delete=True, exclusive=True, durable=False )
        self.r.__enter__()
        # self.l = getattr( self.f, self.logging_queue )( connection=self.connection, consume=True, pool='poll-request', keys=[ '#.task_id.%s.#' % (task_id,) ], auto_delete=True, exclusive=True, durable=False )
        with self.s as q:
            logging.debug("sending to %s: %s" % (q,request))
            k = None
            q.put( request, key=k )

    def report(self):
        # see if we get a response soon
        def timeout_handler(signum,frame):
            logging.error("Error: Response timeout")
            sys.exit(1)
        signal.signal( signal.SIGALRM, timeout_handler )
        signal.alarm(self.request_timeout)

        logging.error("recving from %s" % (self.r,))
        for i in self.r.get():
            signal.alarm(0) # cancel
            print i
        return
    
    def submit_and_report( self, request ):
        self.submit( request )
        self.report()

class JobStatusQueue(object):
    """
    wrapper around a queue to enable logging
    """
    def __init__(self, queue, log=None ):
        self.queue = queue
        logging.debug("job status queue from: %s" % (self.queue,))
        self.queue.__enter__()
        self.log = log
    def __del__(self, *args, **kwargs):
        if self.queue:
            self.queue.__exit__()
    def _routing_key( self, msg ):
        return msg.id
    def report(self, message, job, level='debug', local_too=False ):
        if job == None:
            job = self.log
        self.msg = LogMessage( meta=job['_meta'], context=job['context'], data=[] )
        data = []
        if isinstance( message, list ):
            data = message
        else:
            data = [ message ]
        self.msg.data = data
        self.msg.level = level
        # logging.error("MESSAGE %s" % (self.msg,))
        if self.queue:
            # logging.debug("> REPORTIN %s\tDATA %s: ID: %s\t%s" % (level, data, self.msg.id, self.msg))
            self.queue.put( self.msg, key=self.msg.id )
        if local_too:
            getattr( logging, level )( message )
    def data( self, message, job=None ):
        if isinstance( message, GeneratorType ):
            self.report( [ m for m in message ], job, level='data' )
        else:
            self.report( message, job, level='data' )
    
    def debug( self, message, job=None, local_too=False ):
        self.report( message, job, level='debug', local_too=local_too )
    def info( self, message, job=None, local_too=False ):
        self.report( message, job, level='info', local_too=local_too )
    def warn( self, message, job=None, local_too=False ):
        self.report( message, job, level='warn', local_too=local_too )
    def error( self, message, job=None, local_too=False ):
        self.report( message, job, level='error', local_too=local_too )
    def terminate(self):
        if self.queue:
            # logging.error("QUEUE TERMINATE: %s" % (self.queue,))
            self.queue.end( key=self.msg.id )
