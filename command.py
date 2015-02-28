import argparse
import sys

import yaml

try:
    from setproctitle import setproctitle
except:
    def setproctitle(name):
        return
        
from slac_utils.queues import MultiprocessQueue
from multiprocessing import Process, Pool
from slac_utils.util import get_array
from slac_utils.klasses import create_klass
from slac_utils.string import camel_case_to_underscore
from slac_utils.time import sleep

from pwd import getpwnam
from grp import getgrnam

from copy import copy
from collections import MutableMapping

import signal
from daemon import DaemonContext
from lockfile import FileLock
import os

import logging
from slac_utils.logger import init_loggers as create_loggers
        
import traceback



class DefaultList(list):
    """
    allows settings to reference with dot notation
    allows resetting of datastructures from default
    """
    def __copy__(self):
        return []


class ManagerDaemon( object ):
    
    # __slots__ = ( 'manager', 'manager_obj', 'proc_name', 'working' )

    manager = None
    manager_obj = None
    proc_name = 'daemon'
    working = None
    
    def run( self, *args, **kwargs ):
        # logging.error('init...')
        self.manager_obj = self.manager( *args, **kwargs )
        # print('creating %s' % (self.proc_name,))
        if self.proc_name:
            setproctitle( self.proc_name )
        # logging.info("staring obj %s" % (self.manager_obj,))
        self.manager_obj.start( *args, **kwargs )
        # set name again
        if self.proc_name:
            setproctitle( self.proc_name )

    def terminate(self, *args, **kwargs ):
        logging.warn( 'Terminating '+ str(self.manager_obj) )
        self.working = False
        if self.manager_obj:
            self.manager_obj.terminate()
        sys.exit(1)
    
    def reload( self, *args, **kwargs ):
        logging.warn('Reloading ' + str(self))
        if self.manager_obj:
            self.manager_obj.reload()
        # reswpan?

    def start( self, *args, **kwargs ):

        self.working = True
        initial = True

        # run it!
        try:
            
            while self.working:

                verbose = kwargs['verbose'] if 'verbose' in kwargs else False
                
                if ( 'foreground' in kwargs and kwargs['foreground'] == False ) \
                    or ( 'daemonise' in kwargs and kwargs['daemonise'] == True ):

                    if not 'logfile' in kwargs:
                        raise IOError, 'logfile required for daemons'
                    elif kwargs['logfile'] == None:
                        raise IOError, 'logfile required for daemons'

                    # create daemon
                    context = DaemonContext()
                    # print "PIDFILE %s" % kwargs['pidfile']
                    if 'pidfile' in kwargs and not kwargs['pidfile'] == None:
                        context.pidfile = FileLock( kwargs['pidfile'] )
                    if 'uid' in kwargs and not kwargs['uid'] == None:
                        context.uid = getpwnam( kwargs['uid'] ).pw_uid
                    if 'gid' in kwargs and not kwargs['gid'] == None:
                        context.gid = getgrnam( kwargs['gid'] ).gr_gid

                    # add signal maps
                    context.signal_map = {
                        signal.SIGTERM: self.terminate,
                        signal.SIGHUP: self.reload,
                    }
                    try:
                        with context:
                            if initial:
                                create_loggers( file=kwargs['logfile'], verbose=verbose )
                            self.run( *args, **kwargs )
                    except Exception,e:
                        raise e

                    sys.exit(0)

                else:

                    if initial:
                        if not 'logfile' in kwargs:
                            kwargs['logfile'] = None
                        create_loggers( file=kwargs['logfile'], verbose=verbose )

                    # associate signal maps
                    signal.signal( signal.SIGTERM, self.terminate )
                    signal.signal( signal.SIGHUP, self.reload )

                    self.run( *args, **kwargs )
                
                initial = False
                
                logging.info('respawning')
                sleep( 5 )
                
        except KeyboardInterrupt, e:
            logging.info('ctrl-c received')
            self.working = False
            self.terminate( None, None )

        except:
            traceback.print_exc()

        sys.exit(0)



class Command( object ):
    """
    wrapper for executing command line arguments
    """
    # __slots__ = ()
    
    @classmethod
    def create_parser( cls, parser, settings, parents=[] ):
        # raise NotImplementedError, 'create_parser'
        pass
        
    def execute( self, *args, **kwargs ):
        try:
            args, kwargs = self.parse_args( *args, **kwargs )
            args, kwargs = self.pre_run( *args, **kwargs )
            self.run( *args, **kwargs )
        except KeyboardInterrupt, e:
            pass
            # logging.info('ctrl-c received')
        except SystemExit:
            pass
        except:
            traceback.print_exc()

    def parse_args( *args, **kwargs ):
        return args, kwargs
        
    def pre_run( self, *args, **kwargs ):
        return args, kwargs

    def run( self, *args, **kwargs ):
        raise NotImplementedError, 'run()'



class CommandDaemon( ManagerDaemon, Command ):
    """
    daemonises a manager
    """
    def pre_run( self, *args, **kwargs ):
        logfile = kwargs['logfile'] if 'logfile' in kwargs else None
        verbose = kwargs['verbose'] if 'verbose' in kwargs else False
        create_loggers( file=logfile, verbose=verbose )
        return args, kwargs

    def run( self, *args, **kwargs ):
        self.manager_obj = self.manager( *args, **kwargs )
        if self.proc_name:
            setproctitle( self.proc_name )
        self.manager_obj.start( *args, **kwargs )
        # set name again
        if self.proc_name:
            setproctitle( self.proc_name )


class CommandDispatcher( Command ):
    """
    a top level command that dispatches to subcommands
    """
    __slots__ = ( 'commands' )
    commands = []
    
    @classmethod
    def create_parser( cls, parser, settings, parents=[] ):
        # print "CLS %s" % (cls,)
        subparsers = parser.add_subparsers(help=cls.__name__.lower()+' sub-command help')
        for c in cls.commands:
            n = camel_case_to_underscore(c.__name__)
            # print "\n++ command dispatcher subcommand name: %s (%s)" % (n,c)
            t = {}
            try:
                m = n.upper()
                t = load_conf( settings['SUBCOMMAND_CONFIG'][m] )
            except KeyError,e:
                pass
            all_settings = Settings( **dict(settings.items() + t.items()) )
            # print "  local settings: %s" % (all_settings.store,)

            # print "  PARENT %s %s" % (c,parents)
            this = subparsers.add_parser( n, help=c.__doc__,  conflict_handler='resolve', parents=parents, description=c.__doc__ )
            this.set_defaults( subcls=c )

            c.create_parser( this, all_settings, parents=parents )
    
    def run( self, *args, **kwargs ):
        """
        just find which subcommand to execute and do it
        """
        # print "SUBCLS: %s" % (kwargs['subcls'])
        klass = kwargs['subcls']()
        klass.execute( *args, **kwargs )


class Settings( MutableMapping ):
    def __str__(self):
        return "%s" % (self.__dict__,)
    def __init__(self, *args, **kwargs):
        self.store = dict()
        self.update(dict(*args, **kwargs)) # use the free update to set keys
    def __getitem__(self, key):
        return self.store[self.__keytransform__(key)]
    def __setitem__(self, key, value):
        self.store[self.__keytransform__(key)] = value
    def __delitem__(self, key):
        del self.store[self.__keytransform__(key)]
    def __iter__(self):
        return iter(self.store)
    def __len__(self):
        return len(self.store)
    def __keytransform__(self, key):
        return key
    def __getattr__(self,key):
        return self.store[self.__keytransform__(key)]
    # def __setattr__(self,key,value):
    #     self.store[key] = value


def load_conf( path ):
    if type(path)==str and os.path.exists(path):
        with open( path, 'r' ) as f:
            # print("loading config file " + cmd_map[name]['conf'] )
            d = yaml.load(f)
            if not d == None:
                return Settings( **d )
    return {}

def execute_command( cmd_map, parser, *args ):
    """
    command line dispatcher; parser should be the common parsing arguments
    """
    p = argparse.ArgumentParser( conflict_handler='resolve', add_help=False, description=parser.description ) 
    # ns, extra = parser.parse_known_args()
    # print "START %s %s" % (ns,extra)
    p.add_argument( '-h', '--help', help='show this help message and exit', action='help' )
    subparsers = p.add_subparsers(help='sub-command help')
    subparser = {}
    for name,d in cmd_map.iteritems():
        # print "adding %s %s" % (name,d)

        if not 'klass' in cmd_map[name]:
            raise SyntaxError, 'undefined command class for %s' % (name,)
        k = create_klass( cmd_map[name]['klass'] )
        # print " k: %s %s" % (k,issubclass(k,CommandDispatcher))

        subparser[name] = subparsers.add_parser( name, help=k.__doc__, conflict_handler='resolve', parents=[parser], description=k.__doc__ )

        # load the default conf file if exists
        conf = []
        if 'conf' in cmd_map[name]:
            if isinstance( cmd_map[name]['conf'], list ):
                conf = cmd_map[name]['conf']
            else:
                conf.append( cmd_map[name]['conf'] )
        settings = {}
        for c in conf:
            s =  load_conf( c )
            settings = Settings( **dict( settings.items() + s.items() ) )
            # print "  > SETTINGS: %s" % (settings,)
        k.create_parser( subparser[name], settings, parents=[parser] )
        subparser[name].set_defaults( cls=k )
                        
    o = vars( p.parse_args() )
    # instantiate klass and execute
    klass = o['cls']()
    klass.execute( **o )


def process_map_wrapper( args ):
    """
    simple wrapper to allow normal function signatures for a multiprocess function
    """
    target = args.pop(0)
    return target( *args )

class MultiprocessMixin( object ):
    """
    run a list of items in parallel, calling method target
    """
    def map( self, target_function, iterable, target_args=[], num_workers=1 ):
        pool = Pool( processes=num_workers )
        args = target_args
        iterable_args = [ [target_function,a] + target_args for a in iterable ]
        return pool.map( process_map_wrapper, iterable_args )
        
    def fold( self, iterable, map_output, ignore=[] ):
        for i,v in enumerate(iterable):
            if not v in ignore:
                yield iterable[i], map_output[i]
        return

    def parallel_run( self, target, items, num_workers=1, target_args=[] ):
        
        # work queue
        q = MultiprocessQueue()
        # results queue
        p = MultiprocessQueue()

        # multiprocess
        procs = []
        full_args = [ q, p ]
        full_args.extend( target_args )
        for i in range(int(num_workers)):
            t = Process( target=target, args=full_args )
            t.daemon = True
            t.start()
            procs.append( t )

        # check to see if hosts is a file and determine appropriate argument array
        items = get_array( items )
        for d in items:
            q.put( d )
            
        # get results
        n = 0
        while n < len(items):
            yield p.get()
            n = n + 1
            p.task_done() 
 
        # await all to be finished
        q.join()

        # clean up
        for i in range( num_workers ):
            q.put(None)
        
        return
        

