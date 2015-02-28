from Queue import Queue as StandardQueue, Empty
try:
    import amqplib.client_0_8 as amqp
except:
    pass

from multiprocessing import JoinableQueue

from slac_utils.time import sleep, add_tz
from uuid import uuid1 as uuid
from datetime import datetime

try:
   import cPickle as pickle
except:
   import pickle
import json

import logging

try:
    import redis
except:
    #    logging.warn("no redis module found")
    pass
    


class JsonEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return "%s" % ( add_tz(obj), )
        return json.JSONEncoder.default(self, obj)


END_MARKER = '===END==='

class Queue( object):
    """
    simulate the standard python queue object
    """
    q = []
    
    def __init__(self):
        self.q = []
        
    def __enter__(self):
        return self
        
    def __exit__( self, *args, **kwargs ):
        pass
    
    def __nonzero__(self):
        """
        enable use of if Queue():, in this case, we're a dummy class so it's not
        """
        return False
    
    def put( self, item, **kwargs ):
        """ puts the item into the queue """
        pass
        
    def get( self, non_blocking=True, **kwargs ):
        """ gets a single item from teh queue, if non_blocking, then may return None """
        pass
        
    def consume( self, callback, limit=0, **kwargs ):
        """
        get from queue and call the callback with the item in the queue forever
        """
        raise NotImplementedError, 'consume()'
        
    def qsize( self ):
        """
        get the estimate sie of the queue
        """
        raise NotImplementedError, 'qsize()'
        
    def task_done( self ):
        """
        mark the task/item as done
        """
        raise NotImplementedError, 'task_done()'

    def end( self, **kwargs ):
        # self.put( END_MARKER )
        pass
    

class QueueFactory(object):
    """
    simple class that returns queue objects for publish or consume
    """
    # options to be passed to construciton of each queue
    queue_options = {}
    # use this to remap the keys for incoming constructor arguments to something useable directly by the queue constructor
    kwargs_remap = {}
    
    def __init__(self, *args, **kwargs ):
        for k,v in kwargs.iteritems():
            if k in self.kwargs_remap:
                k = self.kwargs_remap[k]
            self.queue_options[k] = v
        # logging.debug( 'created queue factory: %s' %(self.queue_options))

class PyAmqpLibQueueFactory( QueueFactory ):
    
    kwargs_remap = { 'vhost': 'virtual_host', 'user': 'userid' }
    # support for RR of amqp servers
    host_index = 0

    def get_queue( self, name, type, **kwargs ):
        if 'exchange_name' in kwargs:
            if kwargs['exchange_name'] == None:
                kwargs['exchange_name'] = name
        else:
            kwargs['exchange_name'] = name
        kwargs['queue_name'] = name
        kwargs['type'] = type
        # logging.error("FAC: %s %s" % (self.queue_options, kwargs,))
        return PyAmqpLibQueue( **dict( self.queue_options.items() + kwargs.items() ) )

    def get_connection_options( self, **kwargs ):
        # be mindful of the fact that host can be a list, so if we are at the end
        for i in ( 'port', 'host', 'virtual_host', 'userid', 'password' ):
            if i in self.queue_options:
                kwargs[i] = self.queue_options[i]
        return kwargs
        
    def get_connection( self, **kwargs ):
        # get a connection; could be any from list of hosts
        these_kwargs = self.get_connection_options()
        conn, chanid, self.host_index = get_pyamqp_connection( port=these_kwargs['port'], virtual_host=these_kwargs['virtual_host'], user=these_kwargs['userid'], password=these_kwargs['password'], *these_kwargs['host'], host_index=self.host_index )
        return conn
    
def get_pyamqp_connection( *hosts, **kwargs ):
    #port=5672, virtual_host='/', user='guest', password='guest', backoff=1, *hosts ):
    # support for RR of amqp hosts
    # reorg connection order based on last_idx
    last = 0
    if 'host_index' in kwargs and kwargs['host_index']:
        try:
            last = int(kwargs['host_index'])
        except:
            pass
    items = []
    end = []
    for i,h in enumerate(hosts):
        if i < last:
            end.append( (i,h) )
        else:
            items.append( (i,h) )
    items = items + end
    for idx, connection in items:
        try:
            this_host, _tmp, this_port = connection.partition(':')
            this_port = int(this_port) if not this_port == '' else int( kwargs['port'] )
            logging.debug("connecting to message broker %s:%s/%s" % (this_host,this_port,kwargs['virtual_host']))
            this = '%s:%s' % ( this_host, this_port )
            conn = amqp.Connection( host=this, virtual_host=kwargs['virtual_host'], userid=kwargs['user'], password=kwargs['password'] )
            chan = get_pyamqp_channel( conn )
            chan.access_request( kwargs['virtual_host'], active=True, write=True, read=True)
            return conn, chan, idx
        except Exception, e:
            logging.warn("connecting to queue failed: %s" % (e,))
            sleep( 1 )
    raise IOError( 'no queue members available' )

def get_pyamqp_channel( conn ):
    chan_id = conn._get_free_channel_id()
    chan = conn.channel( channel_id=chan_id )
    logging.debug("  using channel 0x%x (%s) %s" % (id(conn) ,chan_id, chan))
    return chan

class PyAmqpLibQueue( Queue ):
    
    conn = None
    chan = None
    _callback = None
    
    # tag for acknowledging stuff
    tag = ''
    
    host_index = 0
    host = 'localhost'
    port = 5672
    virtual_host = '/'
    userid =    'guest'
    password =  'guest'
        
    exchange_name = 'generic'
    type    = 'direct'
    pool = 'default'
    queue_name = 'q'
    keys    = ('#',)
    durable = True
    exclusive = False

    insist  = False
    auto_delete = True
    no_ack  = False
    queue_arguments = None
    
    queues_declared = False
    consuming = False
    
    format = 'pickle' # | json
    
    def __init__(self, **kwargs):
        # logging.warn("init queue: %s" % (kwargs,) )
        for k in ( 'port', 'host', 'virtual_host', 'userid', 'password', 'exchange_name', 'type', 'pool', 'queue_name', 'keys', 'format', 'queue_arguments' ):
            if k in kwargs and not kwargs[k] == None:
                setattr( self, k, kwargs[k] )
                # logging.debug("  set %s %s -> %s" % (k,kwargs[k],getattr(self,k)))
        self.queue_name = "%s@%s.[%s]" % ( self.exchange_name, self.pool, ':'.join( self.keys ) )
        self.tag = str(uuid())
        self.conn = kwargs['connection'] if 'connection' in kwargs else None
        self.chan = None
        self.queues_declared = False

    def __nonzero__(self):
        return True

    def __str__(self):
        s = self.host
        if isinstance( s, list ):
            s = self.host[self.host_index]
        return '<PyAmqpLibQueue at 0x%x amqp://%s%s, %s exchange: %s q: %s (%s)>' % (id(self), s, self.virtual_host, self.type, self.exchange_name, self.queue_name, self.format )


    def __enter__(self):
        """ try all registered hosts/ports channels """
        # reuse a connection if initaited with one
        if not self.conn:
            self.conn, self.chan, self.host_index = get_pyamqp_connection( port=self.port, virtual_host=self.virtual_host, user=self.userid, password=self.password, host_index=self.host_index, *self.host )
  
        if not self.conn:
            raise IOError( 'could not connect to any queue')
                
        # deal with channels, exchanges, queues etc.
        if not self.chan:
            self.chan = get_pyamqp_channel( self.conn )

        self.chan.exchange_declare( exchange=self.exchange_name, type=self.type, durable=self.durable, auto_delete=False )
        logging.debug('  declared %s exchange %s (%s %s)' % ( self.type, self.exchange_name, 'durable' if self.durable else 'non-durable', 'auto_del' if self.auto_delete else 'not auto_del' ) )
        # logging.warn( ' %s' % (self,))
        return self        
        
        
    def __exit__(self, *args, **kwargs):
        if self.chan:
            try:
                self.chan.close()
            except:
                logging.error("Error closing chan %s" % (self.chan) )
        self.chan = None
        if self.conn:
            try:
                self.conn.close()
            except:
                logging.error("Error closing conn %s" % (self.conn) )
        self.conn = None
    
    def connected(self):
        if self.chan and self.conn:
            return True
        return False
    
    def __del__( self ):
        self.__exit__()
        
    def delete( self ):
        self.chan.queue_delete( self.queue_name )

    def _consumer_start( self, prefetch_count=None ):
        """ declare that we want to listen for stuff """
        if prefetch_count:
            self.chan.basic_qos( prefetch_size=0, prefetch_count=prefetch_count, a_global=False )
        self.chan.queue_declare( queue=self.queue_name, durable=self.durable, exclusive=self.exclusive, auto_delete=self.auto_delete, arguments=self.queue_arguments )
        for k in self.keys:
            logging.debug("  binding key %s on queue %s" % (k,self.queue_name) )
            self.chan.queue_bind( queue=self.queue_name, exchange=self.exchange_name, routing_key=k )
        self.queues_declared = True
        self.consuming = True
    
    def get(self, non_blocking=False, poll_time=0.01, no_ack=False, prefetch_count=1, format='pickle' ):
        """
        generator for picking things off the queue. if non_blocking, then exit as soon as there's nothing left in the queue, otherwise keep on picking things off every poll_time seconds.
        """
        if not self.queues_declared:
            self._consumer_start( prefetch_count=prefetch_count )
        while self.consuming:
            msg = None
            try:
                msg = self.chan.basic_get(self.queue_name, no_ack=no_ack)
                if msg:
                    if format == 'pickle':
                        body = pickle.loads( msg.body )
                    elif format == 'json':
                        body = json.loads( msg.body )
                    else:
                        raise NotImplementedError, 'unsupported serialisation %s' % format
                    if body == END_MARKER:
                        self.consuming = False
                    else:
                        yield body
                # exit if we don't have anything!
                elif non_blocking:
                    break
            except Exception,e:
                logging.error("%s %s" % (type(e),e))
            finally:
                if msg and no_ack == False:
                    self.chan.basic_ack( msg.delivery_tag )

            # don't thrash
            sleep( poll_time )

        return
        
    def _consume( self, msg ):
        # logging.warn( 'consuming msg: %s' % (msg,) )
        body = None
        if msg:
            body = msg.body
            if self.format == 'pickle':
                body = pickle.loads( msg.body )
            elif self.format == 'json':
                body = json.loads( msg.body )
            else:
                raise NotImplementedError, 'unknown serialisation format %s' % self.format
            if body == END_MARKER:
                self.consuming = False
        self._callback( body, msg )
        
        
    def consume( self, callback, no_ack=None, limit=0, format='pickle', prefetch=1 ):
        logging.debug("consuming %s..."%(self))
        # we need to unpickle, so we allways call _consume, and let _consume call the callback
        self._callback = callback
        self._consumer_start( prefetch_count=prefetch )
        no_ack = self.no_ack if no_ack == None else no_ack
        n = 0
        self.chan.basic_consume( queue=self.queue_name, no_ack=no_ack, callback=self._consume, consumer_tag=self.tag)
        while self.tag and ( limit == 0 or n < limit ):
            # logging.debug('waiting on %d (%d)' % (n,limit))
            self.chan.wait()
            n = n + 1
        logging.debug('+++ ending consume')
        self.chan.basic_cancel( self.tag )
        

    def _producer_start( self ):
        if not self.chan:
            self.__enter__()
        pass
        
    def put( self, item, key='#', mode=1, headers={} ):
        if key == None:
            key = ''
        self._producer_start()
        this = item
        # headers = item['_meta'] if '_meta' in item
        # del this['_meta']
        # logging.error("PUT: %s (headers %s)" % (item,headers))
        # logging.error("PUT %s: %s (%s): %s" % (self.format,key,self.exchange_name,item))
        body = None
        if self.format == 'pickle':
            body = pickle.dumps( item )
        elif self.format == 'json':
            body = json.dumps( item, cls=JsonEncoder )
        else:
            raise NotImplementedError, 'unknown serialisation format %s' % self.format
        msg = amqp.Message( body, application_headers=headers )
        msg.properties['delivery_mode'] = mode
        self.chan.basic_publish( msg, exchange=self.exchange_name, routing_key=key )
        
    def task_done( self, msg ):
        # logging.warn("task_doen %s" % (msg.delivery_tag,))
        self.chan.basic_ack( msg.delivery_tag )

    def end( self, key=None ):
        self.put( END_MARKER, key=key )
    

    
class PyAmqpLibMultiQueue( Queue ):
    """
    Amqp queue that allows binding to multiple exchanges and routing keys
    """
    
    conn = None
    chan = None
    _callback = None
    
    # tag for acknowledging stuff
    tag = ''
    
    # defaults
    host = 'localhost'
    port = 5672
    virtual_host = '/'
    userid =    'guest'
    password =  'guest'
        
    queue_name = 'q'
    specs = ( {}, )
    exchange_name = 'generic'
    type    = 'direct'
    pool = 'default'
    keys    = ('#',)
    durable = True
    exclusive = False

    insist  = False
    auto_delete = True
    no_ack  = False
    queue_arguments = None
    
    queues_declared = False
    consuming = False
    
    format = 'pickle' # | json
    
    def __init__(self, *args, **kwargs):
        """
        synopsis
        q = PyAmqpLibMultiQueue( 
            { 
                'exchange': <exchange_name>',
                'type': '<topic|direct|fanout>',
                'durable': True|False,
                'auto_delete': True|False,
                'keys': [ '<str>', '<str>', ... ]
            },
            ....
            host=<str>,
            port=<int>,
            virtual_host=<str>,
            userid=<str>,
            password=<str>,
            
            pool=<str>,
            queue_name=<str>,
            format=json|pickle,
            queue_arguments={}
        )
        """
        self.specs = args
        for k in ( 'port', 'host', 'virtual_host', 'userid', 'password', 'pool', 'queue_name', 'format', 'queue_arguments' ):
            if k in kwargs and not kwargs[k] == None:
                setattr( self, k, kwargs[k] )
                # logging.debug("  set %s %s -> %s" % (k,kwargs[k],getattr(self,k)))

        # take default values if not exist
        for s in self.specs:
            for i in ( 'type', 'auto_delete', 'durable', 'keys' ):
                if not i in s:
                    s[i] = getattr(self,i)

        self.queue_name = "%s" % ( ':'.join(self._partial_str()) )
        self.tag = "%s" % id(self)
        self.conn = kwargs['connection'] if 'connection' in kwargs else None
        self.chan = None
        self.queues_declared = False

    def __nonzero__(self):
        return True

    def _partial_str( self ):
        for s in self.spec:
            yield "%s:%s@%s.[%s]" % (s['type'],s['name'],self.pool,':'.join(s['keys'])) 
        return
        
    def __str__(self):
        return '<PyAmqpLibMultiQueue at 0x%x amqp://%s:%s%s: q: %s, %s>' % (id(self),self.host,self.port, self.virtual_host, self.queue_name, ', '.join(self._partial_str() ) )

    def __enter__(self):
        """
        establish amqp connection and exchanges
        """

        """ try all registered hosts/ports channels """
        # reuse a connection if initaited with one
        if not self.conn:
            self.conn, self.chan, self.host_index = get_pyamqp_connection( port=self.port, virtual_host=self.virtual_host, user=self.userid, password=self.password, *self.host )
  
        if not self.conn:
            raise IOError( 'could not connect to any queue')
                
        # deal with channels, exchanges, queues etc.
        if not self.chan:
            self.chan = get_pyamqp_channel( self.conn )

        #logging.warn("  using channel 0x%x (%s) %s" % (id(self) ,chan_id, self.chan))
        for s in self.specs:
            self.chan.exchange_declare( exchange=s['exchange'], type=s['type'], durable=s['durable'], auto_delete=s['auto_delete'] )
        return self
        
    def __exit__(self, *args, **kwargs):
        if self.chan:
            try:
                self.chan.close()
            except:
                logging.error("Error closing chan %s" % (self.chan) )
        self.chan = None
        if self.conn:
            try:
                self.conn.close()
            except:
                logging.error("Error closing conn %s" % (self.conn) )
        self.conn = None
    
    def connected(self):
        if self.chan and self.conn:
            return True
        return False
    
    def __del__( self ):
        self.__exit__()
        
    def delete( self ):
        self.chan.queue_delete( self.queue_name )

    def _consumer_start( self, prefetch_count=None ):
        """ declare that we want to listen for stuff """
        if prefetch_count:
            self.chan.basic_qos( prefetch_size=0, prefetch_count=prefetch_count, a_global=False )
        # declare the queue
        self.chan.queue_declare( queue=self.queue_name, durable=self.durable, exclusive=self.exclusive, auto_delete=self.auto_delete, arguments=self.queue_arguments )
        # bind all keys and exchanges to queue
        for s in self.specs:
            for k in s['keys']:
                logging.debug("  binding exchange %s key %s on queue %s" % (s['exchange'],k,self.queue_name) )
                self.chan.queue_bind( queue=self.queue_name, exchange=s['exchange'], routing_key=k )
        self.queues_declared = True
        self.consuming = True
    
    def get(self, non_blocking=False, poll_time=0.01, no_ack=False, prefetch_count=1 ):
        """
        generator for picking things off the queue. if non_blocking, then exit as soon as there's nothing left in the queue, otherwise keep on picking things off every poll_time seconds.
        """
        if not self.queues_declared:
            self._consumer_start( prefetch_count=prefetch_count )
        while self.consuming:
            try:
                msg = self.chan.basic_get(self.queue_name, no_ack=no_ack)
                if msg:
                    body = pickle.loads( msg.body )
                    if body == END_MARKER:
                        self.consuming = False
                    else:
                        yield body
                # exit if we don't have anything!
                elif non_blocking:
                    break
            except Exception,e:
                logging.error("%s %s" % (type(e),e))
            finally:
                if msg and no_ack == False:
                    self.chan.basic_ack( msg.delivery_tag )

            # don't thrash
            sleep( poll_time )

        return
        
    def _consume( self, msg ):
        # logging.warn( 'consuming msg: %s' % (msg,) )
        body = None
        if msg:
            body = msg.body
            if self.format == 'pickle':
                body = pickle.loads( msg.body )
            elif self.format == 'json':
                body = json.loads( msg.body )
            else:
                raise NotImplementedError, 'unknown serialisation format %s' % self.format
            if body == END_MARKER:
                self.consuming = False
        self._callback( body, msg )
        
        
    def consume( self, callback, no_ack=None, limit=0, format='pickle', prefetch=1 ):
        logging.debug("consuming %s..."%(self))
        # we need to unpickle, so we allways call _consume, and let _consume call the callback
        self._callback = callback
        self._consumer_start( prefetch_count=prefetch )
        no_ack = self.no_ack if no_ack == None else no_ack
        n = 0
        self.chan.basic_consume( queue=self.queue_name, no_ack=no_ack, callback=self._consume, consumer_tag=self.tag)
        while self.tag and ( limit == 0 or n < limit ):
            # logging.debug('waiting on %d (%d)' % (n,limit))
            self.chan.wait()
            n = n + 1
        logging.debug('+++ ending consume')
        self.chan.basic_cancel( self.tag )
        

    def _producer_start( self ):
        pass
        
    def put( self, item, key='#', mode=1, headers={} ):
        if key == None:
            key = ''
        self._producer_start()
        this = item
        # headers = item['_meta'] if '_meta' in item
        # del this['_meta']
        # logging.error("PUT: %s (headers %s)" % (item,headers))
        # logging.error("PUT: %s (%s): %s" % (key,self.exchange_name,item))
        body = None
        if self.format == 'pickle':
            body = pickle.dumps( item )
        elif self.format == 'json':
            body = json.loads( item )
        else:
            raise NotImplementedError, 'unknown serialisation format %s' % self.format
        msg = amqp.Message( body, application_headers=headers )
        msg.properties['delivery_mode'] = mode
        self.chan.basic_publish( msg, exchange=self.exchange_name, routing_key=key )
        
    def task_done( self, msg ):
        # logging.warn("task_doen %s" % (msg.delivery_tag,))
        self.chan.basic_ack( msg.delivery_tag )

    def end( self, key=None ):
        self.put( END_MARKER, key=key )

class MultiprocessQueue( Queue ):
    
    queue = None
    
    def __init__(self, **kwargs):
        self.queue = JoinableQueue()

    def __nonzero__(self):
        return True

    def __str__(self):
        return '<MultiProcessQueue at 0x%x>' % (id(self),)

    def __enter__(self):
        return self
        
    def __exit__(self, *args, **kwargs):
        pass
        
    def __del__( self ):
        self.__exit__()
        
    def get(self, non_blocking=False):
        # body = pickle.load( self.queue.get() )
        # logging.warn('RCV: %s, body: %s' %(msg, body))
        # return body
        return self.queue.get()
    
    def join(self):
        return self.queue.join()
        
    def consume( self, callback, no_ack=None, limit=0 ):
        logging.debug("consuming %s..."%(self))
        while limit == 0 or n < limit:
            # logging.debug('waiting on %d (%d)' % (n,limit))
            item = self.get()
            callback( item, {} )
            n = n + 1
        # logging.warn('+++ ending consume')
        
    def put( self, item, **kwargs ):
        # msg = pickle.dumps(item)
        self.queue.put( item )
        
    def task_done( self, *args ):
        self.queue.task_done()
        
class RedisQueue( Queue ):
    """
    use a single key and LIST value to RPUSH and BLPOP
    """
    
    conn = None
    queue = None
    consuming = True
    
    
    def __init__(self, **kwargs):
        for k in ( 'host', 'port', 'db', 'key' ):
            if k in kwargs and not kwargs[k] == None:
                if k == 'host':
                    kwargs['host'], _tmp, port = kwargs['host'].partition(':')
                    if port:
                        kwargs['port'] = int(port)
                setattr( self, k, kwargs[k] )


    def __enter__(self):
        """
        establish redis connection
        """
        # reuse a connection if initaited with one
        if not self.conn:
            self.conn = redis.ConnectionPool( host=self.host, port=self.port, db=self.db )
        self.queue = redis.Redis( connection_pool=self.conn )
        return self
        
    def __exit__(self, *args, **kwargs):
        self.conn = None
    
    def get(self, non_blocking=False, poll_time=0.01, no_ack=False, prefetch_count=1 ):
        """
        generator for picking things off the queue. if non_blocking, then exit as soon as there's nothing left in the queue, otherwise keep on picking things off every poll_time seconds.
        """
        while self.consuming:
            try:
                msg = None
                if not non_blocking:
                    msg = self.queue.blpop( self.key )
                else:
                    msg = self.queue.lpop( self.key )
                if msg:
                    yield msg[1]
            except Exception,e:
                logging.error("%s %s" % (type(e),e))
            sleep( poll_time )
        return
    
    def put( self, item ):
        self.queue.rpush( self.key, item )
    
    def consume( self, callback ):
        # we need to unpickle, so we allways call _consume, and let _consume call the callback
        self._callback = callback
        for item in self.get():
            yield callback( item, {} )
        return
    
class RedisPubSubQueue( RedisQueue ):
    """
    publish/listen on pubsub
    """
    
    def __enter__(self):
        super( RedisPubSubQueue, self ).__enter__()
        # add pubsub
        self.pubsub = self.queue.pubsub()
        
        
    def get(self, non_blocking=False, poll_time=0.01, no_ack=False, prefetch_count=1 ):
        while self.consuming:
            try:
                msg = None
                if not non_blocking:
                    msg = self.queue.blpop( self.key )
                else:
                    msg = self.queue.lpop( self.key )
                if msg:
                    yield msg[1]
            except Exception,e:
                logging.error("%s %s" % (type(e),e))
            sleep( poll_time )
        return
    
    def put( self, item ):
        self.queue.publish( self.key, item )
    
    def consume( self, callback ):
        # we need to unpickle, so we allways call _consume, and let _consume call the callback
        self.pubsub.subscribe( self.key )
        for item in self.pubsub.listen():
            if item['type'] == 'message':
                yield callback( item['data'], {} )
        return
    
