from slac_utils.time import now, datetime_to_epoch, sleep
import datetime
from slac_utils.queues import PyAmqpLibQueue
from slac_utils.hashing import ConsistentHashRing

from collections import deque

import gc

from struct import pack
from pickle import dumps
import socket

import logging


class StatisticsMixin( object ):
    """
    a supervisor that relays statistics of workers back to a store 
    """
    stats_feeder = None
    stats_feeder_obj = None
    stats_premable = ''
    
    def init_stats(self,*args,**kwargs):
        if self.stats_feeder and 'stats_host' in kwargs and 'stats_port' in kwargs:
            f = getattr( self, 'stats_feeder' )
            self.stats_feeder_obj = f( host=kwargs['stats_host'], port=kwargs['stats_port' ])

    def statistics_key( self, job ):
        return self.stats_preamble + '.' + str(job['device'])

    def process_stats(self, job, retries=1 ):
        if 'stats' in job['_meta']:
            t = now()
            # logging.warn("sending stats: " + str(t) + ', key=' + str(self.statistics_key(job)) + ": " + str(job['_meta']['stats']))
            try:
                self.stats_feeder_obj.send( t, self.statistics_key(job), statistics=job['_meta']['stats'] )
            except Exception,e:
                logging.error("Could not send statistics: " + str(e))
        

class StatisticsFeeder( object ):
    """
    generic agent to push statistics to something
    """
    stats_messages = None
    
    def __init__(self,*args,**kwargs):
        for k,v in kwargs.iteritems():
            setattr( self, k, v )
        self.stats_messages = deque()
        self.init(*args,**kwargs)
        
    def init(self,*args,**kwargs):
        pass

    def __enter__(self):
        return self
        
    def __exit__(self,*args,**kwargs):
        pass
    
    def send( self, statistics={} ):
        raise NotImplementedError, 'not implemented'


class PyAmqpLibFeeder( StatisticsFeeder ):
    queue = None 
    def init(self,*args,**kwargs):
        logging.debug("ARGS: %s KWARGS: %s"%(args,kwargs))
        super( PyAmqpLibFeeder, self ).init(*args,**kwargs )
        if kwargs.has_key('key_premable'):
            del kwargs['key_preamble'] 
        else:
            self.key_preamble = ''
        self.queue = PyAmpqLibQueue(**kwargs)

    def __enter__(self):
        self.queue.__enter__()
    def __exit__(self):
        self.queue.__exit__()

    def send( self, time, key, statistics={}, retries=3 ):
        if isinstance( time, datetime.datetime ):
            time = datetime_to_epoch( time )
        for k,v in statistics.iteritems():
            self.queue.put( '%f %d' % (v,time), key=self.key_preamble + '.' + k )


class CarbonFeeder( StatisticsFeeder ):
    """
    A generic class to send statistics to graphite/carbon
    """
    pickle = True
    sock = None
    state = False
    backing_off = False
    backoff_time = 0.5
    backoff_window = 0
    
    def init(self,*args,**kwargs):
        super( CarbonFeeder, self ).init(*args,**kwargs)
        self.pickle = True
        self.backing_off = False
        self.backoff_window = 0
        self.__enter__()
        
    def __enter__(self):
        try:
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            logging.info("connecting %s with %s:%s (%r) msgs: %d" % ( self, self.host,self.port,self.sock, len(self.stats_messages), ) )
            # self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.sock.connect( ( self.host, int(self.port) ) )
            self.sock.settimeout( 2*self.backoff_time )
            # logging.info("  connect ok, timeout %s" % (self.sock.gettimeout()) )
            self.state = True
        except socket.error, e:
            self.state = False
            # logging.warn("  connect not ok: %s" % (e,))
            if self.sock:
                self.sock.close()
            self.sock = None
            logging.warn("  could not connect to %s:%s: %s" %(self.host,self.port,e) )
        return self

    def __exit__(self,*args,**kwargs):
        if self.sock:
            self.sock.close()
        self.state = False
        self.sock = None

    def __del__(self):
        # flush cache out to host, if it's down, then try 3 times (each 10x backoff time wait)
        self.backoff_time = self.backoff_time * 3
        while len(self.stats_messages) > 0:
            n = self._send( 100 )
            logging.info("flushing... %s" % (n))
            
    def reconnect( self ):
        self.__exit__()
        sleep( self.backoff_time )
        self.__enter__()
        
    def _send( self, number ):

        size = len(self.stats_messages)
        if number > size:
            number = size

        if not self.state or not self.sock:
            self.reconnect()

        # this = deque()
        this = []
        try:

            if self.sock == None:
                raise Exception, 'no socket available'

            ts = now()
            
            # as we're using deque, we have to pop if (no peeking)
            for x in xrange(0,number):
                this.append(self.stats_messages.popleft())
                
            # try sending the stuff
            # logging.debug("sending %s" % this)
            payload = dumps(this)
            header = pack("!L", len(payload))
            self.sock.sendall( header + payload )

            # time it and report
            ts = now() - ts
            ts = "%.3f" % float( (float(ts.microseconds) + float(ts.seconds*1000000))/1000000 )
            logging.info("%s:%s sent %s datapoints (%s left) in %ssec" % (self.host, self.port, number, size - number, ts ) )

            return len(this)
                    
        except Exception,e:
            self.__exit__()
            # don't loose the data! put it back into the deque
            if len(this) > 0:
                self.stats_messages.extendleft(this)
            logging.warning("%s:%s send error: %s %s" % ( self.host, self.port, type(e), e ))
            
        # logging.error('could not store stats %s (count %s)' % (key,len(self.stats_messages)))
        return None

    def load_from_disk( self ):
        pass
        
    def page_to_disk( self ):
        """
        in order to prevent hogging up memory, if the len(self.stats_message) gets too large
        then we page the data off to disk
        """
        pass

    def send( self, time, key, statistics, min_chunks=500, max_chunks=750, backoff_threshold=25000 ):
        """
        try to be a little clever in not stalling the updates as we can keep a cache on this
        system, we use two variables: backing_off and backoff_window. if the grpahite server is
        stalled, we set backing_off to true, and we do not attempt to send anything until we 
        have accumulated max_chunks more items (since the last try)
        """
        if isinstance( time, datetime.datetime ):
            time = datetime_to_epoch( time )
        
        gc.disable()
        for k,v in statistics.iteritems():
            try:
                v = float(v)
                this_key = "%s.%s" % (key,k)
                this_key = this_key.replace( '/', '.' )
                # logging.info(" %s: %s\t%s" % ( time, this_key, v ))
                self.stats_messages.append( ( this_key, (time, v) ) )
                self.backoff_window = self.backoff_window + 1
            except Exception, e:
                logging.error("Error parsing %s: (%s) %s in %s" % (v, type(e), e, key) )
        gc.enable()

        # facility not hammering the failed host too much and thus slowing us down
        # we use self.backoff_window as a counter for outstanding messages
        # if this value goes over max_chunks, we try sending again
        if self.backing_off:
            if self.backoff_window > backoff_threshold:
                self.backing_off = False
                self.backoff_window = self.backoff_window - backoff_threshold

        # okay to send
        if not self.backing_off:
            # send! if we succeed, then good
            # if we're backing off, then try sending max_chunks, else min_chunks  
            # also, try to avoid bursting after backing_off?
            if self.backoff_window >= min_chunks:

                size = len(self.stats_messages)              # send everything we have
                num = size
                if num > max_chunks:
                    num = max_chunks    # limit number of items sent

                # send the data
                sent = self._send( num )
                if not sent == None:
                    self.backing_off = False
                    self.backoff_window = self.backoff_window - sent   
                    if self.backoff_window < 0:
                        self.backoff_window = 0 # important!                     
                    # logging.info("%s:%s after %s\tsize %s/%s window %s: %s"%( self.host, self.port, sent, size, len(self.stats_messages), self.backoff_window, self.backing_off ) )
                    return True
                    
                # if sending fails, then we wait another width before we try again
                else:
                    # use fact that we should have sent the entire size if we aren't backing offc
                    self.backing_off = True
                    # logging.error("%s %s: send failed: num %s size %s window %s, backoff %s" % (self.host, self.port, num, len(self.stats_messages), self.backoff_window, self.backing_off ) )
                    return False

        return None


class MultiCarbonFeeder( StatisticsFeeder ):
    instance_ports = {}
    ring = None
    feeders = {}
    def init(self,**kwargs):
        super( MultiCarbonFeeder, self).init(**kwargs) 
        # expects self.instances = [ 'ip:port:instance', 'ip:port:instance', ]
        if not len(self.instances) > 0:
            raise Exception, 'no carbon instances defined, use CARBON_INSTANCES'
        self.instance_ports = {} # { (server, instance) : port }
        self.ring = ConsistentHashRing([])
        self.feeders = {}
        for i in self.instances:
            s, p, n = i.split(':')
            self.add_instance( s, p, n )
            # connect to each instance
            self.feeders[(s,p,n)] = CarbonFeeder( host=s, port=p )
    
    def __exit__(self,*args,**kwargs):
        for i in self.feeders:
            self.feeder.__exit__(*args,**kwargs)
    
    def add_instance(self,server,port,instance):
        if (server, instance) in self.instance_ports:
          raise Exception("destination instance (%s, %s) already configured" % (server, instance))
        self.instance_ports[ (server, instance) ] = port
        self.ring.add_node( (server, instance) )
        
    def remove_instance(self,server,port,instance):    
        if (server, instance) not in self.instance_ports:
          raise Exception("destination instance (%s, %s) not configured" % (server, instance))
        del self.instance_ports[ (server, instance) ]
        self.ring.remove_node( (server, instance) )
    
    def get_instance(self,key):
        (s, i) = self.ring.get_node(key)
        p = self.instance_ports[ (s, i) ]
        k = (s, p, i)
        if k in self.feeders:
            # logging.info("%s:%s:%s" % (s, p, i))
            return self.feeders[k]
        raise Exception, 'could not find feeder for %s:%s:%s' % (s, p, i)
        
    def send( self, time, key, statistics={}, min_chunks=500, max_chunks=1000, backoff_threshold=25000 ):
        # logging.info("sending...")
        # as statistics is a hash, we need to append concat with key to get appropriate feeder
        data = {}
        for k,v in statistics.iteritems():
            this_key = "%s.%s" % (key,k)
            this_key = this_key.replace( '/', '.' )
            # for f in self.get_instances(this_key):
            f = self.get_instance(this_key)
            if not f in data:
                data[f] = {}
            data[f][k] = v
        for f in data:
            f.send( time, key, statistics=data[f], min_chunks=min_chunks, max_chunks=max_chunks, backoff_threshold=backoff_threshold )
