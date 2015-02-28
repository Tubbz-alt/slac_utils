from __future__ import absolute_import

import time as _time
import datetime
import pytz

import logging

UNIX_EPOCH = datetime.datetime(1970, 1, 1, 0, 0, tzinfo = pytz.utc)

def now( tz=pytz.utc ):
    return datetime.datetime.utcnow().replace( tzinfo=tz )
    
def now_as_epoch( ):
    return datetime_to_epoch( now() )

def datetime_to_epoch(dt):
    # logging.debug("  in : %s %s"%(type(dt), dt))
    d = dt - UNIX_EPOCH
    # 24*60*60 = 86400
    return d.seconds + (d.days*86400) + (d.microseconds*1e-6)
    # logging.debug("  out: %s %s"%(type(s), s))
    # return s
    
def sleep(s):
    _time.sleep(s)

def string_to_datetime( string, strp, tz=pytz.utc ):
    return datetime.datetime.strptime( string, strp ).replace( tzinfo=tz )

def add_tz( timestamp, tz=pytz.utc ):
    return timestamp.replace( tzinfo=tz )
        
def utcfromtimestamp( timestamp, tz=pytz.utc ):
    return add_tz( datetime.datetime.utcfromtimestamp( timestamp ), tz=tz )

def delta( ts_start, ts_end=None ):
    if ts_end == None:
        ts_end = now()
    delta = ts_end - ts_start
    return float( (float(delta.microseconds) + float(delta.seconds*1000000))/1000000 )