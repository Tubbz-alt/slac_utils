from __future__ import absolute_import

from re import sub, compile

import string as _string

def camel_case_to_underscore(name):
    s = sub('(.)([A-Z0-9][a-z0-9]+)', r'\1_\2', name)
    return sub('([a-z0-9])([A-Z])', r'\1_\2', s).lower()

def underscore_to_camelcase(word):
    return ''.join(x.capitalize() or '_' for x in word.split('_'))

ANSISEQUENCE = compile(r'\x1B\[[^A-Za-z]*[A-Za-z]')
VALID_CHARS = ''.join( ' ' if n < 32 or n > 126 else chr(n) for n in xrange(256) ) 

def strip_ansi(s):
    # return _string.translate( s.replace( ANSISEQUENCE, '' ), VALID_CHARS )
    return ANSISEQUENCE.sub( '', s )
    
def strip_non_ascii(s):
    return _string.translate( s, VALID_CHARS )
    
def dict_to_kv(d,keys=[],join_char=' ',missing_ok=True,modifier="'"):
    """
    convert a dict to a string, optionally use only the keys provided
    """
    s = []
    if len(keys) == 0:
        keys = d.keys()
    for k in sorted(keys):
        add = True
        if not k in d:
            if not missing_ok:
                raise KeyError( 'key %s is required but does not exist' % (k,))
            else:
                add = False
        if add:
            s.append( "%s=%s%s%s" % (k,modifier,d[k],modifier))
    return join_char.join(s)

def flatten( a, b ):
    """
    merge two dictionaries
    """
    # hstore can only store text values
    d = {}
    for k,v in dict( a.items() + b.items() ).iteritems():
        d[k] = str(v)
    return d

if __name__ == '__main__':
    for i in ( 'Pan5060', 'CiscoAsa' ):
        print "%s -> %s" % (i, camel_case_to_underscore(i) )