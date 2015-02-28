import remctl
from remctl import RemctlError

from os import environ
import logging

class Remctl( object ):
    """
    generic interface to remctl; run() is a generator object spitting out the message responses
    """
    command_preamble = []
    args = []

    def __init__(self, server, port, service, krb5ccname=None ):
        if krb5ccname == None and not 'KRB5CCAME' in environ:
            raise RemctlError, 'no token'
        self.server = server
        self.port = port
        self.service = service
        self.remctl = remctl.Remctl()

    def connect(self):
        try:
          res = self.remctl.open( host=self.server, port=self.port, principal=self.service )
        except RemctlError, e:
          logging.error( str(e) )
          raise e

    def run( self, args=[] ):
        """
        generator function to return the output live
        """
        if len(args) == 0:
            args = self.args
        
        c = self.command_preamble + args
        logging.error("Commands: " + str(c))
        self.remctl.command( c )
        t, o, s, st, e = self.remctl.output()
        m = ''
        while t != 'done':
            if t == 'output':
                m = o.rstrip()
            # elif t == 'status':
            #     x =  str(t) + ": " + str(st)
            elif t == 'error':
                m =  str(t) + ": " + str(e)
            yield m
            t, o, s, st, e = self.remctl.output()
        return

    def close(self):
        self.remctl.close()
        
    def __del__(self):
        self.close()
        
    
class NetShow( Remctl ):
    # requires in remctl/conf.d
    #  net show /opt/net-config/bin/net-config ANYUSER
    command_preamble = [ 'net', 'show', 'port' ]

class NetEdit( Remctl ):
    command_preamble = [ 'net', 'edit', 'port' ]
    
    