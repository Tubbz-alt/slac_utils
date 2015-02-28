"""
This file demonstrates writing tests using the unittest module. These will pass
when you run "manage.py test".

Replace this with more appropriate tests for your application.
"""

from django.test import TestCase
from ptolemy_py.util.models import Process, Manager
from ptolemy_py.util.logger import QueueHandler

import logging
# logging.basicConfig( level=logging.DEBUG, format='%(levelname)-8s %(message)s' )


def function_call( arg ):
    return arg


class Simple( Process ):
    def run(self):
        return self.work_item

class ProcessTest(TestCase):

    def test_simple_init(self):
        p = Process()
        self.assertEqual(type(p).__name__, 'Process')

    def test_simple_extend( self ):
        d = { 'bollocks': 'to this' }
        x = Simple( d )
        s = x.start()
        self.assertEqual( s, d )
        
class ProcessPoolManagerTest( TestCase ):
    
    def test_init( self ):
        p = ProcessPoolManager()
        self.assertEqual( type(p).__name__, 'ProcessPoolManager' )
        
    def test_run( self ):
        p = ProcessPoolManager( min_children=1 )
        p.start()
        self.assertEqual( True, True )
        
        
class QueueHandlerTest( TestCase ):
    
    def setUp(self):
        h = QueueHandler()
        # self.assertEqual( type(h).__name__, 'QueueHandler' )
        self.h = h
        
    def test_init(self):
        l = logging.getLogger('tester')
        l.addHandler(self.h)
        l.debug('debug test')