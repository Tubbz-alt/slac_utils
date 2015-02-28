import importlib
import pkgutil
import sys
from inspect import getfile
import imp

import logging

def inheritors(klass):
    subclasses = set()
    work = [klass]
    while work:
        parent = work.pop()
        for child in parent.__subclasses__():
            if child not in subclasses:
                subclasses.add(child)
                work.append(child)
    return subclasses


def create_klass( item ):
    # if klass is string, resolve to class name
    # check to make sure it's a child of Command?
    if isinstance( item, str ):
        m, k = item.rsplit(".", 1)
        module = importlib.import_module( m )
        return getattr(module,k)    
    return item
    

def path_of_module(m):
    return getfile( m )    

def modules_in_path(dirname):
    # logging.error("DI: %s" % (dirname,))
    for importer, package_name, _ in pkgutil.iter_modules([dirname]):
        full_package_name = '%s/%s' % (dirname, package_name) + '.py'
        # logging.error("FULL: %s %s" % (full_package_name, package_name) )
        yield package_name, full_package_name

def import_module( klass, package=None ):
    #logging.error("IMPORT: %s" % (klass,))
    return importlib.import_module( klass, package=package )
    # fp, p, desc = imp.find_module( klass, path )
    # return imp.load_module( klass, fp, p, desc )
    