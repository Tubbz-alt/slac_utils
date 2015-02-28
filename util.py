import os
from re import match

def get_array( file ):
    """ given the file, reads in values and spits out the array """
    if file == "None" or file == '':
        return None
    f = []
    if not type(file) == list:
        f.append( file )
    else:
        f = file
    array = []

    # logging.error("determining f: " + str(f) + ", " + str(file))

    for i in f:
        try:
            f = open( i, 'r' )
            for l in f.readlines():
                # logging.error("L: " + str(l))
                h = l.rstrip()
                if not h == "":
                    array.append( h )
            f.close()
        except IOError, e:
            array.append( i )
    return array

            
def read_file_contents( file ):
    if file == None or file == "None" or file == '':
        return None

    if os.path.isdir( file ):
        return file

    # TODO: check for permissions etc
    f = open( file )
    password = f.readline()
    f.close()
    password = password.rstrip()

    #logging.debug("password is " + password )
    return password


def arrayify( i, key ):
    a = []
    if key in i:
        if isinstance( i[key], list ):
            a = i[key]
        else:
            a.append( i[key] )
    return a

def boolean( value ):
    """ given an input, returns whether it's true or false, or none """
    # logging.warn( "IN: '" + str( value ) + "'")

    if match( '(?i)y(es)?', str(value) ) or match( '(?i)t(rue)', str(value) ) or match( '(?i)e(nable)', str(value) ):
        return True
    elif match( '(?i)n(o)?', str(value) ) or match( '(?i)f(alse)', str(value) ) or match( '(?i)d(isable)', str(value) ):
        return False

    v = int( value )
    if v > 0:
        return True
    return False


class DictDiffer( object ):
    def __init__(self, past, current):
        self.current, self.past = current, past
        self.set_current, self.set_past = set(current.keys()), set(past.keys())
        self.intersect = self.set_current.intersection(self.set_past)
    def added(self):
        return self.set_current - self.intersect 
    def removed(self):
        return self.set_past - self.intersect 
    def changed(self):
        return set(o for o in self.intersect if self.past[o] != self.current[o])
    def unchanged(self):
        return set(o for o in self.intersect if self.past[o] == self.current[o])