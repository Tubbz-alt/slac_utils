try:
   import cPickle as pickle
except:
   import pickle

class Message( dict ):
    """
    generic message for transport
    timestamp = <datetime>,
    _meta = {
        'task_id': <str>,
        'type': 'task',
        'key':  <str>,
        'key_name' = []
    },
    context = {
        # contains items that this data refers to, eg the
    }
    data = [
    {},
    ]
    """
    
    def __init__( self, meta={}, context={}, data={} ):
        dict.__init__(self,{
            '_meta': meta,
            'context': context,
            'data': data
        })

    def __getitem__(self, key):
        return super(Message, self).__getitem__(key)
    __getattr__ = __getitem__
    
    def __setitem__(self, key, value):
        return super(Message,self).__setitem__(key, value)
    __setattr__ = __setitem__

    def __delitem__(self,key):
        return super(Message,self).__delitem__(key)
    __delattr__ = __delitem__

    def __getstate__(self):
        return self.__dict__.copy()

    def __setstate__(self, dict):
        self.__dict__.update(dict)
        
    def copy(self):
        return pickle.loads(pickle.dumps(self))
    def __deepcopy__(self,memo):
        return self.copy()


if __name__ == "__main__":
    m = Message()
    print("message %s" % (m,))
    
    m['context'] = { 'hello': 'there' }
    print("message %s" % (m,))


    m.context = { 'hello': 'there again' }
    print("message %s" % (m,))
    
    
    for k,v in m.iteritems():
        print("iteritems %s\t%s"%(k,v))
        
    p = pickle.dumps(m)
    print("pickle %s" % (p,))

    # print("unpickle %s" % (pickle.load(open(p)))