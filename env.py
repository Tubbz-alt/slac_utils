from socket import getfqdn
from getpass import getuser
from os import environ


def get_user( fqdn=True ):
    """ 
    get the username from (in order)
    1) REMOTE_USER (for web apps)
    2) local + fqdn (if req)
    """
    user = getuser()
    if fqdn:
        dn = str(getfqdn()).split('.')
        user = "%s@%s" % (user, '.'.join(dn[1:]).upper() )
    #logging.debug("LOCAL USER: %s REMOTE_USER: %s" % ( user, environ['REMOTE_USER'] if 'REMOTE_USER' in environ else None,))
    if 'REMOTE_USER' in environ: user = environ['REMOTE_USER']
    return user
    
if __name__ == "__main__":
    print get_user( fqdn=False )
