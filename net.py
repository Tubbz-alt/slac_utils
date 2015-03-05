import socket,struct
from ipaddress import IPv4Network, IPv4Address, IPv4Interface
from re import match, search

import logging


def dec2bin(n):    
    binstr = ''
    if n < 0:  raise ValueError, "must be a positive integer"
    if n == 0: return '0'
    while n > 0:
        binstr = str(n % 2) + binstr
        n = n >> 1
    return binstr

def netmask_to_prefixlen( n ):
    r = ''
    parts = n.split('.')
    if len(parts) == 4:
        for i in parts:
            r = r + dec2bin( int(i) )
    else:
        return None
    return r.count("1")

def prefixlen_to_netmask(len):
    mask = ''
    try:
        len = int(len)
    except:
        return None
        
    if len < 0 or len > 32:
        return None
    
    for t in range(4):
        if len > 7:
            mask += '255.'
        else:
            dec = 255 - (2**(8 - len) - 1)
            mask += str(dec) + '.'
        len -= 8
        if len < 0:
            len = 0
    
    return mask[:-1]


def make_mask(n):
    "return a mask of n bits as a long integer"
    logging.error("BITS: " + str(n) )
    return (2L<<int(n)-1) - 1

def dotted_quad_to_num(ip):
    "convert decimal dotted quad string to long integer"
    s = socket.inet_aton(ip)
    # logging.error("S: %s" % (s,))
    return struct.unpack('>L',s)[0]

def network_mask(ip,mask_or_len):
    "Convert a network address to a long integer" 
    a = mask_or_len.split('.')
    # logging.error("IN: %s" % (a,))
    bits = 0
    if len(a) == 4:
        bits = netmask_to_prefixlen(mask_or_len)
    elif len(a) == 1:
        try:
            bits = int(mask_or_len)
        except:
            return None
    else:
        return None
    return dotted_quad_to_num(ip) & make_mask(bits)


def valid_netmask_or_prefixlen( v ):
    ok = True
    a = v.split('.')
    if len(a) == 4:
        for i in a:
            if i < 0 or i > 255:
                ok = False
    elif len(a) == 1:
        try:
            if int(v) < 0:
                ok = False
        except:
            ok = False
    return ok
    
def address_in_network(ip,prefix,mask_or_len):
    "Is an address in a network"
    i = dotted_quad_to_num(ip)
    n = network_mask( prefix, mask_or_len )
    return i & n == n

def to_network(ip,mask_or_len=None):
    if mask_or_len:
        s = "%s/%s" % (ip, mask_or_len) 
    else:
        s = ip
    try:    
        return IPv4Interface( s ).network
    except:
        pass
        
    return None

def network_address( ip, mask_or_len ):
    return to_network( ip, mask_or_len ).network_address
    
def host_count( ip, mask_or_len ):
    return to_network( ip, mask_or_len ).num_addresses
    
def to_ip( n ):
    i = None
    try:
        i = IPv4Address( n )
    except:
        pass
    return i

def to_network_from_list( networks ):
    "given a list of networks, group them up into subnets and yield each distinct network"
    tally = {}
    for s in networks:
        t = s.prefix
        try:
            t = "%s" % (to_network( s.prefix, s.netmask ),)
        except:
            pass
        if not t in tally:
            tally[t] = []
        logging.error("      %s %s %s" % (s.prefix, s.netmask, t))
        tally[t].append( s )
                        
    i = tally.keys()
    if len(i) == 1:
        # all fit within the same network!
        prefix = i[0].split('/')
        logging.error("  PREFIX: %s" % (prefix[0],))
        # get the sorted first ip
        networks[0].prefix = prefix[0]
        yield networks[0]
    else:
        for s in networks:
            logging.error("  unknown network %s" % (s,))
            yield s
            
    return
    

def sort_networks( networks ):
    return sorted( networks, key=lambda n: IPv4Address(n.prefix) )
    
TRUNCATE_NAME = {
    'Fa':   'Fa',
    'FastEthernet': 'Fa',
    'Gi':   'Gi',
    'GigabitEthernet':  'Gi',
    'gigabitethernet':  'Gi',
    'gi':  'Gi',
    'Te':   'Te',
    'TenGigabitEthernet':   'Te',
    'e':    'Eth',
    'Eth':    'Eth',
    'Ethernet': 'Eth',
    'Vlan': 'Vl',
    'Vl':   'Vl',
    'port-channel': 'Po',
    'Port-channel': 'Po',
    'Po':   'Po',
    'Null': 'Null',
}
def truncate_physical_port( port ):
    """ normalise a physical port name """
    m = match( r'^(?P<speed>\D+)?\d+\S*', port )
    if m:
        s = m.group('speed')
        if s in TRUNCATE_NAME:
            port = port.replace( s, TRUNCATE_NAME[s] )
    return port

def extract_physical_port( str ):
    """ extract a physical port (truncated) name from a string """
    for i in str.split():
        # assume has slash character
        for n in TRUNCATE_NAME.keys():
            if len(n) > 1 and i.startswith( n ):
                s = truncate_physical_port( i )
                # logging.error("S: %s" % (s,))
                return s
    return None
    
    
def mac_address( mac, format='host' ):
    m = mac.replace('.','').replace(':','').lower()
    if len(m) < 12:
        # try to pad
        m = ''
        if ':' in mac:
            for i in mac.split(":"):
                if len(i) < 2:
                    i = '0%s' % i
                m = m + i
        if m < 12:
            raise Exception('not valid mac address')
    if format == 'host':
        a = [ m[i:i+2] for i in xrange(0,len(m),2) ]
        return ':'.join(a)
    elif format == 'net':
        a = [ m[i:i+4] for i in xrange(0,len(m),4) ]
        return '.'.join(a)
    return m