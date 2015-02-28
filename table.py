from uuid import uuid1 as uuid
from os import environ
from getpass import getuser

from re import findall
from copy import copy
from operator import itemgetter

from netconfig import util

import sys, signal
from socket import getfqdn

try:
    from collections import OrderedDict
except:
    from ordereddict import OrderedDict
from texttable import Texttable
import datetime

import logging
import json

class Table( Texttable ):
    """
    inherited texttable to allow tsv and csv outputs
    """
    TABLE = 0
    CSV = 1
    TSV = 2
    
    def __len__(self):
        return len(self._rows)


    def draw(self, format=None):

        if format == None:
            format = self.TABLE

        if not self._header and not self._rows:
            return
        self._compute_cols_width()
        self._check_align()
        out = ""
        if format == self.TABLE and self._has_border():
            out += self._hline()
        if self._header:
            out += self._draw_line(self._header, isheader=True, format=format )
            if self._has_header():
                if format == self.TABLE:
                    out += self._hline_header()
                else:
                    out += "\n"
        length = 0
        for row in self._rows:
            length += 1
            out += self._draw_line(row, format=format )
            if format == self.TABLE:
                if self._has_hlines() and length < len(self._rows):
                    out += self._hline()
            else:
                out += "\n"
        if format == self.TABLE and self._has_border():
            out += self._hline()
        return out[:-1]

    def _draw_line(self, line, isheader=False, format=None ):
        if format == self.TABLE:
            line = self._splitit(line, isheader)
            space = " "
            out  = ""
            for i in range(len(line[0])):
                if self._has_border():
                    out += "%s " % self._char_vert
                length = 0
                for cell, width, align in zip(line, self._width, self._align):
                    length += 1
                    cell_line = cell[i]
                    fill = width - len(cell_line)
                    if isheader:
                        align = "c"
                    if align == "r":
                        out += "%s " % (fill * space + cell_line)
                    elif align == "c":
                        out += "%s " % (fill/2 * space + cell_line \
                                + (fill/2 + fill%2) * space)
                    else:
                        out += "%s " % (cell_line + fill * space)
                    if length < len(line):
                        out += "%s " % [space, self._char_vert][self._has_vlines()]
                out += "%s\n" % ['', self._char_vert][self._has_border()]
            return out

        out = ""
        delim = " "
        if format == self.CSV:
            delim = ','
        elif format == self.TSV:
            delim = "\t"
        if isheader:
            out += '# '
        for i in xrange(len(line)):
            out += str(line[i]) 
            if i < len(line) - 1:
                out += delim
        return out



# adapted from http://code.activestate.com/recipes/285264-natural-string-sorting/
def try_int(s):
    try: return int(s)
    except: return s

def nsort_key(s):
    # logging.warn("SORT: " + (s) + " -> " + str(findall(r'(\d+|\D+)', s)) )
    return map(try_int, findall(r'(\d+|\D+)', s))

def ncmp(a, b):
    return cmp(nsort_key(a), nsort_key(b))

def ncasecmp(a, b):
    return ncmp(a.lower(), b.lower())

def nsort(seq, cmp=ncasecmp, key=None):
    seq.sort(cmp, key=key)
    
def nsorted(seq, cmp=ncasecmp, key=None):
    t = copy(seq)
    nsort(t, cmp, key=key)
    # logging.warn('')
    return t



class JsonEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            return "%s" % (obj,) # iso?
        return json.JSONEncoder.default(self, obj)


# exhibit basically returns exactly the same view each time, but with a differing json call
class TableMixin( object ):

    # where to point for the api calls for teh json data
    columns = OrderedDict()
    
    # what field to use in columns for meta information
    mapping = {
        'fields': { 'field': None, 'default': None },
        'header': { 'field': 'label', 'default': None },
        'type': { 'field': 'type', 'default': 'a' },
        'halign': { 'field': 'halign', 'default': 'l' },
        'valign': { 'field': 'valign', 'default': 'm' },
    }
    
    value_remap = {}
    split_remap = {}
    _data = []
    
    def __init__(self ):
        pass
        
    def parse_meta( self, columns ):
        meta = {}
        cols = []

        for i in columns:
            if i in self.columns:
                for m, data in self.mapping.items():
                    v = self.mapping[m]['default']
                    if v == None:
                        v = i
                    f = self.mapping[m]['field']
                    
                    if f in self.columns[i]:
                        v = self.columns[i][f]
                    
                    if not m in meta:
                        meta[m] = []
                    meta[m].append( v )
                    
            # add colum
            cols.append( i )
            
        # logging.error( str(meta) )
        return meta, cols
    
    # def parse_filters(self, **kwargs):
    #     return **kwargs
    
    def add_row( self, d ):
        self._data.append(d)
    
    
    formats = {
        'tsv': Table.TSV,
        'csv': Table.CSV,
        None: Table.TABLE,
        'table': Table.TABLE,
        'json': 'json',
    }

    def render(self, filter={}, columns=[], width=132, format=None, sortby=None ):
        
        if format == 'json':
            return json.dumps( self._data, cls=JsonEncoder )
        
        if len(columns) == 0:
            for c in self.columns:
                columns.append(c)

        meta, cols = self.parse_meta( columns )

        table = Table( max_width=width )
        
        table.header( meta['header'] )
        table.set_cols_dtype( meta['type'] )
        table.set_cols_align( meta['halign'] )
        table.set_cols_valign( meta['valign'] )

        # logging.error("KWARGS: " + str(filter))
        self.kwargs = filter

        # default sortby
        
        stuff = self._data if sortby == None else nsorted( self._data, key=lambda k: k[sortby] )
        # for i in nsorted( self._data, key=lambda k: k[sortby] ):
        for i in stuff:
            this = []
            for c in cols:
                if c in i:
                    # value remap
                    text = str(i[c])
                    # deal with arrays
                    if type(i[c]) == list:
                        if c in self.split_remap:
                            parts = []
                            for j in i[c]:
                                parts.append(str(j))
                            text = ", ".join(parts)
                    if c in self.value_map:
                        if text in self.value_map[c]:
                            text = self.value_map[c][text]
                    this.append( text )

                else:
                    # raise Exception, "no " + str(c) + " in " + str(i)
                    this.append( '-' )
            # logging.debug( str(this) )
            table.add_row( this )
        
        return table.draw( format=self.formats[format] )



class PortTable( TableMixin ):
    columns = OrderedDict([
        ( 'port', { 'label': 'Port' } ),
        ( 'alias', { 'label': 'Alias', 'type': 't' } ),
        ( 'status', { 'label': 'Status' } ),
        # ( 'state', { 'label': 'Admin' } ),
        # ( 'protocol', { 'label': 'Proto' } ),
        ( 'type', { 'label': 'Port Type'} ),
        ( 'vlan', { 'label': 'VLAN(s)' } ),
        ( 'vlan_name', { 'label': 'Subnet(s)' } ),
        ( 'speed', { 'label': 'Speed' } ),
        ( 'duplex', { 'label': 'Duplex' } ),
        ( 'autoneg', { 'label': 'Autoneg' } )
    ])
    value_map = {
        # 'state': { 'True': 'up', 'False': 'down' },
        # 'protocol': { 'True': 'up', 'False': 'down' },
        'autoneg': { 'True': 'auto', 'False': 'fixed' },
        'alias': { 'None': '' },
        # 'trunk': { 'False': 'access', 'True': 'trunk' },
        'speed': { 'None': '-' },
        'duplex': { 'None': '-' },
    }
    split_remap = {
        'vlan': ',',
        'vlan_name': ',',
    }

class TransceiverTable( TableMixin ):
    columns = OrderedDict([
        ( 'port', { 'label': 'Port' } ),
        ( 'type', { 'label': 'Type' }  ),
        ( 'temperature', { 'label': 'Temp (C)', 'type': 'f', 'halign': 'r' } ),
        ( 'tx', { 'label': 'TX (dbm)', 'type': 'f', 'halign': 'r' } ),
        ( 'rx', { 'label': 'RX (dbm)', 'type': 'f', 'halign': 'r' } ),
    ])
    value_map = {}
    

class SpanningTreeTable( TableMixin ):
    columns = OrderedDict([
        ( 'port', { 'label': 'Port' } ),
        # ( 'vlan', { 'label': 'VLAN' } ),
        # ( 'mode', { 'label': 'Mode' } ),
        ( 'portfast', { 'label': 'Portfast' } ),
        # ( 'root_address', { 'label': 'Root' } ),
    ])
    value_map = {}

class HostTable( TableMixin ):
    columns = OrderedDict([
        ( 'ip', { 'label': 'IP Address' } ),
        ( 'netmask', { 'label': 'Netmask' } ),
        ( 'tag', { 'label': 'Tag', 'type': 'i', 'halign': 'r' } ),
        ( 'tag_desc', { 'label': 'Tag Description' } ),
    ])
    value_map = {}
