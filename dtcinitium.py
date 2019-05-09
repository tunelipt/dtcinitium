"""
This module handles communication with the DTC Initium 
"""

import socket
import numpy as np
from select import select
import threading
import time
import struct
from collections import namedtuple


class CmdParser(object):
    """
    Parses DTC Initium commands.

    This class could be incorporated into `DTCInitiumCore`.

    A detailed description of the commands is available in the user manual of the
    DTC Initium.
    """
    
    def __init__(self, crs=111):
        """
        `cmd = CmdParser(crs=111)`

        Creates a command parser for DTC Initium python interface.

         * `crs` Not used, present for legacy reasons.
        """
        self.crs = crs
        return
    
    def SD1(self, scn=1, npp=64, lrn=1, scnlst=None):
        """
        Configures scanners connected to the frame.
        
        
        """
        
        if scnlst is None:
            scnlst = [[scn, npp, lrn]]
        cmd = 'SD1 %d' % self.crs
        
        for s in scnlst:
            cmd += " (%s %d %d)" % (s[0], s[1], s[2])
        
        return cmd + ";\n"
    def SD2(self, stbl=1, nfr=64, nms=1, msd=500, trm=0, scm=1, ocf=2):
        frd = 0
        cmd = "SD2 %d %d (%d %d) (%d %d) (%s %s) %d;\n" % \
        (self.crs, stbl, nfr, frd, nms, msd, trm, scm, ocf)
        return cmd
    
    def SD3(self, stbl=1, *sport):
        cmd = "SD3 %d %d" % (self.crs, stbl)
        for s in sport:
            cmd += ", %s" % s
            
        return cmd + ";\n"
            
    def SD5(self, stbl=1, actx=1):
        cmd = "SD5 %d %d %d;\n" % (self.crs, stbl, actx)
        return cmd
    
    def PC4(self, unx=3, fctr=None, lrn=1):
        cmd = "PC4 %d %d" % (lrn,unx)
        if fctr is not None:
            cmd += " %s" % fctr
        return cmd + ";\n"
    def CV1(self, valpos, puldur):
        cmd = "CV1 %d %d;\n" % (valpos, puldur)
        return cmd

    def CP1(self, puldur):
        return "CP1 %d;\n" % puldur

    def CP2(self, stbtim):
        return "CP2 %d;\n" % stbtim

    def CA2(self, lrn=1):
        cmd = "CA2 %s;\n" % lrn
        return cmd
    
    def OP2(self, stbl, *sport):
        cmd = "OP2 %d -%d" % (self.crs, stbl)
        
        for s in sport:
            cmd += ", %s" % s
        return cmd + ";\n"
    def OP3(self, stbl, *sport):
        cmd = "OP3 %d %d" % (self.crs, stbl)
        
        for s in sport:
            cmd += ", %s" % s
        return cmd + ";\n"
    def OP5(self, stbl):
        return "OP5 %d %d;\n" % (self.crs, stbl)
    
    
    def AD0(self):
        return "AD0;\n"
    
    def AD2(self, stbl=1, nms=None):
        cmd = "AD2 %d" % stbl
        if nms is not None:
            cmd += " %s" % nms
        return cmd + ";\n"
    
    def LA1(self, sport):
        return "LA1 %d %s;\n" % (self.crs, sport)
    def LA4(self):
        return "LA4 %d;\n" % self.crs


# Container for type 4 packet

Packet04 = namedtuple('Packet04', ['code', 'type', 'msg', 'warn'])

# Container for type 128 packer (error packet)
Packet128 = namedtuple('Packet128', ['code', 'type', 'msg', 'error'])

# Container for type 8 packet (integer values)
Packet08 = namedtuple('Packet08', ['code', 'type', 'msg', 'value'])

# Container for type 8 packet (32 bit floats values)
Packet09 = namedtuple('Packet09', ['code', 'type', 'msg', 'value'])

# Container for type 33 packet (Binary array data format)
Packet33 = namedtuple('Packet33', ['code', 'type', 'msg', 'nrows', 'ncols', 'data'])

# Container for type 16 packet (Binary stream 2 byte raw data)
Packet16 = namedtuple('Packet16', ['code', 'type', 'msg', 'msnum', 'nvals', 
                                 'iutype', 'stbl', 'nfr', 'cnvt', 
                                 'seq', 'data'])
# Container for type 17 packet (Binary stream 3 byte raw data)
Packet17 = namedtuple('Packet17', ['code', 'type', 'msg', 'msnum', 'nvals', 
                                 'iutype', 'stbl', 'nfr', 'cnvt', 
                                 'seq', 'data'])
# Container for type 19 packet (Binary stream 32 bit float data)
Packet19 = namedtuple('Packet19', ['code', 'type', 'msg', 'msnum', 'nvals', 
                                 'iutype', 'stbl', 'nfr', 'cnvt', 
                                 'seq', 'data'])




class DTCInitiumCore(object):
    """
    `DTCInitiumCore`

    Low level interface to the DTC Initium. Handles most communication and implements the
    the basic API. It can be used to debug and develop new functionality but the higher
    level interface `DTCInitium`
    
    """
    def __init__(self, ip='192.168.129.7'):
        """
        `dev = DTCInitiumCore(ip='192.168.129.7')`

         * `ip` - IP address of the DTC Initium.

        """
        # Socket that communicates with the DTC Initium
        self.ip = ip
        self.open()
        self.cmd = CmdParser(111)
        self.methods = dict(SD1=self.SD1, SD2=self.SD2, SD3=self.SD3, 
                            SD5=self.SD5, PC4=self.PC4, CV1=self.CV1, 
                            CP1=self.CP1, CP2=self.CP2, CA2=self.CA2,
                            OP2=self.OP2, OP3=self.OP3, OP5=self.OP5, 
                            AD0=self.AD0, AD2=self.AD2, 
                            LA1=self.LA1, LA4=self.LA4)
        self.s.settimeout(3)

    def open(self):
        self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        time.sleep(0.2)
        self.s.connect( (self.ip, 8400) )
        return
    
    def socket(self):
        "Returns the socket used to communicate with the DTC Initium"
        return self.s
    
    def __del__(self):
        "Destructor. Remember to close the socket "
        self.close()
    
    def close(self):
        "Actually close the socket"
        self.s.close()
        return
    
    def is_pending(self, timeout=0.5):
        """
        `dev.is_pending(timeout=0.5)`

         * `timeout` - Timeout in seconds that the function should wait.
         
        Check whether information is still comming in from the socket. Not used for the DTC Initium since no
        simple text response is available from the DTC Initium.
        """
        r, w, x = select([self.s], [], [], timeout)
        if r == []:
            return None
        else:
            return True    
    def read_packet04(self, c, t, m):
        """
        Read a type 04 packet
        
        `self.read_packet04(c, t, m)`

         *  `c` - Response code
         *  `t` - Type of response. Should be 4
         *  `m` - MsgLen

         After reading and parsing the 4 bytes header, this function will read the rest
         of the  information (4 bytes) which is a positive number if a warning was issued 0
         for normal operations.
         """
        warn  = struct.unpack('>i', self.s.recv(4))[0]
        return Packet04(c, t, m, warn)
    
    def read_packet128(self, c, t, m):
        """
        Read a type 128 packet
        
        `self.read_packet128(c, t, m)`

         *  `c` - Response code
         *  `t` - Type of response. Should be 4
         *  `m` - MsgLen

         After reading and parsing the 4 bytes header, this function will read the rest of the information.

         This is an error packet and is returned whenever the DTC Initium returns an error.
         """
        err  = struct.unpack('>i', self.s.recv(4))[0]
        return Packet128(c, t, m, err)
    
    def read_packet08(self, c, t, m):
        """
        Read a type 08 packet
        
        `self.read_packet08(c, t, m)`

         *  `c` - Response code
         *  `t` - Type of response. Should be 4
         *  `m` - MsgLen

         After reading and parsing the 4 bytes header, this function will read the remaining 4 bytes
         into a 32 bit integer.
         """
        
        val  = struct.unpack('>i', self.s.recv(4))[0]
        return Packet08(c, t, m, val)
    
    def read_packet09(self, c, t, m):
        """
        Read a type 09 packet
        
        `self.read_packet09(c, t, m)`

         *  `c` - Response code
         *  `t` - Type of response. Should be 4
         *  `m` - MsgLen

         After reading and parsing the 4 bytes header, this function will read the remaining 4 bytes
         into a 32 bit float.
         """
        val  = struct.unpack('>f', self.s.recv(4))[0]
        return Packet09(c, t, m, val)
    
    def read_packet33(self, c, t, m):
        """
        Read a type 33 packet
        
        `self.read_packet33(c, t, m)`

         *  `c` - Response code
         *  `t` - Type of response. Should be 4
         *  `m` - MsgLen

         After reading and parsing the 4 bytes header, this function will read the remaining 4 bytes
         which contain the number of rows and columns of a 4 byte array that is read in sequence.
         """
        nrows, ncols  = struct.unpack('>HH', self.s.recv(4))
        dt = np.dtype('>i4')
        
        buffer = self.s.recv(nrows*ncols*4)
        
        data = np.frombuffer(buffer, dt).reshape(nrows, ncols)
        return Packet33(c, t, m, nrows, ncols, data)
    
    def read_packet16(self, c, t, m):
        """
        Read a type 16 packet
        
        `self.read_packet16(c, t, m)`

         *  `c` - Response code
         *  `t` - Type of response. Should be 4
         *  `m` - MsgLen

         After reading and parsing the 4 bytes header, this function will read the remaining 4 bytes
         which contain the message number and the number of values in the stream. Then there are
         several details of the data acquisition and a sequence of two byte integers.
        
         """
        msnum, nvals = struct.unpack('>HH', self.s.recv(4))
        tpl = struct.unpack('>BBBBBBBBBBBBHBB', self.s.recv(16))
        
        iutyp = tpl[3]
        stbl = tpl[4]
        nfr = tpl[5]
        conv = tpl[13]
        seq = tpl[14]
        
        dt = np.dtype(np.int16)
        dt = dt.newbyteorder('>')
        data = np.frombuffer(self.s.recv(nvals*2), dtype=dt)
        return Packet16(c, t, m, msnum, nvals, iutyp, stbl, nfr, 
                        conv, seq, data)

    def read_packet17(self, c, t, m):
        """
        Read a type 17 packet
        
        `self.read_packet17(c, t, m)`

         *  `c` - Response code
         *  `t` - Type of response. Should be 4
         *  `m` - MsgLen

         After reading and parsing the 4 bytes header, this function will read the remaining 4 bytes
         which contain the message number and the number of values in the stream. Then there are
         several details of the data acquisition and a sequence of 3 byte integers. For now, the raw bytes
         are read.
        
         """
        msnum, nvals = struct.unpack('>HH', self.s.recv(4))
        tpl = struct.unpack('>BBBBBBBBBBBBHBB', self.s.recv(16))
        
        iutyp = tpl[3]
        stbl = tpl[4]
        nfr = tpl[5]
        conv = tpl[13]
        seq = tpl[14]
        
        dt = np.dtype(np.int8)
        data = np.frombuffer(self.s.recv(nvals*3), dtype=dt)
        return Packet17(c, t, m, msnum, nvals, iutyp, stbl, nfr, 
                        conv, seq, data)
        
    
    def read_packet19(self, c, t, m):
        """
        Read a type 19 packet
        
        `self.read_packet19(c, t, m)`

         *  `c` - Response code
         *  `t` - Type of response. Should be 4
         *  `m` - MsgLen

         After reading and parsing the 4 bytes header, this function will read the remaining 4 bytes
         which contain the message number and the number of values in the stream. Then there are
         several details of the data acquisition and a sequence of 32 bit floats is read.
        
         """
        msnum, nvals = struct.unpack('>HH', self.s.recv(4))
        tpl = struct.unpack('>BBBBBBBBBBBBHBB', self.s.recv(16))
        
        iutyp = tpl[3]
        stbl = tpl[4]
        nfr = tpl[5]
        conv = tpl[13]
        seq = tpl[14]
        
        dt = np.dtype(np.float32)
        dt = dt.newbyteorder('>')
        data = np.frombuffer(self.s.recv(nvals*4), dtype=dt)
        return Packet19(c, t, m, msnum, nvals, iutyp, stbl, nfr, 
                        conv, seq, data)

        
    def response(self, err=True):
        """
        Read a response package from the DTC Initium

        `self.response(err=True)`

         * `err` - Raise an error if type 128 packet is read.

        Reads a 4 byte header of the incoming packet. Then call a specialized
        method to read the rest of the exact packet.
        """
        c, t, m = struct.unpack('>BBH', self.s.recv(4))
        if t == 4:
            pack = self.read_packet04(c, t, m)
        elif t==8:
            pack = self.read_packet08(c, t, m)
        elif t==9:
            pack = self.read_packet09(c, t, m)
        elif t==16:
            pack = self.read_packet16(c, t, m)
        elif t==17:
            pack = self.read_packet17(c, t, m)
        elif t==19:
            pack = self.read_packet19(c, t, m)
        elif t==33:
            pack = self.read_packet33(c, t, m)
        elif t==128:
            pack = self.read_packet128(c, t, m)
            if err:
                raise RuntimeError("Error code: %s, msg=%s - ERROR: %s" % (c, m, pack.error))
            
        else:
            raise RuntimeError("Unknown packet type %d!"%t)
        return pack

    def simpleacquire(self, stbl=1, nms=None):
        """
        Execute a data acquisition

        `self.simpleacquire(stbl=1, nms=None)`

         * `stbl` Setup table used
         * `nms` Number of measurement sets to read.

         This method doesn't do any configuration and just executes the AD2 command and reads the responses
         until a type 4 packet or an error is read.
        """
        self.AD2(stbl, nms)
        data = []
        while True:
            p = self.response()
            if p.type == 4 or p.type ==128:
                break
            else:
                data.append(p)
        return data
    
    def SD1(self, scn=1, npp=64, lrn=1, scnlst=None):
        cmd = self.cmd.SD1(scn, npp, lrn, scnlst)
        self.s.send(cmd.encode())
        return None
    def SD1str(self, args):
        cmd = "SD1 %d %s;\n" % (self.cmd.crs, args)
        self.s.send(cmd.encode())
        
        
    def SD2(self, stbl=1, nfr=64, nms=1, msd=500, trm=0, scm=1, ocf=2):
        cmd = self.cmd.SD2(stbl, nfr, nms, msd, trm, scm, ocf)
        self.s.send(cmd.encode())
        return None
    
    def SD3(self, stbl=1, *sport):
        cmd = self.cmd.SD3(stbl, *sport)
        self.s.send(cmd.encode())
        return None
            
    def SD5(self, stbl=1, actx=1):
        cmd = self.cmd.SD5(stbl, actx)
        self.s.send(cmd.encode())
        return None
    
    def PC4(self, unx=3, fctr=None, lrn=1):
        cmd = self.cmd.PC4(unx, fctr, lrn)
        self.s.send(cmd.encode())
        return None

    def CV1(self, valpos, puldur):
        cmd = self.cmd.CV1(valpos, puldur)
        self.s.send(cmd.encode())
        return None
    
    def CP1(self, puldur):
        cmd = self.cmd.CP1(puldur)
        self.s.send(cmd.encode())
        return None
    
    def CP2(self, stbtim):
        cmd = self.cmd.CP2(stbtim)
        self.s.send(cmd.encode())
        return None
    
    def CA2(self, lrn=1):
        cmd = self.cmd.CA2(lrn)
        self.s.send(cmd.encode())
        return None
    
    def OP2(self, stbl, *sport):
        cmd = self.cmd.OP2(stbl, *sport)
        self.s.send(cmd.encode())
        return None
    
    def OP3(self, stbl, *sport):
        cmd = self.cmd.OP3(stbl, *sport)
        self.s.send(cmd.encode())
        return None
    
    def OP5(self, stbl):
        cmd = self.cmd.OP5(stbl)
        self.s.send(cmd.encode())
        return None
    
    
    def AD0(self):
        cmd = self.cmd.AD0()
        self.s.send(cmd.encode())
        return None
    
    def AD2(self, stbl=1, nms=None):
        cmd = self.cmd.AD2(stbl, nms)
        self.s.send(cmd.encode())
        return None
    
    def LA1(self, sport):
        cmd = self.cmd.LA1(sport)
        self.s.send(cmd.encode())
        return None
    
    def LA4(self):
        cmd = self.cmd.LA4()
        self.s.send(cmd.encode())
        return None
    def cmdstr(self, scmd):
        self.s.send(scmd.encode())
        return None
    def cmd(self, cmd, *args, **argkeys):
        self.methods[cmd](*args, **argkeys)
        return None
    



def parse_range(s):
    """
    Parses a string containing an integer or a range of integers.

    Accepts strings such as '123' or '1-20'.
    """
    lims = [int(x) for x in s.split('-')]
    if len(lims) > 2:
        raise ValueError("Expected 'x-y' where x and y are integers. Got %s instead!" % s)
        
    return lims



def scanner_port(s):
    """
    Break up a port name into scanner and port number.
    The naming convention used by the DTC Initium is that
    pressure ports are named XYZ where X is the number of the
    scanner and YZ is the port number in said scanner.

    This function receives a port name and makes sure it is a
    string of the format "XYZ". If it is an integer, the integer
    is transformed into a such a string. The integers `X` and `XY`
    are returned.
    """
    s = str(s)
    if len(s) != 3:
        raise ValueError("Port %s is not a valid port" % s)
    sc = int(s[0])
    p = int(s[1:])
    
    return sc, p

class Scanners(object):
    """
    This class is used to parse scanner lists. The DTC Initium is very flexible
    in how the scanners are configured. Eight scanners can be connected to
    the DTC Initium and the scanners can have different number of pressure ports.
    Groups of scanners can be logically associated as well (so that they can be zeroed
    together or  use the same units.

    Arguments:
    
     * `scn` Specifies the scanners.
     * `npp` Default number of pressure ports per scanner
     * `lrn` Default lrn (logical range number)
     
    The simplest case is when the values of `npp` and `lrn` are the
    same for all scanners. The simples way is to specify the range of scanners. Individual groups can
    be specified if `scn` is a list.

    ```python
    In [13]: from dtcinitium import Scanners                                        
    
    In [14]: s = Scanners("1-8"); s.scanners                                        
    Out[14]: [([1, 2, 3, 4, 5, 6, 7, 8], 64, 1)]
    
    In [15]: s = Scanners("1"); s.scanners                                          
    Out[15]: [([1], 64, 1)]
    
    In [16]: s = Scanners("1", 32, 2); s.scanners                                   
    Out[16]: [([1], 32, 2)]
    
    In [17]: s = Scanners([1,4,5]); s.scanners                                      
    Out[17]: [([1], 64, 1), ([4], 64, 1), ([5], 64, 1)]
    
    In [18]: s = Scanners([("1-4", 32, 1), ("5-8", 64, 2)]); s.scanners             
    Out[18]: [([1, 2, 3, 4], 32, 1), ([5, 6, 7, 8], 64, 2)]
    
    In [19]: s = Scanners([("1-4", 32, 1), ("5-8")]); s.scanners                    
    Out[19]: [([1, 2, 3, 4], 32, 1), ([5, 6, 7, 8], 64, 1)]
        
    ```

    The class also builds, for each scanner, a dictionary containing the
    number of ports available:
    ```python
    In [27]: s = Scanners([("1-2", 64,1), ("3-4", 32, 1), (5, 16,1)]); s.scanners   
    Out[27]: [([1, 2], 64, 1), ([3, 4], 32, 1), ([5], 16, 1)]
    In [29]: s.nports                                                               
    Out[29]: {1: 64, 2: 64, 3: 32, 4: 32, 5: 16}
    ```

    After defining the scanners, to build the argument to DTC Initium command SD1,
    the method `sd1args()` returns the corresponding string to be used in the SD1
    command:
    ```python
    In [32]: s = Scanners([("1-2", 64,1), ("3-4", 32, 1), (5, 16,1)]); s.sd1args()  
    Out[32]: '(1-2, 64, 1) (3-4, 32, 1) (5, 16, 1)'
    ```

    The method `default_ports()` return a list with every port that can be used in
    the
    ```python
    In [31]: s = dtcinitium.Scanners(1,16); s.ports_default()                       
    Out[31]: ['101-116']
    ```

    Finally, since different setup tables can read from different ports, The method
    `list_ports` returns a list of individual ports from port ranges. The method
    also checks whether the ports are valid or if there are repeated ports:

    ```python
    In [46]: s=Scanners("1-8"); s.list_ports("101-102", "201-201", "301-302")       
    Out[46]: [101, 102, 201, 301, 302]
    ```
    
    """
    def __init__(self, scn, npp=64, lrn=1):
        
        self.lrn = lrn
        self.npp = npp
        
        if not isinstance(scn, list):
            scn1 = [scn]
        else:
            scn1 = scn
        self.scanners = [self.parse_scanner(s) for s in scn1]
        self.nports = {}
        for ss in self.scanners:
            n = ss[1]
            for s in ss[0]: 
                self.nports[s] = n
        return
    
    def range_list(self, s):
        if isinstance(s, int):
            return [s]
        elif isinstance(s, str):
            lims = parse_range(s)
            if len(lims) == 2:
                return [i for i in range(lims[0], lims[1]+1)]
            else:
                return [lims[0]]
        
        raise ValueError("Integer or range (as string) expected!")
        return
        
    def parse_scanner(self, s):
        if isinstance(s, int):
            r = ([s], self.npp, self.lrn)
        elif isinstance(s, str):
            r = (self.range_list(s), self.npp, self.lrn)
        elif isinstance(s, (list, tuple)):
            r = (self.range_list(s[0]), s[1], s[2])
        else:
            raise ValueError("Can't parse %s!" % s)
        return r
    
    def group_str(self, snl):
        
        s = snl[0]
        n = snl[1]
        l = snl[2]
        
        if len(s) > 1:
            ss = "%d-%d" % (s[0], s[-1])
        else:
            ss = "%d" % s[0]
        
        return "(%s, %d, %d)" % (ss, n, l)
    def sd1args(self):
        
        
        slst = [self.group_str(s) for s in self.scanners]
        
        return " ".join(slst)

    def list_ports_aux(self, p):
        lims = [x.strip() for x in p.split('-')]
        
        if len(lims) == 1:
            s1, p1 = scanner_port(p)
            n1 = self.nports[s1]
            if not s1 in self.nports:
                raise ValueError("Scanner %d not configured or available!" % s1)
            if p1 < 0 or p1 > n1:
                raise ValueError("Port %s is not valid!" % lims[0])
                    
            return [int(lims[0])]
        
        s1,p1 = scanner_port(lims[0])
        n1 = self.nports[s1]
        s2,p2 = scanner_port(lims[1])
        n2 = self.nports[s2]
        
        for i in range(s1, s2+1):
            if not i in self.nports:
                raise ValueError("Scanner %d not configured or available!" % i)
        if p1 < 1:
            raise ValueError("Port %s is not valid!" % lims[0])
        if p2 > n2:
            raise ValueError("Port %s is not valid!" % lims[1])
        
        
        if s1==s2:
            return [s1*100 + i for i in range(p1, p2+1)]
        
        ports = [s1*100 + i for i in range(p1, n1+1)]
        for i in range(s1+1, s2):
            ports.extend([(i*100 + k) for k in range(1,self.nports[i]+1)])
        if s2 > s1:
            ports.extend([(s2*100 + k) for k in range(1,p2+1)])
        return ports
    
    def list_ports(self, *plst):
        
        if not isinstance(plst, (list, tuple)):
            return self.list_ports_aux(plst)
        ports = []
        
        for p in plst:
            p1 = self.list_ports_aux(p)
            for i in p1:
                if i in ports:
                    raise ValueError("Repeated ports not supported!")
            ports.extend(p1)
        
        return ports
    
    def ports_default(self):
        
        plst = []
        for lrn in self.scanners:
            npp = lrn[1]
            for s in lrn[0]:
                plst.append( "%d-%d" % (s*100 + 1, s*100 + npp))
        return plst
    
    def count_ports_aux(self, p):
        lims = [x.strip() for x in p.split('-')]
        if len(lims) == 1:
            s1, p1 = scanner_port(lims)
            if s1 not in self.nports:
                raise ValueError("Port %s not valid!" % lims[0])
            return 1
        s1, p1 = scanner_port(lims[0])
        s2, p2 = scanner_port(lims[1])
        
        nn = 0
        for i in range(s1, s2+1):
            if i in self.nports:
                nn += self.nports[i]
            else:
                raise ValueError("Scanner %d not configured!" % i)
        if p1 > 1:
            nn -= (p1-1)
        elif p1 == 0:
            raise ValueError("Port %s not valid!" % lims[0])
            
        if p2 < self.nports[s2]:
            nn -= (self.nports[s2] - p2)
        elif p2 > self.nports[s2]:
            raise ValueError("Port %s not valid!" % lims[1])
            
        return nn
            
    def count_ports(self, plst):
        if not isinstance(plst, (list,tuple)):
            plst = [plst]
        
        return sum([self.count_ports_aux(p) for p in plst])
        
        
        


DASetupTable = namedtuple('DASetupTable', ['stbl', 'nfr', 'nms', 'msd', 
                                           'trm', 'scm', 'ocf', 'port', 
                                          'nchans', 'fast'])

class DTCThread(threading.Thread):
    
    def __init__(self, acq):
        threading.Thread.__init__(self)
        self.acq = acq
        return
    
    def run(self):
        self.acq.acquire()
        return

class DTCAcquire(object):

    def __init__(self, s, stbl, nms, buf, nchans):

        self.nms = nms
        self.buf = buf
        self.nchans = nchans
        self.nbytes = nchans*4 + 24
        self.stbl = stbl
        self.s = s

        self.t0 = 0.0
        self.t1 = 0.0
        self.t2 = 0.0
        self.nsamples = 0
        self.acquiring = False
        return
            
    def acquire(self):
        self.nsamples = 0
        self.acquiring = True
        
        self.s.send( ("AD2 %d %d;\n" % (self.stbl, self.nms)).encode() )

        self.t0 = time.perf_counter()
        self.t1 = self.t0
        self.t2 = self.t0
        
        self.s.recv_into(self.buf[0], self.nbytes)

        self.t1 = time.perf_counter()
        self.nsamples = 1
        for i in range(1, self.nms):
            self.s.recv_into(self.buf[i], self.nbytes)
            self.t2 = time.perf_counter()
            self.nsamples = i+1

        self.acquiring = False

        return 

        
    def samplesread(self):
        return self.nsamples

    def samplerate(self):
        return (self.nsamples-1) / max(4e-3, (self.t2 - self.t1))
    
    
        
class DTCInitium(object):
    """
    High level interface to the DTCInitium.

    
    """
    
    def __init__(self, scanners, ipaddr='192.168.129.7'):
        
        self.dtc = DTCInitiumCore(ipaddr)
        self.s = self.dtc.socket()
        self.s.settimeout(3)
        self.scanners = Scanners(scanners)
        self.timeout = 1
        self.s.settimeout(self.timeout)
        self.thread = None
        self.acquiring = False
        self.nsamples = 0
        self.acq = None
        
        try:
            self.dtc.SD1str(self.scanners.sd1args())
            self.dtc.response(err=True)
            self.dtc.PC4(3)
            self.dtc.response(err=True)
            self.stbl = {}
        
            self.config(5, nfr=1, nms=1, msd=50, trm=0, fast=False)
            npress = 30000
            nbytes = 512*4 + 24
            self.buffer = np.zeros( (npress,nbytes), np.uint8)
            self.buflen = npress
            self.nbytes = nbytes
            self.dtacq = 3.3
        except:
            self.s.close()
            raise RuntimeError("Not able to start")

    def open(self):
        self.dtc.open()
        return
    def close(self):
        self.dtc.close()
    def __del__(self):
        self.dtc.close()
        
    
    def config(self, stbl=5, nfr=1, nms=1, msd=50, fast=False, trm=0, port=None, fast=False):
        if self.acquiring:
            raise RuntimeError("No configuration while acquisition going on!")
        
        if stbl < 1 or stbl > 5:
            raise ValueError("STBL should be between 1 and 5!")
            
        if port is None:
            port = self.scanners.ports_default()
        else:
            if not isinstance(port, (list,tuple)):
                port = [port]
        
            
        nchans = len(self.scanners.list_ports(*port))
        
        self.stbl[stbl] = DASetupTable(stbl=stbl, nfr=nfr, nms=nms, msd=msd, trm=trm, scm=1, ocf=2, port=port, nchans=nchans, 
                           fast=fast)
        self.dtc.SD2(stbl=stbl, nfr=nfr, nms=nms, msd=msd, trm=trm, scm=1, ocf=2)
        self.dtc.response(err=True)
        
        self.dtc.SD3(stbl, *port)
        self.dtc.response(err=True)
        
    def allocbuffer(self, stbl, npress=None):
        
        if not stbl in self.stbl:
            raise ValueError("STBL %d not configured!" % stbl)
        
        nms = self.stbl[stbl].nms
        if npress is None:
            npress = self.buflen
        else:
            npress = self.stbl[stbl].nms
        npress = max(npress, nms)
        
        if npress > self.buflen:
            self.buffer = np.zeros( (npress, self.nbytes), np.uint8)
            self.buflen = npress
        return

    def stop(self):
        self.s.send(b"AD0;\n")
        return self.response()
    
    def acquire(self, stbl=1, nms=None):
        if self.acquiring:
            raise RuntimeError("Wait for the acquisition to end before acquiring data again!")
        
        if not stbl in self.stbl:
            raise ValueError("STBL %d not defined. Used config method to define it." % stbl)
        if nms is None:
            nms = self.stbl[stbl].nms

        nfr = self.stbl[stbl].nfr
        msd = self.stbl[stbl].msd
        nchans = self.stbl[stbl].nchans

        if self.stbl[stbl].fast:
            self.dtc.SD5(-1, 0)
            self.dtc.response()

        # Estimate reading time:

        dt = max(nfr * 4, msd)

        self.s.settimeout(max(0.3, 2*dt*1e-3))

        self.acq = DTCAcquire(self.s, stbl, nms, self.buffer, nchans)


        self.acq.acquire()

        r = self.dtc.response()
        if r.type != 4:
            raise RuntimeError("Problem reading data!")

        if self.stbl[stbl].fast:
            self.dtc.SD5(-1, 1)
            self.dtc.response()

        freq = self.acq.samplerate()

        self.acq = None
        self.s.settimeout(self.timeout)
        
        return self.get_pressure(nms, nchans), freq
    #def hardzero(self
    def start(self, stbl=1, nms=None):

        if self.acquiring:
            raise RuntimeError("Wait for the acquisition to end before acquiring data again!")
        
        if not stbl in self.stbl:
            raise ValueError("STBL %d not defined. Used config method to define it." % stbl)
        if nms is None:
            nms = self.stbl[stbl].nms
        nfr = self.stbl[stbl].nfr
        msd = self.stbl[stbl].msd
        nchans = self.stbl[stbl].nchans

        if self.stbl[stbl].fast:
            self.dtc.SD5(-1, 0)
            self.dtc.response()

        # Estimate reading time:

        dt = max(nfr * 4, msd)

        self.s.settimeout(max(0.3, 2*dt*1e-3))

        nbytes = nchans*4 + 24

        self.acq = DTCAcquire(self.s, stbl, nms, self.buffer, nchans)
        self.thread = DTCThread(self.acq)
        self.acquiring = True
        self.thread.start()

    def read(self):
        self.thread.join()

        r = self.dtc.response()
        if r.type != 4:
            raise RuntimeError("Problem reading data!")
        
        stbl = self.acq.stbl
        if self.stbl[stbl].fast:
            self.dtc.SD5(-1, 1)
            self.dtc.response()

        freq = self.acq.samplerate()
        nms = self.acq.nms
        nchans = self.acq.nchans
        
        self.acq = None
        self.acquiring = False
        self.s.settimeout(self.timeout)
        
        return self.get_pressure(nms, nchans), freq

    def isacquiring(self):
        return self.acquiring
    def samplesread(self):
        if self.acquiring:
            return self.acq.samplesread()
        else:
            raise RuntimeError("No acquisition going on!")
    def samplerate(self):
        if self.acquiring:
            return self.acq.samplerate()
        else:
            raise RuntimeError("No acquisition going on!")
        
    
        
    def get_pressure(self, nms, nchans):
        press = np.zeros( (nms, nchans), np.float64)
        b1 = 24
        b2 = b1 + nchans*4
        for i in range(nms):
            np.copyto(press[i], self.buffer[i,b1:b2].view('>f4'))
        return press
    

    def acquire0(self, stbl=1, nms=None):
        
        if not stbl in self.stbl:
            raise ValueError("STBL %d not defined. Used config method to define it." % stbl)
        if nms is None:
            nms = self.stbl[stbl].nms
        
        nfr = self.stbl[stbl].nfr
        msd = self.stbl[stbl].msd
        nchans = self.stbl[stbl].nchans
        if self.stbl[stbl].fast:
            self.dtc.SD5(-1, 0)
            self.dtc.response()
        # Estimate the data rate:
        dt = max(msd, nfr*self.dtacq)
        self.s.settimeout(max(3*dt, 1))
        nbytes = nchans*4 + 24

        self.dtc.AD2(stbl, nms)
        

        tstart = time.perf_counter()
        
        self.s.recv_into(self.buffer[0], nbytes)

        t1 = time.perf_counter()

        for i in range(1,nms):
            self.s.recv_into(self.buffer[i], nbytes)
            t2 = time.perf_counter()
        
        tend = time.perf_counter()
        
        self.s.settimeout(self.timeout)

        
        r = self.dtc.response()
        
        if r.type != 4:
             raise RuntimeError("Problem reading data!")
        
        if self.stbl[stbl].fast:
            self.dtc.SD5(-1, 1)
            self.dtc.response()
        press = np.zeros( (nms, nchans), np.float64)
        b1 = 24
        b2 = b1 + nchans*4
        
        
        for i in range(nms):
            np.copyto(press[i], self.buffer[i,b1:b2].view('>f4'))
        if nms > 1:
            dt = (tend-t1) / (nms-1)
        else:
            dt = t1 - tstart
        return press, 1.0/dt
    
        
    def simpleacquire(self, stbl=5, nms=None):
        
        if not stbl in self.stbl:
            raise ValueError("STBL %d not defined. Used config method to define it." % stbl)
        
        if nms is None:
            nms = self.stbl[stbl].nms
        nfr = self.stbl[stbl].nfr
        msd = self.stbl[stbl].msd
        
        if self.stbl[stbl].fast:
            self.dtc.SD5(-1, 0)
            self.dtc.response()
            
        
        dados = []
        self.dtc.AD2(stbl, nms)
        
        dt = nfr * max(msd, 8) * 1000
        #self.s.settimeout(max(0.2, 3*dt))
        
        self.nsamples = 0
        tstart = time.perf_counter()

        dados.append(self.dtc.response(err=True))

        t1 = time.perf_counter()
        
        for i in range(1,nms):#while True:
            r = self.dtc.response(err=True)
            #if r.type==4:
            #    break
            dados.append(r)
        t2 = time.perf_counter()
        self.dtc.response(err=True)
                
        if self.stbl[stbl].fast:
            self.dtc.SD5(-1, 1)
            self.dtc.response()
        
        return dados, nms, tstart, t2, t1        
            
