"""
dummy_katcp_wrapper.py
----------------------

As the filename suggests, this is a dummy version of katcp_wrapper, which is used
for debugging the hipsr-server script. It pretends to be the katcp_wrapper, and makes
dummy FpgaClient objects which will communicate with fake data. This allows the script
to operate *without* a connection to the roach boards.

This script is not intended to be a panacea, it has only been tested with hipsr-server.


Usage
~~~~~
Add this to your file:

    if opts.debug:
        import dummy_katcp_wrapper as katcp_wrapper
        katcp_helpers.katcp_wrapper = katcp_wrapper

And then let the dodgy magic of this script take over. Note that if you include things that use
katcp_wrapper, after you include them you'l have to do as I've done above for katcp_helpers...
"""

import numpy as np
import struct, time


class FpgaClient(object):
    def __init__(self, fpga, katcp_port=714, timeout=10):
        
        self.fpga       = fpga 
        self.host       = fpga
        self.katcp_port = katcp_port 
        self.timeout    = timeout
        self.registers  = {
            'sys_clk' : 200e6, 
            'sys_scratchpad' : 'test'
        }
        
        self.brams      = {}
        
        # HIPSR design specific register values
        self.acc_regs =  {
            'acc_cnt': 0, 
            'o_acc_cnt': 0, 
            'acc_counter' : 0 
            }

        self.data_brams = {
            'snap_xx0_bram': self.random_bandpass()[::2],
            'snap_xx1_bram': self.random_bandpass()[1::2],
            'snap_yy0_bram': self.random_bandpass()[::2],
            'snap_yy1_bram': self.random_bandpass()[1::2]
        }
        
    def progdev(self, boffile):
        self.boffile = boffile
        return "ok"
    
    def is_connected(self):
        return True
    
    def listdev(self):
        return self.registers.keys()
    
    def write_int(self, reg_id, value, blindwrite=True):
        self.registers[reg_id] = value
        return  "ok"
    
    def read_int(self, reg_id, blindwrite=True):
        try:
            if self.acc_regs.has_key(reg_id):
                self.acc_regs[reg_id] = self.acc_regs[reg_id] + 1
                time.sleep(0.5)
                return self.acc_regs[reg_id]
            elif self.registers.has_key(reg_id):
                return self.registers[reg_id]
            else:
                return 0
        except:
            raise
    
    def read(self, bram_id, num_bytes):
        try:
            if self.data_brams.has_key(bram_id):
                fmt = '>%iL'%(num_bytes/4)
                data = self.random_bandpass()[::2]
                packed = struct.pack(fmt, *data)
                return packed
            elif self.brams.has_key(bram_id):
                data = self.brams[bram_id]
                return data
            else:
                fmt    = '%iB'%num_bytes
                data   = np.random.random_integers(0, 255, num_bytes).tolist()
                packed = struct.pack(fmt, *data)
                return packed
        except:
            raise

    def write(self, bram_id, packed):
        self.brams[bram_id] = packed
        return "ok"
    
    def random_bandpass(self):
        """ Generate a fake bandpass with some noise on it """
        lo = np.ones([1024]) * 1e2
        hi = np.ones([8192-2048]) * 1e5
        bp = np.append(np.append(lo, hi), lo) 
        noise = np.random.random_integers(0, 100, 8192)
        spike = 1e6 * np.random.random()
        spike_bin = np.random.random_integers(1024, 6976)
        bp[spike_bin-2:spike_bin+2] += spike
    
        return bp + noise
        