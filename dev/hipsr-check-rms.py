#! /usr/bin/env python
# encoding: utf-8
"""
hipsr-digitizer-rms.py
======================

Compute RMS of digitizers.


Copyright (c) 2013 The HIPSR collaboration. All rights reserved.
"""

import time
import numpy as np

import hipsr_core.katcp_helpers
import hipsr_core.config as config
import hipsr_core.katcp_wrapper as katcp_wrapper

from lib.colorterm import colorterm as cprint

# Python metadata
__version__  = config.__version__
__author__   = config.__author__
__email__    = config.__email__
__license__  = config.__license__
__modified__ = datetime.fromtimestamp(os.path.getmtime(os.path.abspath( __file__ )))

if __name__ == '__main__':
    roachlist    = config.roachlist
    katcp_port   = config.katcp_port

    fpgas = [katcp_wrapper.FpgaClient(roach, katcp_port) for roach in roachlist]
    time.sleep(0.1)
    
    # Make sure ROACH boards are programmed
    try:
        fpgas[0].read_int("mux_sel")
    except:
        print "Please wait, reprogramming ROACH boards..."
        katcp_helpers.reprogram("hipsr_400_8192")
        katcp_helpers.reconfigure("hipsr_400_8192")
        time.sleep(0.5)
        print "OK"
    
    # Read RMS levels  
    cprint.green("\nROACH RMS LEVELS:")
    cprint.green("-----------------")
    for fpga in fpgalist:        
        levels = katcp_helpers.getSpectrum(fpga, 'rms_levels')
        if levels['rms_x'] > 50 or levels['rms_y'] > 50:
            cprint.red( "%s RMS: %2.2f (A), %2.2f (B)"%(roachlist[fpga.host], levels['rms_x'], levels['rms_y']))
        else:
            print "%s RMS: %2.2f (A), %2.2f (B)"%(roachlist[fpga.host], levels['rms_x'], levels['rms_y'])

    # Read NAR levels
    cprint.green("\nROACH NAR LEVELS:")
    cprint.green("-----------------")
    for fpga in fpgalist:        
        levels = katcp_helpers.getSpectrum(fpga, 'rms_levels')
        n_bits_x, n_bits_y = np.log2(levels['nar_x_on']), np.log2(levels['nar_y_on'])
        if n_bits_x > 30 or n_bits_y > 30:
            cprint.red( "%s NAR power (bits): %2.2f (A), %2.2f (B)"%(roachlist[fpga.host], n_bits_x, n_bits_y))
        else:     
            print "%s NAR power (bits): %2.2f (A), %2.2f (B)"%(roachlist[fpga.host], n_bits_x, n_bits_y)
    
    