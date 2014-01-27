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

from hipsr_core.katcp_helpers import snap
import hipsr_core.config as config
import hipsr_core.katcp_wrapper as katcp_wrapper

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

    for fpga in fpgalist:
        fpga.write_int('mux_sel',0)
        adc_data_x = snap(fpga, 'snap_mux')
        adc_data_x = snap(fpga, 'snap_mux')
        print "%s RMS: %2.2f"%(roachlist[fpga.host], float(np.std(adc_data_x)))

        fpga.write_int('mux_sel',2)
        adc_data_y = snap(fpga, 'snap_mux')
        adc_data_y = snap(fpga, 'snap_mux')
        print "%s RMS: %2.2f"%(roachlist[fpga.host], float(np.std(adc_data_y)))