#! /usr/bin/env python
# encoding: utf-8
"""
hdf_server.py
=============

HDF Server class for hipsr.
"""

import time, sys, os, socket, random, select, re
import hipsr_core.config as config
from   hipsr_core.hipsr6 import createMultiBeam
import mpserver

class HdfServer(mpserver.MpServer):
    """ HDF5 Writer thread """
    def __init__(self, project_id, dir_path, mainQueue, printQueue, hdfQueue, tcsQueue, flavor=None):
        self.name = 'hdf_server'
        self.project_id       = project_id
        self.dir_path         = dir_path
        self.hdfQueue         = hdfQueue
        self.printQueue       = printQueue
        self.tcsQueue         = tcsQueue
        self.hdf_file         = None
        self.hdf_is_open      = False
        self.hdf_write_enable = False
        self.tbPointing       = None
        self.tbData           = None
        self.tbObservation    = None
        self.tbWeather        = None
        self.tbFirmwareConfig = None
        self.tbScanPointing   = None
        self.new_file_each_obs= False 
        self.debug = False

        if flavor is None:
            self.flavor = 'hipsr_400_8192'
        else:
            self.flavor = flavor

        super(HdfServer, self).__init__(self.name, printQueue, mainQueue)
    
    def setWriteEnable(self, val):
        """ Set write enable to on or off """
        self.hdf_write_enable = val
        if self.hdf_write_enable:
            self.mprint("HDF Thread: Write enabled.")
        else:
            self.mprint("HDF Thread: Write disabled.")
        
    def createNewFile(self, tcs_filename=None):
        """ Closes current file and creates a new one"""
        #print "HERE2: %s"%tcs_filename
        if self.hdf_is_open:
            self.hdf_write_enable = False
            self.hdf_is_open      = False
            self.mprint("closing %s"%self.hdf_file.filename)
            self.hdf_file.close()

        try:
            timestamp = time.time()
            now = time.gmtime(timestamp)
            now_str    = "%d-%02d-%02d_%02d%02d%02d"%(now.tm_year, now.tm_mon, now.tm_mday, now.tm_hour, now.tm_min, now.tm_sec)
            filestamp  = "%s_%s"%(self.project_id, now_str)
            dirstamp   = "%d-%02d-%02d"%(now.tm_year, now.tm_mon, now.tm_mday)

            self.mprint("\nFile creation")
            self.mprint("-------------\n")

            if not os.path.exists(self.dir_path):
                self.mprint("Creating directory %s"%self.dir_path)
                os.makedirs(self.dir_path)
            if not os.path.exists(os.path.join(self.dir_path,dirstamp)):
                self.mprint("Creating directory %s"%dirstamp)
                os.makedirs(os.path.join(self.dir_path,dirstamp))

            if tcs_filename: filename = tcs_filename
            else:            filename = '%s.h5'%filestamp
            self.mprint("Creating file %s"%filename)
            self.mprint("Flavor: %s"%self.flavor)
            self.hdf_file = createMultiBeam(filename, os.path.join(self.dir_path, dirstamp), flavor=self.flavor)
            time.sleep(1e-3) # Make sure file has created successfully...

            self.hdf_is_open      = True
            self.tcsQueue.put({'hdf_is_open': True})
            self.data             = None
            self.tbPointing       = self.hdf_file.root.pointing
            self.tbRawData        = self.hdf_file.root.raw_data
            self.tbObservation    = self.hdf_file.root.observation
            self.tbWeather        = self.hdf_file.root.weather
            self.tbFirmwareConfig = self.hdf_file.root.firmware_config
            self.tbScanPointing   = self.hdf_file.root.scan_pointing

            # Write firmware config
            fpga_config = config.fpga_config[self.flavor]
            fpga_config["firmware"] = fpga_config["firmware"]
            self.data = {'firmware_config': fpga_config}
            self.writeFirmwareConfig()
            self.data = None

        except:
            raise
    
    def writePointing(self, val=None):
        """ Write pointing row from stored data """
        if self.hdf_is_open and self.data:
          for key in self.data["pointing"].keys():
              self.tbPointing.row[key] = self.data["pointing"][key]
          self.tbPointing.row.append()
          self.tbPointing.flush()

    def writeObservation(self, val=None):
        """ Write observation row from stored data """
        if self.hdf_is_open and self.data:
            for key in self.data["observation"].keys():
                if key != 'conf_name': 
                  self.tbObservation.row[key]  = self.data["observation"][key]    
            self.tbObservation.row.append()
            self.tbObservation.flush()
  
    def writeRawData(self, val=None):
        """ Write raw_data row from stored data """
        if self.hdf_is_open and self.data:
            for beam_id in self.data["raw_data"].keys():
                beam = self.hdf_file.getNode('/raw_data', beam_id)
                for key in self.data["raw_data"][beam_id].keys():
                    beam.row[key]  = self.data["raw_data"][beam_id][key]
                beam.row.append()
                beam.flush()

    def writeWeather(self, val=None):
        """ Write weather row from stored data """
        if self.hdf_is_open and self.data:
            for key in self.data["weather"].keys():
                self.tbWeather.row[key] = self.data["weather"][key]
            self.tbWeather.row.append()
            self.tbWeather.flush()
      
    def writeFirmwareConfig(self, val=None):
        """ Write firmware_config row from stored data """
        if self.hdf_is_open and self.data:
            for key in self.data["firmware_config"].keys():
                self.tbFirmwareConfig.row[key]        = self.data["firmware_config"][key]
            self.tbFirmwareConfig.row.append()
            self.tbFirmwareConfig.flush()

    def writeScanPointing(self, val=None):
        """ Write scan_pointing row from stored data """
        if self.hdf_is_open and self.data:
            for key in self.data["scan_pointing"].keys():
                # Look out for capitals!
                self.tbScanPointing.row[key.lower()] = self.data["scan_pointing"][key]
            self.tbScanPointing.row.append()
            self.tbScanPointing.flush()

    def safeExit(self, val=None):
        """ Uh oh. That escalated quickly. """
        try:
            self.mprint("hdf_server: safe exit called.")
            if self.hdf_is_open:
                self.server_enabled = False
                self.hdf_write_enable = False
                self.hdf_is_open      = False
                self.mprint("hdf_server: closing %s"%self.hdf_file.filename)
                self.hdf_file.flush()
                self.hdf_file.close()
                del(self.hdf_file)

        except:
            self.mprint("hdf_server: ERROR: Safe exit failed")
            raise
    
    def serverMain(self):
        """ Main HDF writer routine """
        self.mprint("HDF server: writing to directory %s..."%self.dir_path)
        while self.server_enabled:
            # Note that no data will be written to queue when TCS thread is set to disabled,
            # So no need to check self.hdf_write_enable
            while not self.hdfQueue.empty():

                self.data = self.hdfQueue.get()

                validKeys = {
                  'pointing'        : self.writePointing,
                  'raw_data'        : self.writeRawData,
                  'observation'     : self.writeObservation,
                  'weather'         : self.writeWeather,
                  'firmware'        : self.writeFirmwareConfig,
                  'scan_pointing'   : self.writeScanPointing,
                  'create_new_file' : self.createNewFile,
                  'write_enable'    : self.setWriteEnable
                }

                for key in self.data.keys():
                    if key == 'write_enable':
                        self.mprint("%s: %s"%(key, self.data[key]))
                        self.setWriteEnable(self.data[key])
                    elif key == 'create_new_file':
                        print "HERE: %s"%self.data
                        self.createNewFile(self.data[key])
                    elif key == 'safe_exit':
                        self.safeExit()
                    elif self.hdf_write_enable and self.hdf_is_open:
                         validKeys[key](self.data[key])
                time.sleep(1e-6)

        self.mprint("hdf_server: exiting.")