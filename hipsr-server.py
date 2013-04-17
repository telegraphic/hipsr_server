#! /usr/bin/env python
# encoding: utf-8
"""
hipsr_server.py
===============

HIPSR wideband spectrometer server script. This script reprograms and reconfigures the roach
boards, creates a data file, and then begins collecting data from TCS and the roach boards.

A seperate thread is created for each roach board, so that reading and writing data can be done
in parallel. In addition, there is a thread which acts as a server that listend for TCS messages.
To write to the HDF data file, these threads append an I/O requests to a FIFO (Queue.Queue) which
is constantly checked by yet another thread.

Copyright (c) 2012 The HIPSR collaboration. All rights reserved.
"""


import time, sys, os, socket, random, select, re
from datetime import datetime
from optparse import OptionParser
import json
from collections import deque   # Ring buffer

import numpy as np
import cPickle as pkl
import threading, Queue

import lib.katcp_wrapper as katcp_wrapper
from   lib.katcp_helpers import stitch, snap, squashData, squashSpectrum, getSpectrum
import lib.katcp_helpers as katcp_helpers
import lib.config as config
import lib.HIPSR5 as hipsr5 
import lib.astroCoords as coords
import lib.hipsr_control as hipsr_control
from lib.printers import Logger

# Python metadata
__version__  = config.__version__
__author__   = config.__author__
__email__    = config.__email__
__license__  = config.__license__
__modified__ = datetime.fromtimestamp(os.path.getmtime(os.path.abspath( __file__ )))


class threadMonitor(object):
    """ Class for failed thread detection"""
    def __init__(self):
        self.tcs_ok     = False
        self.plotter_ok = False
        self.hdf_ok     = False
        self.katcp_ok   = False
     
    def allSystemsGo(self):
        return self.tcs_ok & self.plotter_ok & self.hdf_ok & self.katcp_ok
         
        
class tcsServer(threading.Thread):
    """ TCS server class 
    
    A TCP server which listens for TCS commands and parses them.
    Values are then stored in a shared python dictionary.
    """
    def __init__(self, host, port):
        threading.Thread.__init__(self)
        
        self.hdf_write_enable = False
        self.server_enabled = True
        self.host = host
        self.port = port
        self.ack_msg = "ok\n"
        self.send_udp   = False
        self.debug = False
        
        self.obs_setup = {
            'frequency' : 0, 
            'bandwidth' : 0, 
            'receiver' : 'Parkes multibeam', 
            'telescope' : 'Parkes 64m',
            'project_id' : 'P', 
            'num_beams' : 0, 
            'ref_beam' : 0, 
            'feed_rotation' : '',
            'feed_angle' : 0, 
            'acc_len' : 0,
            'dwell_time': 0,
            'observer' : 'D Fault',
            'conf_name' : '',
            'scan_rate' : 0
        }
    
        self.pointing_data = {
            'timestamp': 0,
            'ra' : 0,
            'dec' : 0,
            'source' : ''
        }
        
        self.scan_pointing = {
            'focus_tan' : 0,
            'focus_axial' : 0,
            'focus_rot'  : 0,
            'par_angle' : 0,
            'azimuth'  : 0,
            'elevation' : 0,
            'timestamp' : 0
        } 
        
        # Add keys for multibeam RA and DEC entries
        raj_keys = ["MB%02d_raj"%b_id for b_id in range(1,14)]
        for key in raj_keys: self.scan_pointing[key] = 0
        dcj_keys = ["MB%02d_dcj"%b_id for b_id in range(1,14)]
        for key in dcj_keys: self.scan_pointing[key] = 0

    def toJson(self, cmd, val):
        """ Converts a dictionary of numpy arrays into a dictionary of lists."""
        return json.dumps({cmd : val})
    
    def setFreq(self, val):
        print "%-15s : %s"%("Central freq.", val.strip())
        self.obs_setup["frequency"] = val.strip()
        if self.send_udp:
            msg = self.toJson('tcs-frequency', val.strip())
            plotterThread.udpQueue.append(msg)
        return self.ack_msg

    def setBandwidth(self, val):
        print "%-15s : %s"%("Bandwidth", val.strip())
        self.obs_setup["bandwidth"] = val.strip()
        if self.send_udp:
            msg = self.toJson('tcs-bandwidth', val.strip())
            plotterThread.udpQueue.append(msg)
        return self.ack_msg

    def setObserver(self, val):
        print "%-15s : %s"%("Observer", val.strip())
        self.obs_setup["observer"] = val.strip()
        return self.ack_msg

    def setObsMode(self, val):
        print "%-15s : %s"%("Obs. mode", val.strip())
        self.obs_setup["obs_mode"] = val.strip()
        return self.ack_msg

    def setSrc(self, val):
        print "%-15s : %s"%("Source name", val.strip())
        self.pointing_data["source"] = val.strip()
        return self.ack_msg

    def setRa(self, val):
        ra = coords.rastring2deg(val)
        print "%-15s : %s"%("Source RA", val.strip())
        self.pointing_data["ra"] = ra
        return self.ack_msg

    def setDec(self, val):
        dec = coords.decstring2deg(val)
        print "%-15s : %s"%("Source DEC", val.strip())
        self.pointing_data["dec"] = dec
        return self.ack_msg 

    def setReceiver(self, val):
        print "%-15s : %s"%("Receiver", val.strip())
        self.obs_setup["receiver"] = val.strip()
        return self.ack_msg

    def setProjectId(self, val):
        print "%-15s : %s"%("Project ID", val.strip())
        self.obs_setup["project_id"] = val.strip()
        return self.ack_msg

    def setNumBeams(self, val):
        print "%-15s : %s"%("No. beams", val.strip())
        self.obs_setup["num_beams"] = val.strip()
        return self.ack_msg

    def setRefBeam(self, val):
        print "%-15s : %s"%("Ref. beam", val.strip())
        self.obs_setup["ref_beam"] = val.strip()
        return self.ack_msg

    def setFeedRotation(self, val):
        print "%-15s : %s"%("Feed rotation", val.strip())
        self.obs_setup["feed_rotation"] = val.strip()
        return self.ack_msg

    def setFeedAngle(self, val):
        print "%-15s : %s"%("Feed angle", val.strip())
        self.obs_setup["feed_angle"] = val.strip()
        return self.ack_msg

    def setAccLen(self, val):
        print "%-15s : %s"%("Acc. length", val.strip())
        self.obs_setup["acc_len"] = val.strip()
        return self.ack_msg

    def setDwellTime(self, val):
        print "%-15s : %s"%("Dwell time", val.strip())
        self.obs_setup["dwell_time"] = val.strip()
        return self.ack_msg

    def setConfName(self, val=0):
        print "%-15s : %s"%("Config name", val.strip())
        self.obs_setup["conf_name"] = val.strip()
        return self.ack_msg

    def setScanRate(self, val=0):
        print "%-15s : %s"%("Scan rate", val.strip())
        self.obs_setup["scan_rate"] = val.strip()
        return self.ack_msg
    
    # The following commands may be called every cycle
    def setScanRaDec(self, cmd, val):
        #print "Command: %s,        Value: %s"%(cmd, val.strip())
        self.scan_pointing[cmd] = val.strip()
        return self.ack_msg
    
    def setAzimuth(self, val):
        #print "Command: AZ,        Value: %s"%val.strip()
        self.scan_pointing["azimuth"] = val.strip()
        return self.ack_msg
        
    def setElevation(self, val):
        #print "Command: EL,        Value: %s"%val.strip()
        self.scan_pointing["elevation"] = val.strip()
        return self.ack_msg
        
    def setParAngle(self, val):
        #print "Command: PAR,       Value: %s"%val.strip()
        self.scan_pointing["par_angle"] = val.strip()
        return self.ack_msg
        
    def setFocusTan(self, val):
        #print "Command: FOCUSTAN,  Value: %s"%val.strip()
        self.scan_pointing["focus_tan"] = val.strip()
        return self.ack_msg
        
    def setFocusAxi(self, val):
        #print "Command: FOCUSAXI,  Value: %s"%val.strip()
        self.scan_pointing["focus_axi"] = val.strip()
        return self.ack_msg
        
    def setFocusRot(self, val):
        #print "Command: FOCUSROT,  Value: %s"%val.strip()
        self.scan_pointing["focus_rot"] = val.strip()
        return self.ack_msg
    
    def setUtcCycle(self, val):
        #print "Command: UTC_CYCLE,  Value: %s"%val.strip()
        # Convert time string into timestamp
        d_d = datetime.strptime(val.strip(), "%Y-%m-%d-%H:%M:%S.%f")
        d_t = time.mktime(d_d.utctimetuple()) + (d_d.microsecond / 1e6)
        self.scan_pointing["timestamp"] = d_t
        return self.ack_msg

    def endUtcCycle(self):
        #print "Command: UTC_CYCLE_END"
        if hdfThread.hdf_write_enable:
            hdfThread.hdfQueue.put({'scan_pointing' : self.scan_pointing})
        return self.ack_msg          
    
    def setNoMatch(self, val=0):
        """ Default action if command is not found in command dictionary"""
        print "Error: command not supported."
        if val: print val
        return self.ack_msg
    
    def startObs(self, val=0):
        """ Starts observation data capture """
        # Check if HDF file is currently open
        
        timestamp = time.time()
        now = time.gmtime(timestamp)
        date_fmt = "start_utc %d-%02d-%02d_%02d%02d%02d"%(now.tm_year, now.tm_mon, now.tm_mday, now.tm_hour, now.tm_min, now.tm_sec)  
        print "starting observation: ", date_fmt
        
        if not hdfThread.hdf_is_open:
            hdfThread.createNewFile()
        elif hdfThread.new_file_each_obs:
            hdfThread.createNewFile()
        
        self.obs_setup["date"] = timestamp
        self.pointing_data["timestamp"] = timestamp
        
        hdf_data = []
        hdf_data.append({'observation' : self.obs_setup})
        hdf_data.append({'pointing'    : self.pointing_data})
        
        hdfThread.hdf_write_enable = True
        return date_fmt, hdf_data
    
    def newFile(self, val):
        # Create new file
        hdfThread.createNewFile(val.strip())
        return self.ack_msg

    def commandDict(self, cmd,val):
        """ This is essentially a case statement that searches for commands in a dict. """
        return {
            'freq'         : self.setFreq,
            'src'          : self.setSrc,
            'ra'           : self.setRa,
            'dec'          : self.setDec,
            'band'         : self.setBandwidth,
            'receiver'     : self.setReceiver,
            'pid'          : self.setProjectId,
            'nbeam'        : self.setNumBeams,
            'refbeam'      : self.setRefBeam,
            'feedrotation' : self.setFeedRotation,
            'feedangle'    : self.setFeedAngle,
            'taccum'       : self.setAccLen,
            'dwell'        : self.setDwellTime,
            'confname'     : self.setConfName,
            'observer'     : self.setObserver,
            'obstype'      : self.setObsMode,
            'start'        : self.startObs,
            'az'           : self.setAzimuth,
            'el'           : self.setElevation,
            'par'          : self.setParAngle,
            'focustan'     : self.setFocusTan,
            'focusaxi'     : self.setFocusAxi,
            'focusrot'     : self.setFocusRot,
            'utc_cycle'    : self.setUtcCycle,
            'utc_cycle_end': self.endUtcCycle,
            'new_file'     : self.newFile,
            'scanrate'     : self.setScanRate
            }.get(cmd, self.setNoMatch)(val)    # setNoMatch is default if cmd not found
    
    def run(self):
        """ Run TCP/IP server """
        try:
          tcs_regex       = '(?P<cmd>\w+)\s(?P<val>.+)%s'%config.tcs_regex_esc
          
          print "TCS listener: Waiting for TCS data %s:%s... "%(self.host, self.port)
          open_sockets = []
          listening_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
          listening_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
          listening_socket.bind((self.host,self.port))
          listening_socket.listen(5)
          
          # Tell threadMonitor that all is OK
          threadmon.tcs_ok = True
          
          while self.server_enabled:        
              # Waits for I/O being available for reading from any socket object.
              rlist, wlist, xlist = select.select( [listening_socket] + open_sockets, [], [] )
              for i in rlist:
                  if i is listening_socket:
                      new_socket, addr = listening_socket.accept()
                      open_sockets.append(new_socket)
                  else:
                      data = i.recv(1024)
                      if data == "":
                          open_sockets.remove(i)
                          print "TCS I/O: Connection closed"
                      else:
                          if self.debug:
                              print repr(data)
                          
                          # Check for start message
                          is_start = re.search('start%s'%config.tcs_regex_esc, data)
                          if is_start:
                              print "TCS I/O: received start."
                              start_msg, hdf_data = self.startObs() 
                              i.send(start_msg)
                              time.sleep(1e-3)
                              #i.close()
                              
                              for item in hdf_data:
                                  hdfThread.hdfQueue.put(item)
                          
                          # Check for stop message  
                          is_stop = re.search('stop%s'%config.tcs_regex_esc, data)
                          if is_stop:
                              hdfThread.hdf_write_enable = False
                              print "TCS I/O: received stop. Write disabled."
                              i.send(self.ack_msg)
                          
                          is_utc_end = re.search('utc_cycle_end%s'%config.tcs_regex_esc, data)
                          if is_utc_end:
                              self.endUtcCycle()
                              i.send(self.ack_msg)

                          is_conf = re.search('confname %s'%config.tcs_regex_esc, data)
                          if is_conf:
                              print "confname rec'd."
                              i.send(self.ack_msg)
                          
                          match = re.search(tcs_regex, data)
                          if match:
                              (cmd, val) = (match.groupdict()["cmd"], match.groupdict()["val"])
                              #print cmd, val
                              if cmd[0:2] == 'MB':
                                  recv_msg = self.setScanRaDec(cmd, val)
                                  i.send(recv_msg)
                              elif cmd[0:8] == 'new_file':
                                  recv_msg = self.commandDict(cmd, val)
                                  i.send(recv_msg)
                              else:
                                  recv_msg = self.commandDict(cmd, val)
                                  i.send(recv_msg)
        except:
             print "Error: TCS server crashed."
             threadmon.tcs_ok = False
             raise             
 
class plotterServer(threading.Thread):
    """ UDP data server for hipsr-gui plotter """
    def __init__(self, host, port):
        threading.Thread.__init__(self)
        self.host = host
        self.port = port
        
        self.socket   = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.udpQueue = deque(maxlen=13*10)
        
        self.server_enabled = True
        self.debug = False
        
    def run(self):
        try:
          self.socket.connect((self.host, self.port))
          print "Plotter : serving UDP packets on %s port %s... "%(plotter_host, plotter_port)
          # Tell threadMonitor that all is OK
          threadmon.plotter_ok = True
          
          while self.server_enabled:
              try:
                  msg = self.udpQueue.pop()
                  try:
                      #print "sending UDP packet to %s"%plotter_host
                      self.socket.send(msg)
                      time.sleep(0.02)
                  except:
                      #print "Warning: cannot connect to UDP plotter. Sleeping."
                      time.sleep(10)
                      self.udpQueue.clear()
              except:
                  time.sleep(0.1)
        except:
            print "Error: Plotter server crashed."
            threadmon.plotter_ok = False
            raise

class hdfServer(threading.Thread):
    """ HDF5 Writer thread """
    def __init__(self, project_id, dir_path):
        threading.Thread.__init__(self)
        
        self.project_id       = project_id
        self.dir_path         = dir_path
        self.server_enabled   = True
        self.hdfQueue         = Queue.Queue()
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
    
    def createNewFile(self, tcs_filename=None):
      """ Closes current file and creates a new one"""
      if self.hdf_is_open:
          self.hdf_write_enable = False
          self.hdf_is_open      = False
          print "closing %s"%self.hdf_file.filename
          self.hdf_file.close()
          
      try:
          timestamp = time.time()
          now = time.gmtime(timestamp)
          now_str    = "%d-%02d-%02d_%02d%02d%02d"%(now.tm_year, now.tm_mon, now.tm_mday, now.tm_hour, now.tm_min, now.tm_sec)
          filestamp  = "%s_%s"%(self.project_id, now_str)
          dirstamp   = "%d-%02d-%02d"%(now.tm_year, now.tm_mon, now.tm_mday)
          
          print "\nFile creation"
          print "-------------\n"
          
          
          
          if not os.path.exists(self.dir_path):
              print "Creating directory %s"%self.dir_path
              os.makedirs(self.dir_path)
          if not os.path.exists(os.path.join(self.dir_path,dirstamp)):
              print "Creating directory %s"%dirstamp
              os.makedirs(os.path.join(self.dir_path,dirstamp))
        
          if tcs_filename: filename = tcs_filename
          else:            filename = '%s.h5'%filestamp
          self.hdf_file = hipsr5.createMultiBeam(filename, os.path.join(self.dir_path, dirstamp))
          
          time.sleep(1e-3) # Make sure file has created successfully...
          
          self.hdf_is_open      = True
          
          self.data             = None
          self.tbPointing       = self.hdf_file.root.pointing
          self.tbRawData        = self.hdf_file.root.raw_data
          self.tbObservation    = self.hdf_file.root.observation
          self.tbWeather        = self.hdf_file.root.weather
          self.tbFirmwareConfig = self.hdf_file.root.firmware_config 
          self.tbScanPointing   = self.hdf_file.root.scan_pointing
          
          # Write firmware config
          fpga_config = {
              "firmware"      : config.boffile,
              "acc_len"       : config.fpga_config["acc_len"],
              "fft_shift"     : config.fpga_config["fft_shift"],
              "quant_xx_gain" : config.fpga_config["quant_xx_gain"],
              "quant_yy_gain" : config.fpga_config["quant_yy_gain"],
              "quant_xy_gain" : config.fpga_config["quant_xy_gain"],
              "quant_xy_gain" : config.fpga_config["quant_xy_gain"],
              "mux_sel"       : config.fpga_config["mux_sel"],
              }
          self.data = {'firmware_config': fpga_config}
          self.writeFirmwareConfig()
          self.data = None
          
      except:
          print "Error: could not create new file."
          threadmon.hdf_ok = False
          raise
    
    def writePointing(self):
        if self.hdf_is_open and self.data:
          self.tbPointing.row["timestamp"] = self.data["pointing"]["timestamp"]
          self.tbPointing.row["source"]    = self.data["pointing"]["source"]
          self.tbPointing.row["ra"]        = self.data["pointing"]["ra"]
          self.tbPointing.row["dec"]       = self.data["pointing"]["dec"]
          self.tbPointing.row.append()
          self.tbPointing.flush()

    def writeObservation(self):
        if self.hdf_is_open and self.data:
            self.tbObservation.row["telescope"]      = self.data["observation"]["telescope"]
            self.tbObservation.row["receiver"]       = self.data["observation"]["receiver"]
            self.tbObservation.row["frequency"]      = self.data["observation"]["frequency"]
            self.tbObservation.row["date"]           = self.data["observation"]["date"]
            self.tbObservation.row["project_id"]     = self.data["observation"]["project_id"]
            #self.tbObservation.row["project_name"]  = self.data["observation"]["project_name"]
            self.tbObservation.row["observer"]       = self.data["observation"]["observer"]
            self.tbObservation.row["acc_len"]        = self.data["observation"]["acc_len"]
            self.tbObservation.row["bandwidth"]      = self.data["observation"]["bandwidth"]
            self.tbObservation.row["num_beams"]      = self.data["observation"]["num_beams"]
            self.tbObservation.row["ref_beam"]       = self.data["observation"]["ref_beam"]
            self.tbObservation.row["feed_rotation"]  = self.data["observation"]["feed_rotation"]
            self.tbObservation.row["dwell_time"]     = self.data["observation"]["dwell_time"]
            #self.tbObservation.row["conf_name"]      = self.data["observation"]["conf_name"]
            self.tbObservation.row["feed_angle"]     = self.data["observation"]["feed_angle"]
            self.tbObservation.row["scan_rate"]     = self.data["observation"]["scan_rate"]    
            self.tbObservation.row.append()
            self.tbObservation.flush()
  
    def writeRawData(self):
      if self.hdf_is_open and self.data:
        for key in self.data["raw_data"].keys():
            beam = self.hdf_file.getNode('/raw_data',key)
            beam.row["id"]         = self.data["raw_data"][key]["id"]
            beam.row["timestamp"]  = self.data["raw_data"][key]["timestamp"]
            beam.row["xx"]         = self.data["raw_data"][key]["xx"]
            beam.row["yy"]         = self.data["raw_data"][key]["yy"]
            beam.row["re_xy"]      = self.data["raw_data"][key]["re_xy"]
            beam.row["im_xy"]      = self.data["raw_data"][key]["im_xy"]
            beam.row["fft_of"]     = self.data["raw_data"][key]["fft_of"]
            beam.row["adc_clip"]   = self.data["raw_data"][key]["adc_clip"]
            beam.row.append()
            beam.flush()
      
    def writeWeather(self):
      if self.hdf_is_open and self.data:
          self.tbWeather.row["timestamp"]       = self.data["weather"]["timestamp"]
          self.tbWeather.row["temperature"]     = self.data["weather"]["temperature"]
          self.tbWeather.row["pressure"]        = self.data["weather"]["pressure"]
          self.tbWeather.row["humidity"]        = self.data["weather"]["humidity"]
          self.tbWeather.row["wind_speed"]      = self.data["weather"]["wind_speed"]
          self.tbWeather.row["wind_direction"]  = self.data["weather"]["wind_direction"]
          self.tbWeather.row.append()
          self.tbWeather.flush()
      
    def writeFirmwareConfig(self):
      if self.hdf_is_open and self.data:
          #print "Writing firmware config"              
          self.tbFirmwareConfig.row["firmware"]        = self.data["firmware_config"]["firmware"]
          self.tbFirmwareConfig.row["quant_xx_gain"]   = self.data["firmware_config"]["quant_xx_gain"]
          self.tbFirmwareConfig.row["quant_yy_gain"]   = self.data["firmware_config"]["quant_yy_gain"]
          self.tbFirmwareConfig.row["quant_xy_gain"]   = self.data["firmware_config"]["quant_xy_gain"]
          self.tbFirmwareConfig.row["mux_sel"]         = self.data["firmware_config"]["mux_sel"]
          self.tbFirmwareConfig.row["fft_shift"]	   = self.data["firmware_config"]["fft_shift"]
          self.tbFirmwareConfig.row["acc_len"]	       = self.data["firmware_config"]["acc_len"]
          self.tbFirmwareConfig.row.append()
          self.tbFirmwareConfig.flush()

    def writeScanPointing(self):
      if self.hdf_is_open and self.data:
          #print self.tbScanPointing["timestamp"]
          #print                self.data["scan_pointing"]["timestamp"]         
          self.tbScanPointing.row["timestamp"]= self.data["scan_pointing"]["timestamp"]
          self.tbScanPointing.row["mb01_raj"] = self.data["scan_pointing"]["MB01_raj"] 
          self.tbScanPointing.row["mb01_dcj"] = self.data["scan_pointing"]["MB01_dcj"] 
          self.tbScanPointing.row["mb02_raj"] = self.data["scan_pointing"]["MB02_raj"] 
          self.tbScanPointing.row["mb02_dcj"] = self.data["scan_pointing"]["MB02_dcj"] 
          self.tbScanPointing.row["mb03_raj"] = self.data["scan_pointing"]["MB03_raj"] 
          self.tbScanPointing.row["mb03_dcj"] = self.data["scan_pointing"]["MB03_dcj"] 
          self.tbScanPointing.row["mb04_raj"] = self.data["scan_pointing"]["MB04_raj"] 
          self.tbScanPointing.row["mb04_dcj"] = self.data["scan_pointing"]["MB04_dcj"] 
          self.tbScanPointing.row["mb05_raj"] = self.data["scan_pointing"]["MB05_raj"] 
          self.tbScanPointing.row["mb05_dcj"] = self.data["scan_pointing"]["MB05_dcj"] 
          self.tbScanPointing.row["mb06_raj"] = self.data["scan_pointing"]["MB06_raj"] 
          self.tbScanPointing.row["mb06_dcj"] = self.data["scan_pointing"]["MB06_dcj"] 
          self.tbScanPointing.row["mb07_raj"] = self.data["scan_pointing"]["MB07_raj"] 
          self.tbScanPointing.row["mb07_dcj"] = self.data["scan_pointing"]["MB07_dcj"] 
          self.tbScanPointing.row["mb08_raj"] = self.data["scan_pointing"]["MB08_raj"] 
          self.tbScanPointing.row["mb08_dcj"] = self.data["scan_pointing"]["MB08_dcj"] 
          self.tbScanPointing.row["mb09_raj"] = self.data["scan_pointing"]["MB09_raj"] 
          self.tbScanPointing.row["mb09_dcj"] = self.data["scan_pointing"]["MB09_dcj"] 
          self.tbScanPointing.row["mb10_raj"] = self.data["scan_pointing"]["MB10_raj"] 
          self.tbScanPointing.row["mb10_dcj"] = self.data["scan_pointing"]["MB10_dcj"] 
          self.tbScanPointing.row["mb11_raj"] = self.data["scan_pointing"]["MB11_raj"] 
          self.tbScanPointing.row["mb11_dcj"] = self.data["scan_pointing"]["MB11_dcj"] 
          self.tbScanPointing.row["mb12_raj"] = self.data["scan_pointing"]["MB12_raj"] 
          self.tbScanPointing.row["mb12_dcj"] = self.data["scan_pointing"]["MB12_dcj"] 
          self.tbScanPointing.row["mb13_raj"] = self.data["scan_pointing"]["MB13_raj"] 
          self.tbScanPointing.row["mb13_dcj"] = self.data["scan_pointing"]["MB13_dcj"] 
          self.tbScanPointing.row["azimuth"]  = self.data["scan_pointing"]["azimuth"]  
          self.tbScanPointing.row["elevation"]      = self.data["scan_pointing"]["elevation"]  
          self.tbScanPointing.row["par_angle"]      = self.data["scan_pointing"]["par_angle"]  
          self.tbScanPointing.row["focus_tan"]      = self.data["scan_pointing"]["focus_tan"]  
          self.tbScanPointing.row["focus_axial"]    = self.data["scan_pointing"]["focus_axial"]
          self.tbScanPointing.row["focus_rot"]      = self.data["scan_pointing"]["focus_rot"]
          self.tbScanPointing.row.append()
          self.tbScanPointing.flush()  
          
    
    def run(self):
        """ Main HDF writer routine """
        try:
            print "HDF server: writing to directory %s..."%self.dir_path
            # Tell threadMonitor that all is OK
            threadmon.hdf_ok = True
            while self.server_enabled:
              # Note that no data will be written to queue when TCS thread is set to disabled,
              # So no need to check self.hdf_write_enable  
              self.data = self.hdfQueue.get()
              
              validKeys = {
                'pointing'    : self.writePointing,
                'raw_data'    : self.writeRawData,
                'observation' : self.writeObservation,
                'weather'     : self.writeWeather,
                'firmware'    : self.writeFirmwareConfig,
                'scan_pointing': self.writeScanPointing
              }
            
              for key in self.data.keys():
                #print "writing row: %s | "%key,
                validKeys[key]()
        except:
            print "Error: HDF server has crashed."
            threadmon.hdf_ok = False
            raise



class katcpServer(threading.Thread):
    """ Server to control ROACH boards"""
    def __init__(self, queue):
        threading.Thread.__init__(self)
        self.queue = queue
        self.server_enabled = True
        self.debug = False
    
    def toJson(self, npDict):
        """ Converts a dictionary of numpy arrays into a dictionary of lists."""
        for key in npDict.keys():
            for datakey in npDict[key]:
                try:
                    npDict[key][datakey] = npDict[key][datakey].tolist()
                except:
                    pass
        return json.dumps(npDict)
    
    def run(self):
        """ Thread run method. Fetch data from roach"""
        try:
          while self.server_enabled:
            # Get input queue info (FPGA object) 
            fpga = self.queue.get()
            #print "%s started: %s"%(self.getName(), fpga.host)
            beam_id = roachlist[fpga.host]
            
            # Grab data from the FPGA
            #try:
            time.sleep(random.random()/10) # Spread out 
            data = getSpectrum(fpga)
            data["timestamp"] = timestamp
            hdfData = {'raw_data': { beam_id : data }}     
            plotData = squashSpectrum(data)
            if hdfThread.hdf_write_enable and hdfThread.hdf_is_open:
                #print "Putting into queue"
                hdfThread.hdfQueue.put(hdfData)
            
            msgdata = {beam_id : {
                         'xx' : plotData['xx'], 
                         'yy' : plotData['yy'], 
                         'timestamp': time.time()}
                       }
                   
            msg = self.toJson(msgdata)
            plotterThread.udpQueue.append(msg)
                
            #except:
            #    print "Warning: couldn't grab data from %s"%fpga.host
            
            # Signal to queue task complete
            self.queue.task_done()
        except:
          print "Error: KATCP server has crashed."
          threadmon.katcp_ok = False
          raise
        


def getSpectraThreaded(fpgalist, queue):
    """ Starts multiple KATCP servers to collect data from ROACH boards
    
    Spawns multiple threads, with each thread retrieving from a single board.
    A queue is used to block until all threads have completed.   
    """

    # Run threads using queue
    for fpga in fpgalist:
      katcpQueue.put(fpga)
      
    # Make sure all threads have completed
    katcpQueue.join()


#START OF MAIN:
if __name__ == '__main__':
    
    # Option parsing to allow command line arguments to be parsed
    p = OptionParser()
    p.set_usage('hipsr_server.py [options]')
    p.set_description(__doc__)
    p.add_option("-p", "--projectid", dest="project_id", type="string", default=None,
                 help="Project ID")
    p.add_option("-v", "--verbose", dest="verbose", action='store_true', help="Turn on debugging (verbose mode)")
    
    (options, args) = p.parse_args(sys.argv[1:])
    
    print "\nHIPSR SERVER"
    print "------------"
    print "Version:       %s"%__version__
    print "Last modified: %s"%__modified__
    print "Report bugs to %s\n"%__email__
    
    if options.project_id: 
        project_id = options.project_id
    else:
        project_id = raw_input("Please enter your project ID: ")
    
    
    # Start logger
    print "\nStarting logger"
    print "-----------------"
    threadmon = threadMonitor()
    
    now = time.gmtime(time.time())
    now_str    = "%d-%02d-%02d_%02d%02d%02d"%(now.tm_year, now.tm_mon, now.tm_mday, now.tm_hour, now.tm_min, now.tm_sec)
    loggerstamp  = "%s_%s.log"%(project_id, now_str)
    dir_path    = os.path.join(config.data_dir, project_id)
    
    if not os.path.exists(dir_path):
        print "Creating directory %s"%dir_path
        os.makedirs(dir_path)
    
    sys.stdout = Logger(loggerstamp, dir_path)
    print "Logfile %s created in %s"%(loggerstamp, dir_path)
    
    print "\nConfiguration"
    print "-------------"
    print "TCS host:        %15s    port: %5s"%(config.tcs_server,     config.tcs_port)
    print "Plotter host:    %15s    port: %5s"%(config.plotter_host, config.plotter_port)
    
    print "FPGA firmware:    %s"%config.boffile
    print "FPGA reprogram:   %s"%config.reprogram
    print "FPGA reconfigure: %s"%config.reconfigure
    print "KATCP port:       %s"%config.katcp_port
    # Configuration parameters
    boffile      = config.boffile
    reprogram    = config.reprogram
    reconfigure  = config.reconfigure
    plotter_host = config.plotter_host
    plotter_port = config.plotter_port
    tcs_server   = config.tcs_server
    tcs_port     = config.tcs_port
    roachlist    = config.roachlist
    katcp_port   = config.katcp_port
    
    
    print "\nStarting TCS server"
    print "-------------------"
    tcsThread = tcsServer(config.tcs_server, config.tcs_port)
    tcsThread.setDaemon(True)
    tcsThread.start()
    tcsThread.send_udp = True
    if options.verbose:
        tcsThread.debug = True
    time.sleep(0.1)
    
    print "\nStarting Plotter server"
    print "-----------------------"
    plotterThread = plotterServer(config.plotter_host, config.plotter_port)
    plotterThread.setDaemon(True)
    plotterThread.start()
    if options.verbose:
        plotterThread.debug = True
    time.sleep(0.1)
    
    print "\nStarting HDF server"
    print "--------------------"
    hdfThread = hdfServer(project_id, dir_path)
    hdfThread.setDaemon(True)
    hdfThread.new_file_each_obs = False
    hdfThread.start()
    if options.verbose:
        hdfThread.debug = True
    time.sleep(0.1)
    
    # Connect to ROACH boards
    print "\nConfiguring FPGAs"
    print "-----------------\n"
    fpgalist  = [katcp_wrapper.FpgaClient(roach, config.katcp_port, timeout=10) for roach in config.roachlist]
    time.sleep(0.5)
    
    if(reprogram): katcp_helpers.reprogram()
    else: print "skipping reprogramming..."
    
    if(reconfigure): katcp_helpers.reconfigure()
    else: print "skipping reconfiguration.."
    
    time.sleep(1)
    
    print "\nStarting KATCP servers"
    print "------------------------"
    katcpQueue = Queue.Queue()
    for i in range(len(fpgalist)):
       t = katcpServer(katcpQueue)
       t.setDaemon(True)
       t.start()
    print "%i KATCP server daemons started."%len(fpgalist)
    # Tell threadMonitor that all is OK
    threadmon.katcp_ok = True
    
    # Now to start data accumulation while loop
    timestamp = time.time()
    getSpectraThreaded(fpgalist, katcpQueue)
    acc_old, acc_new = fpgalist[0].read_int('o_acc_cnt'), fpgalist[0].read_int('o_acc_cnt')
    
    
    print "\n Starting data capture"
    print "------------------------"
    
    if not threadmon.tcs_ok:
        print "It looks like TCS isn't connecting. Most likely the TCP port is already assigned."
        print "You can check if the port is in use by running:"
        print ">> netstat | grep %s"%config.tcs_port  
          
    
    hdfThread.hdf_write_enable = False  
        
    while threadmon.allSystemsGo():      
      if acc_new > acc_old:
        if hdfThread.hdf_write_enable: print "[WE]",
        else: print "[WD]",
        timestamp = time.time()
        now = time.gmtime(timestamp)
        now_fmt = "%d-%02d-%02d %02d:%02d:%02d"%(now.tm_year, now.tm_mon, now.tm_mday, now.tm_hour, now.tm_min, now.tm_sec)    
        
        ra, dec = float(tcsThread.scan_pointing["MB01_raj"]), float(tcsThread.scan_pointing["MB01_dcj"])
        print("UTC: %s, RA: %02.2f, DEC: %02.2f, Acc: %i"%(now_fmt, ra, dec, acc_new))
        acc_old=acc_new
        timestamp = time.time()
        getSpectraThreaded(fpgalist, katcpQueue)

      acc_new = fpgalist[0].read_int('o_acc_cnt')
      time.sleep(0.5)
    
    if not threadmon.allSystemsGo():
        print "Error: Not all threads running! This script will now close."
        print "TCS thread: %s"%threadmon.tcs_ok
        print "HDF thread: %s"%threadmon.hdf_ok
        print "Plotter thread: %s"%threadmon.plotter_ok
        print "KATCP threads: %s"%threadmon.katcp_ok
        
