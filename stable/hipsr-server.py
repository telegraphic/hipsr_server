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

Copyright (c) 2013 The HIPSR collaboration. All rights reserved.
"""

import time, sys, os, socket, random, select, re
from datetime import datetime
from optparse import OptionParser
from collections import deque   # Ring buffer
from warnings import warn

import numpy as np
import cPickle as pkl
import threading, Queue
try:
    import ujson as json
    USES_UJSON = True
except:
    print "Warning: uJson not installed. Reverting to python's native Json (slower)"
    import json
    USES_UJSON = False

import hipsr_core.katcp_wrapper as katcp_wrapper
from   hipsr_core.katcp_helpers import stitch, snap, squashData, squashSpectrum, getSpectrum
import hipsr_core.katcp_helpers as katcp_helpers
import hipsr_core.config as config
from   hipsr_core.hipsr6 import createMultiBeam
import hipsr_core.astroCoords as coords
from   hipsr_core.printers import Logger

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
            'focus_axi' : 0,
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
        hdfThread.hdfQueue.put({'create_new_file' : val.strip()})
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
          tcs_regex = '(?P<cmd>\w+)\s(?P<val>.+)%s'%config.tcs_regex_esc
          
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
             threadmon.tcs_ok = False
             warn("TCS server crashed.", RuntimeWarning)
             if options.verbose:
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
            threadmon.plotter_ok = False
            warn("Plotter server crashed.", RuntimeWarning)
            if options.verbose:
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
          self.hdf_file    = createMultiBeam(filename, os.path.join(self.dir_path, dirstamp))
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
          fpga_config = config.fpga_config
          fpga_config["firmware"] = config.boffile
          self.data = {'firmware_config': fpga_config}
          self.writeFirmwareConfig()
          self.data = None
          
      except:
          threadmon.hdf_ok = False
          warn("HDF server: Could not create new file.", RuntimeWarning)
          if options.verbose:
              raise
    
    def writePointing(self):
        """ Write pointing row from stored data """
        if self.hdf_is_open and self.data:
          for key in self.data["pointing"].keys():
              self.tbPointing.row[key] = self.data["pointing"][key]
          self.tbPointing.row.append()
          self.tbPointing.flush()

    def writeObservation(self):
        """ Write observation row from stored data """
        if self.hdf_is_open and self.data:
            for key in self.data["observation"].keys():
                if key != 'conf_name':  
                    self.tbObservation.row[key]  = self.data["observation"][key]    
            self.tbObservation.row.append()
            self.tbObservation.flush()
  
    def writeRawData(self):
      """ Write raw_data row from stored data """
      if self.hdf_is_open and self.data:
        for beam_id in self.data["raw_data"].keys():
            beam = self.hdf_file.getNode('/raw_data', beam_id)
            for key in self.data["raw_data"][beam_id].keys():
                beam.row[key]  = self.data["raw_data"][beam_id][key]
            beam.row.append()
            beam.flush()
      
    def writeWeather(self):
      """ Write weather row from stored data """
      if self.hdf_is_open and self.data:
          for key in self.data["weather"].keys():
              self.tbWeather.row[key]       = self.data["weather"][key]
          self.tbWeather.row.append()
          self.tbWeather.flush()
      
    def writeFirmwareConfig(self):
      """ Write firmware_config row from stored data """
      if self.hdf_is_open and self.data:
          for key in self.data["firmware_config"].keys(): 
              self.tbFirmwareConfig.row[key]        = self.data["firmware_config"][key]      
          self.tbFirmwareConfig.row.append()
          self.tbFirmwareConfig.flush()

    def writeScanPointing(self):
      """ Write scan_pointing row from stored data """
      if self.hdf_is_open and self.data:
          for key in self.data["scan_pointing"].keys():  
              # Look out for capitals!
              self.tbScanPointing.row[key.lower()] = self.data["scan_pointing"][key]
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
                validKeys[key]()
        except:
            warn("HDF server has crashed.", RuntimeWarning)
            threadmon.hdf_ok = False
            if options.verbose:
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
                    warn("katcpServer toJSON is acting strange...", RuntimeWarning)
        
        if USES_UJSON:            
            return json.dumps(npDict, double_precision=3)
        else:
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
                hdfThread.hdfQueue.put(hdfData)
            
            msgdata = {beam_id : {
                         'xx' : plotData['xx'], 
                         'yy' : plotData['yy'], 
                         'timestamp': time.time()}
                       }
                   
            msg = self.toJson(msgdata)
            plotterThread.udpQueue.append(msg)
            
            # Signal to queue task complete
            self.queue.task_done()
        except:
          warn("KATCP server has crashed.", RuntimeWarning)
          threadmon.katcp_ok = False
          if options.verbose:
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
    p.add_option("-n", "--new_file_each_obs", dest="new_file_each_obs", action='store_true', 
                 help="Start a new file each observation. If not passed, TCS will control.")
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
    if options.new_file_each_obs:
        hdfThread.new_file_each_obs = True
    else:
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
       if options.verbose:
           t.debug = True
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
        print "Error: One or more server threads have crashed! This script will now close."
        print "TCS thread: %s"%threadmon.tcs_ok
        print "HDF thread: %s"%threadmon.hdf_ok
        print "Plotter thread: %s"%threadmon.plotter_ok
        print "KATCP threads: %s"%threadmon.katcp_ok
        exit()
        
