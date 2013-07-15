#! /usr/bin/env python
# encoding: utf-8
"""
tcs_server.py
=============

TCS Server class for hipsr.
"""

import time, sys, os, socket, select, re
from datetime import datetime
import hipsr_core.config as config
import hipsr_core.astroCoords as coords
import mpserver


class TcsServer(mpserver.MpServer):
    """ TCS server class 
    
    A TCP server which listens for TCS commands and parses them.
    Values are then stored in a shared python dictionary.
    """

    def __init__(self, host, port, printQueue, mainQueue, hdfQueue, plotterQueue, debug=False):

        self.name = 'tcs_server'
        self.hdf_write_enable = False
        self.hdf_is_open = False
        self.server_enabled = True
        self.hdfQueue = hdfQueue
        self.printQueue = printQueue
        self.plotterQueue = plotterQueue
        self.mainQueue = mainQueue
        self.host = host
        self.port = port
        self.ack_msg = "ok\n"
        self.send_udp = True
        self.debug = debug
        self.new_filename = None

        self.obs_setup = {
            'frequency': 0,
            'bandwidth': 0,
            'receiver': 'Parkes multibeam',
            'telescope': 'Parkes 64m',
            'project_id': 'P',
            'num_beams': 0,
            'ref_beam': 0,
            'feed_rotation': '',
            'feed_angle': 0,
            'acc_len': 0,
            'dwell_time': 0,
            'observer': 'D Fault',
            'scan_rate': 0
        }

        self.pointing_data = {
            'timestamp': 0,
            'ra': 0,
            'dec': 0,
            'source': ''
        }

        self.scan_pointing = {
            'focus_tan': 0,
            'focus_axi': 0,
            'focus_rot': 0,
            'par_angle': 0,
            'azimuth': 0,
            'elevation': 0,
            'timestamp': 0
        }

        # Add keys for multibeam RA and DEC entries
        raj_keys = ["MB%02d_raj" % b_id for b_id in range(1, 14)]
        for key in raj_keys:
            self.scan_pointing[key] = 0
        dcj_keys = ["MB%02d_dcj" % b_id for b_id in range(1, 14)]
        for key in dcj_keys:
            self.scan_pointing[key] = 0

        super(TcsServer, self).__init__(self.name, printQueue, mainQueue)

    def setFreq(self, val):
        self.mprint("%-15s : %s" % ("Central freq.", val.strip()))
        self.obs_setup["frequency"] = val.strip()
        if self.send_udp:
            msg = self.toJsonCmd('tcs-frequency', val.strip())
            self.plotterQueue.put(msg)
        return self.ack_msg

    def setBandwidth(self, val):
        self.mprint("%-15s : %s" % ("Bandwidth", val.strip()))
        self.obs_setup["bandwidth"] = val.strip()
        if self.send_udp:
            msg = self.toJsonCmd('tcs-bandwidth', val.strip())
            self.plotterQueue.put(msg)
        return self.ack_msg

    def setObserver(self, val):
        self.mprint("%-15s : %s" % ("Observer", val.strip()))
        self.obs_setup["observer"] = val.strip()
        return self.ack_msg

    def setObsMode(self, val):
        self.mprint("%-15s : %s" % ("Obs. mode", val.strip()))
        self.obs_setup["obs_mode"] = val.strip()
        return self.ack_msg

    def setSrc(self, val):
        self.mprint("%-15s : %s" % ("Source name", val.strip()))
        self.pointing_data["source"] = val.strip()
        return self.ack_msg

    def setRa(self, val):
        ra = coords.rastring2deg(val)
        self.mprint("%-15s : %s" % ("Source RA", val.strip()))
        self.pointing_data["ra"] = ra
        return self.ack_msg

    def setDec(self, val):
        dec = coords.decstring2deg(val)
        self.mprint("%-15s : %s" % ("Source DEC", val.strip()))
        self.pointing_data["dec"] = dec
        return self.ack_msg

    def setReceiver(self, val):
        self.mprint("%-15s : %s" % ("Receiver", val.strip()))
        self.obs_setup["receiver"] = val.strip()
        return self.ack_msg

    def setProjectId(self, val):
        self.mprint("%-15s : %s" % ("Project ID", val.strip()))
        self.obs_setup["project_id"] = val.strip()
        return self.ack_msg

    def setNumBeams(self, val):
        self.mprint("%-15s : %s" % ("No. beams", val.strip()))
        self.obs_setup["num_beams"] = val.strip()
        return self.ack_msg

    def setRefBeam(self, val):
        self.mprint("%-15s : %s" % ("Ref. beam", val.strip()))
        self.obs_setup["ref_beam"] = val.strip()
        return self.ack_msg

    def setFeedRotation(self, val):
        self.mprint("%-15s : %s" % ("Feed rotation", val.strip()))
        self.obs_setup["feed_rotation"] = val.strip()
        return self.ack_msg

    def setFeedAngle(self, val):
        self.mprint("%-15s : %s" % ("Feed angle", val.strip()))
        self.obs_setup["feed_angle"] = val.strip()
        return self.ack_msg

    def setAccLen(self, val):
        self.mprint("%-15s : %s" % ("Acc. length", val.strip()))
        self.obs_setup["acc_len"] = val.strip()
        return self.ack_msg

    def setDwellTime(self, val):
        self.mprint("%-15s : %s" % ("Dwell time", val.strip()))
        self.obs_setup["dwell_time"] = val.strip()
        return self.ack_msg

    def setConfName(self, val=0):
        self.mprint("%-15s : %s" % ("Config name", val.strip()))
        self.obs_setup["conf_name"] = val.strip()
        return self.ack_msg

    def setScanRate(self, val=0):
        self.mprint("%-15s : %s" % ("Scan rate", val.strip()))
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
        if self.hdf_write_enable:
            self.hdfQueue.put({'scan_pointing': self.scan_pointing})
        return self.ack_msg

    def setNoMatch(self, val=0):
        """ Default action if command is not found in command dictionary"""
        self.mprint("Error: command not supported.")
        if val: self.mprint(val)
        return self.ack_msg

    def startObs(self, val=0):
        """ Starts observation data capture """
        # Check if HDF file is currently open

        timestamp = time.time()
        now = time.gmtime(timestamp)
        date_fmt = "start_utc %d-%02d-%02d_%02d%02d%02d" % (
            now.tm_year, now.tm_mon, now.tm_mday, now.tm_hour, now.tm_min, now.tm_sec)
        self.mprint("starting observation: %s" % date_fmt)

        if not self.hdf_is_open:
            self.hdfQueue.put({"create_new_file": self.new_filename})
            #elif self.new_file_each_obs:
        #    self.hdfQueue.put({"create_new_file" : None})

        self.obs_setup["date"] = timestamp
        self.pointing_data["timestamp"] = timestamp

        hdf_data = []
        hdf_data.append({'observation': self.obs_setup})
        hdf_data.append({'pointing': self.pointing_data})

        self.hdfQueue.put({'write_enable': True})
        self.mainQueue.put({'write_enable': True})
        self.hdf_write_enable = True

        return date_fmt, hdf_data

    def newFile(self, val):
        # Create new file
        #self.hdfQueue.put({"create_new_file" : val.strip()})
        self.new_filename = val.strip()
        #print "HERE: %s" % self.new_filename
        return self.ack_msg

    def kill(self, val=None):
        """ The kill switch - sends shutdowns to all processes """
        #self.mprint("TCS I/O: Kill signal received.")
        self.mainQueue.put({'kill': True})
        self.server_enabled = False

    def commandDict(self, cmd, val):
        """ This is essentially a case statement that searches for commands in a dict. """
        return {
            'freq': self.setFreq,
            'src': self.setSrc,
            'ra': self.setRa,
            'dec': self.setDec,
            'band': self.setBandwidth,
            'receiver': self.setReceiver,
            'pid': self.setProjectId,
            'nbeam': self.setNumBeams,
            'refbeam': self.setRefBeam,
            'feedrotation': self.setFeedRotation,
            'feedangle': self.setFeedAngle,
            'taccum': self.setAccLen,
            'dwell': self.setDwellTime,
            'confname': self.setConfName,
            'observer': self.setObserver,
            'obstype': self.setObsMode,
            'start': self.startObs,
            'az': self.setAzimuth,
            'el': self.setElevation,
            'par': self.setParAngle,
            'focustan': self.setFocusTan,
            'focusaxi': self.setFocusAxi,
            'focusrot': self.setFocusRot,
            'utc_cycle': self.setUtcCycle,
            'utc_cycle_end': self.endUtcCycle,
            'new_file': self.newFile,
            'scanrate': self.setScanRate
        }.get(cmd, self.setNoMatch)(val)    # setNoMatch is default if cmd not found

    def serverMain(self):
        """ Run TCP/IP server """
        if self.debug:
            self.mprint("TCS I/O: Debug mode")
            tcs_regex = '(?P<cmd>\w+)\s(?P<val>.+)'
        else:
            tcs_regex = '(?P<cmd>\w+)\s(?P<val>.+)%s' % config.tcs_regex_esc

        self.mprint("TCS listener: Waiting for TCS data %s:%s... " % (self.host, self.port))
        open_sockets = []
        listening_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        listening_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listening_socket.bind((self.host, self.port))
        listening_socket.listen(5)

        while self.server_enabled:
        # Waits for I/O being available for reading from any socket object.
            rlist, wlist, xlist = select.select([listening_socket] + open_sockets, [], [])
            for i in rlist:
                if i is listening_socket:
                    new_socket, addr = listening_socket.accept()
                    open_sockets.append(new_socket)
                else:
                    try:
                        socket_ok = True
                        data = i.recv(1024)
                    except:
                        socket_ok = False
                        self.mprint("TCS I/O: Cannot communicate on socket. Removing from socket list.")
                    if data == "":
                        socket_ok = False
                        self.mprint("TCS I/O: Connection closed.")
                    if not socket_ok:
                        try:
                            open_sockets.remove(i)
                        except TypeError:
                            self.mprint("TCS I/O: Socket already removed from open socket list.")
                    else:
                        if self.debug:
                            self.mprint(repr(data))

                        # Check for start message
                        is_start = re.search('start%s' % config.tcs_regex_esc, data)
                        if is_start:
                            self.mprint("TCS I/O: received start.")
                            start_msg, hdf_data = self.startObs()
                            i.send(start_msg)
                            time.sleep(1e-3)
                            #i.close()

                            for item in hdf_data:
                                self.hdfQueue.put(item)

                        # Check for stop message  
                        is_stop = re.search('stop%s' % config.tcs_regex_esc, data)
                        if is_stop:
                            self.hdfQueue.put({'write_enable': False})
                            self.mainQueue.put({'write_enable': False})
                            self.hdf_write_enable = False
                            self.mprint("TCS I/O: received stop. Write disabled.")
                            i.send(self.ack_msg)

                        # And check for kill
                        is_kill = re.search('kill%s' % config.tcs_regex_esc, data)
                        if is_kill:
                            self.kill()

                        is_utc_end = re.search('utc_cycle_end%s' % config.tcs_regex_esc, data)
                        if is_utc_end:
                            self.endUtcCycle()
                            i.send(self.ack_msg)

                        #is_conf = re.search('confname %s'%config.tcs_regex_esc, data)
                        #if is_conf:
                        #    self.mprint("confname rec'd.")
                        #    i.send(self.ack_msg)

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