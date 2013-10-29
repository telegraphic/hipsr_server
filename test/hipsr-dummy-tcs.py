#! /usr/bin/env python
# encoding: utf-8
"""
hipsr-dummy-tcs.py
==================

A dummy TCS server, which sends data to the hipsr-server. For testing purposes only.

Copyright (c) 2013 The HIPSR collaboration. All rights reserved.
"""

import time, sys, os, socket, random, select, re

#START OF MAIN:
if __name__ == '__main__':

    command_filename =  sys.argv[1]
    print "\nLoading command file: %s"%command_filename
    command_file = open(command_filename)
    commands = command_file.readlines()
    command_file.close()
    
    # Create a TCP/IP socket
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    
    # Connect the socket to the port where the server is listening
    server_address = ('localhost', 8080)
    print 'connecting to %s port %s' % server_address
    sock.connect(server_address)
    
    try:
        for cmd in commands:
            if 'sleep' in cmd:
                time.sleep(1)
            else:
                # Send data
                print "CMD: %s"%cmd
                sock.sendall(cmd)
                time.sleep(0.05)
    except:
        pass

