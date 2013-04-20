#hipsr_server

For HISPEC, all of the signal processing is done on the ROACH boards. The ROACH boards are controlled by 
a script, hipsr-server.py, which collects data from the ROACH boards and metadata (e.g. pointing info) 
from the telescope control system (TCS). The server script then writes the data to HDF5 files. 

Note that hipsr-server.py does not control telescope pointing, it only collects and collates data/metadata.

## Script overview

The hipsr-server collects, collates and writes data to HDF files. To do this, several threads must be run in parallel. 
Firstly, a connection to each ROACH board is made using KATCP, a communication protocol which runs over TCP/IP. 
Each connection runs in a separate thread. In addition to this, a TCP/IP server is set up to communicate with TCS,
which sends ASCII command : value pairs with info about telescope setup, observation config and pointing detiails.

Having multiple threads all attempting to write the same HDF file isn’t good. So, there’s a dedicated HDF thread 
which has a data input queue, into which the KATCP and TCS threads append data. Finally, so we can see what’s going on, 
TCS and KATCP threads send a subset of data over a UDP connection to the hipsr-gui.py script. This UDP connection sends 
python dictionaries converted into JSON.

## Dependencies

* standard Python modules.
* hipsr_core
* ujson
* numpy

For more info see the documentation at http://telegraphic.github.io/hipsr

