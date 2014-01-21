#!/usr/bin/env python
import subprocess, os

class alreadyRunningError(Exception):
    def __init__(self, proc, pid):
        print "\n"
        print "### WARNING"
        print "%s running as PID %i"%(proc, pid)
        print "It looks like there is already a HIPSR server running."
        print "You will need to close this process before continuing."
        print "To kill this process from the command line, type:\n"
        print "  kill %s\n"%pid
        print "Please check with other observers before killing this process."
        print "This script will now exit."
        print "\n"
        exit()

def checkpids():
  """ Check if any server processes are running on the server.
  If a server process is found, return its PID and name """
  current_pid = int(os.getpid())

  ps = subprocess.Popen(['ps', 'aux'], stdout=subprocess.PIPE).communicate()[0]
  processes = ps.split('\n')
  # this specifies the number of splits, so the splitted lines
  # will have (nfields+1) elements
  nfields = len(processes[0].split()) - 1
  for row in processes[1:]:
    try:
      r = row.split(None, nfields)
      pid  = int(r[1])
      proc = r[-1]
      if "server_bpsr_roach_manager.py" in r[-1]:
        raise alreadyRunningError(proc, pid)

      if "hipsr-server.py" in r[-1]:
          if pid != current_pid:
            raise alreadyRunningError(proc, pid)

    except IndexError:
      pass
    except ValueError:
      pass

if __name__ == '__main__':
  checkpids()