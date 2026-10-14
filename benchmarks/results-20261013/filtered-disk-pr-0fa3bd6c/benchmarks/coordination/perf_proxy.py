"""Broadcast acknowledged phase controls to perf stat and perf record.
The unmodified C harness talks to the input FIFO; both instruments must ack
before the harness starts/stops its measured loop. Used for diagnostic samples
only; compare these sampled runs separately from the ordinary stat-only data.
"""
import os,select,threading,time
from pathlib import Path
class PerfProxy:
 def __init__(self,folder):
  self.folder=Path(folder);self.paths={};self.stop=threading.Event();self.error=None;self.commands=[]
  for name in ['control','ack','stat-control','stat-ack','record-control','record-ack']:
   p=self.folder/(name+'.fifo');os.mkfifo(p);self.paths[name]=p
  self.fds={k:os.open(v,os.O_RDWR|os.O_CLOEXEC) for k,v in self.paths.items()}
  self.thread=threading.Thread(target=self._run,daemon=True);self.thread.start()
 def _run(self):
  try:
   while not self.stop.is_set():
    ready,_,_=select.select([self.fds['control']],[],[],0.25)
    if not ready:continue
    data=os.read(self.fds['control'],256);assert data in [b'enable\n',b'disable\n'],data
    rec=dict(command=data.decode().strip(),received_mono_ns=time.monotonic_ns())
    for instrument in ['stat','record']:
     os.write(self.fds[instrument+'-control'],data)
     ready,_,_=select.select([self.fds[instrument+'-ack']],[],[],8)
     assert ready,instrument+' timed out'
     ack=os.read(self.fds[instrument+'-ack'],256);assert ack.startswith(b'ack'),ack
     rec[instrument+'_ack_mono_ns']=time.monotonic_ns()
    os.write(self.fds['ack'],b'ack\n');self.commands.append(rec)
  except BaseException as exc:self.error=repr(exc)
 def close(self):
  self.stop.set();self.thread.join(timeout=10)
  for fd in self.fds.values():os.close(fd)
  for p in self.paths.values():p.unlink()
  assert not self.thread.is_alive() and self.error is None,self.error
