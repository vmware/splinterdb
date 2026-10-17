from pathlib import Path
import subprocess,tarfile,io,tempfile,shutil
wt=Path('/home/rob/projects/splinterdb/.cache/projects-20261011/worktrees/log-group-limit')
with tempfile.TemporaryDirectory(prefix='log-async-format-') as d:
 root=Path(d)
 tarfile.open(fileobj=io.BytesIO(subprocess.check_output(['git','archive','HEAD'],cwd=wt))).extractall(root,filter='data')
 for p in subprocess.check_output(['git','diff','--name-only','HEAD'],cwd=wt,text=True).splitlines():
  target=root/p
  if (wt/p).exists():target.parent.mkdir(parents=True,exist_ok=True);shutil.copyfile(wt/p,target)
  elif target.exists():target.unlink()
 subprocess.run(['bash','format-check.sh'],cwd=root,check=True)
