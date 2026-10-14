import subprocess
from pathlib import Path
root=Path('/home/rob/projects/splinterdb')
coord=root/'.cache/projects-20261011/coordination'
runner=coord/'filter-predicate-20261013/tests-main-0fa3bd6c/run_validation.py'
for mode in ['debug','release']:
 for phase in ['build','test']:
  subprocess.run(['python3',str(coord/'run_job.py'),'--name',f'predicate-main-{mode}-{phase}',phase,'--','python3',str(runner),phase,mode],check=True,cwd=root)
