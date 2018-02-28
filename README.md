# GridMARVEL
A tool for runnning MARVEL assembler on cluster

## REQUIREMENTS
Python (2.5 or later)  
Sun Grid Engine(SGE)  
[MARVEL](https://github.com/schloi/MARVEL)
## INSTALLATION
1. Download GridMARVEL
```commandline
git clone https://github.com/FlyPythons/GridMARVEL.git
```
2. Add the path of MARVEL into gridmarvel/config.py
```commandline
"""
This package includes all software path or directory, software default config and so on
"""

import os.path


MARVEL_DIR = "/install/path/of/MARVEL/" 
BIN_DIR = os.path.join(MARVEL_DIR, "bin")
SCRIPT_DIR = os.path.join(MARVEL_DIR, "scripts")
LIB_DIR = os.path.join(MARVEL_DIR, "lib.python")
DEF_DALIGNER_T_MIN = 20
DEF_DALIGNER_T_MAX = 100
```

## Tutorial
1. Create config for running MARVEL  
ecoli.cfg
```commandline
[general]
job_type = SGE
job_queue = all.q,asm.q
input = raw.fasta
genome_size = 4000000
coverage = 50

concurrent_tasks = 100
refresh_time = 30

pat_DBsplit_option = -x2000 -s200
pat_HPCdaligner_option = --dal 4 -j 4 -I -v -k 14 -t 20 -e 0.70
pat_HPCdaligner_sge_option = -pe smp 4

ovlp_DBsplit_option = -x2000 -s50
ovlp_HPCdaligner_option = --dal 4 -j 4 -I -v -k 14 -t 20 -e 0.70
ovlp_HPCdaligner_sge_option = -pe smp 4

LAfilter_option = -d 40 -n 300 -o 2000 -u 0
```

2. Run MARVEL assembler
```commandline
/path/of/install/bin/run_marvel.py ecoli.cfg
```