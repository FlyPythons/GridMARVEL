#!/usr/bin/env python

import os.path
import argparse
import logging


from gridmarvel.DAGflow import DAG, Task, ParallelTask, do_dag, set_tasks_order
from gridmarvel.config import BIN_DIR, SCRIPT_DIR, DEF_DALIGNER_T_MIN, DEF_DALIGNER_T_MAX, LIB_DIR
from gridmarvel.common import mkdir, read_config


def read_plan(plan):
    scripts = []

    with open(plan) as fh:
        for line in fh.readlines():
            line = line.strip()

            if line:
                scripts.append(line.strip())

    return scripts


def get_dal_t(genome, block_size):
    if genome != -1:
        cov_block = block_size/float(genome)

        if cov_block > 1:
            t = 4*int(cov_block)
        else:
            t = DEF_DALIGNER_T_MIN
    else:
        t = DEF_DALIGNER_T_MIN

    t = min(DEF_DALIGNER_T_MAX, max(DEF_DALIGNER_T_MIN, t))

    return t


def fofn2list(fofn):
    r = []

    with open(fofn) as fh:

        for line in fh.readlines():
            line = line.strip()

            if line:
                r.append(os.path.abspath(line))

    return r


def run_patch(cfg, work_dir):

    db = "raw_data"
    dag = DAG("graph1")

    split_options = cfg["pat_dbsplit_option"].split()
    min_length = [int(i[2:]) for i in split_options if i.startswith("-x")][0]
    block_size = [int(i[2:]) for i in split_options if i.startswith("-s")][0]

    t = get_dal_t(-1, block_size)

    task = Task(
        id="prepare_db",
        work_dir=work_dir,
        type="sge",
        option="",
        script="""
rm -rf {db}.db # delete existed db

{bin}/FA2db -c -x{min_length} {db} {fasta}
{bin}/DBsplit -s{block_size} {db}
{bin}/DBdust {db}
{bin}/HPCdaligner {args} -t {t} -r 0 -o {db} {db}
    """.format(bin=BIN_DIR, db=db, t=t,
               fasta=" ".join(fofn2list(cfg["input_fofn"])),
               args=cfg["pat_hpcdaligner_option"],
               min_length=min_length,
               block_size=block_size)
    )

    dag.add_task(task)

    do_dag(dag, 1, int(cfg["refresh_time"]))

    dal_plan = os.path.join(work_dir, "%s.dalign.plan" % db)
    meg_plan = os.path.join(work_dir, "%s.merge.plan" % db)

    # step2 run_daligne

    dag = DAG("graph2")

    dal_cmds = read_plan(dal_plan)
    meg_cmds = read_plan(meg_plan)
    blocks = [i+1 for i in range(len(meg_cmds))]

    dal_tasks = ParallelTask(
        id="dal",
        work_dir=work_dir,
        type="sge",
        option=cfg["pat_hpcdaligner_sge_option"],
        script="%s/{cmd}" % BIN_DIR,
        cmd=dal_cmds
    )

    dag.add_task(*dal_tasks)

    meg_tasks = ParallelTask(
        id="LAmge",
        work_dir=work_dir,
        type="sge",
        option="",
        script="""\
{bin}/{{cmd}}
{bin}/LAq -d 35 -b {{block}} {db} {db}.{{block}}.las
""".format(bin=BIN_DIR, db=db),
        cmd=meg_cmds,
        block=blocks
    )

    set_tasks_order(dal_tasks, meg_tasks)
    dag.add_task(*meg_tasks)

    tkmeg_task = Task(
        id="TKmerge",
        work_dir=work_dir,
        type="sge",
        option="",
        script="""\
{bin}/TKmerge -d {db} q
{bin}/TKmerge -d {db} trim
""".format(bin=BIN_DIR, db=db)
    )

    tkmeg_task.set_upstream(*meg_tasks)
    dag.add_task(tkmeg_task)

    fix_tasks = ParallelTask(
        id="LAfix",
        work_dir=work_dir,
        type="sge",
        option="",
        script="""\
{bin}/LAfix -Q 30 -g -1 {db} {db}.{{block}}.las {db}.{{block}}.fixed.fasta
""".format(bin=BIN_DIR, db=db),
        block=blocks
    )

    tkmeg_task.set_downstream(*fix_tasks)
    dag.add_task(*fix_tasks)

    fix_join_task = Task(
        id="fix_join",
        work_dir=work_dir,
        type="sge",
        option="",
        script="""
cat {db}.*.fixed.fasta > {db}.fixed.fasta
""".format(db=db)
    )

    fix_join_task.set_upstream(*fix_tasks)
    dag.add_task(fix_join_task)

    do_dag(dag,
           concurrent_tasks=int(cfg["concurrent_tasks"]),
           refresh_time=int(cfg["refresh_time"]))

    return os.path.join(work_dir, "%s.fixed.fasta" % db)


def run_asm(cfg, fix_fasta, work_dir):
    db = "fix_reads"
    dag = DAG("graph3")

    split_options = cfg["ovlp_dbsplit_option"].split()
    print(split_options)
    min_length = [int(i[2:]) for i in split_options if i.startswith("-x")][0]
    block_size = [int(i[2:]) for i in split_options if i.startswith("-s")][0]

    t = get_dal_t(cfg["genome_size"], block_size)

    db_task = Task(
        id="DB_prepare",
        work_dir=work_dir,
        type="sge",
        option="",
        script="""
{bin}/FA2db -c -x{min_length} {db} {fasta}
{bin}/DBsplit -s{block_size} {db}
{bin}/DBdust {db}
{bin}/HPCdaligner {args} -t {t} -r 2 -o {db} {db}
""".format(bin=BIN_DIR, db=db, fasta=fix_fasta, t=t,
           min_length=min_length,
           block_size=block_size,
           args=cfg["ovlp_hpcdaligner_option"])
    )

    dag.add_task(db_task)

    do_dag(dag, 1, int(cfg["refresh_time"]))

    dal_plan = os.path.join(work_dir, "%s.dalign.plan" % db)
    meg_plan = os.path.join(work_dir, "%s.merge.plan" % db)

    # step2 run_daligne

    dag = DAG("graph4")

    dal_cmds = read_plan(dal_plan)
    meg_cmds = read_plan(meg_plan)
    blocks = [i + 1 for i in range(len(meg_cmds))]

    dal_tasks = ParallelTask(
        id="dal",
        work_dir=work_dir,
        type="sge",
        option=cfg["ovlp_hpcdaligner_sge_option"],
        script="%s/{cmd}" % BIN_DIR,
        cmd=dal_cmds
    )

    dag.add_task(*dal_tasks)

    meg_tasks = ParallelTask(
        id="LAmge",
        work_dir=work_dir,
        type="sge",
        option="",
        script="""\
{bin}/{{cmd}}
{bin}/LAstitch -L -f 50  {db} {db}.{{block}}.las {db}.{{block}}.stitch.las
{bin}/LAq -d 30 -s 5 -T trim0 -b {{block}} {db} {db}.{{block}}.stitch.las
    """.format(bin=BIN_DIR, db=db),
        cmd=meg_cmds,
        block=blocks
    )

    set_tasks_order(dal_tasks, meg_tasks)
    dag.add_task(*meg_tasks)

    tkmeg_task = Task(
        id="TKmerge",
        work_dir=work_dir,
        type="sge",
        option="",
        script="""\
{bin}/TKmerge -d {db} q
{bin}/TKmerge -d {db} trim0
    """.format(bin=BIN_DIR, db=db)
    )

    tkmeg_task.set_upstream(*meg_tasks)
    dag.add_task(tkmeg_task)

    larep_tasks = ParallelTask(
        id="LArep",
        work_dir=work_dir,
        type="sge",
        option="",
        script="""\
{bin}/LArepeat -c {cov} -b {{block}} {db} {db}.{{block}}.stitch.las
    """.format(bin=BIN_DIR, db=db, cov=cfg["coverage"]),
        block=blocks
    )

    tkmeg_task.set_downstream(*larep_tasks)
    dag.add_task(*larep_tasks)

    rep_meg_task = Task(
        id="rep_meg",
        work_dir=work_dir,
        type="sge",
        option="",
        script="""\
{bin}/TKmerge -d {db} repeats
""".format(bin=BIN_DIR, db=db)
    )

    rep_meg_task.set_upstream(*larep_tasks)
    dag.add_task(rep_meg_task)

    lagap_tasks = ParallelTask(
        id="LAgap",
        work_dir=work_dir,
        type="sge",
        option="",
        script="""\
{bin}/LAgap -L -t trim0 {db} {db}.{{block}}.stitch.las {db}.{{block}}.gap.las
{bin}/LAq -s 5 -d 30 -u -t trim0 -T trim1 -b {{block}} {db} {db}.{{block}}.gap.las
    """.format(bin=BIN_DIR, db=db, cov=cfg["coverage"]),
        block=blocks
    )

    rep_meg_task.set_downstream(*lagap_tasks)
    dag.add_task(*lagap_tasks)

    gap_meg_task = Task(
        id="gap_meg",
        work_dir=work_dir,
        type="sge",
        option="",
        script="""\
{bin}/TKmerge -d {db} trim1
    """.format(bin=BIN_DIR, db=db)
    )

    gap_meg_task.set_upstream(*lagap_tasks)
    dag.add_task(gap_meg_task)

    filter_tasks = ParallelTask(
        id="LAfit",
        work_dir=work_dir,
        type="sge",
        option="",
        script="""\
{bin}/LAfilter -L -r repeats -t trim1 -T {args} {db} {db}.{{block}}.gap.las {db}.{{block}}.filtered.las
    """.format(bin=BIN_DIR, db=db, args=cfg["lafilter_option"]),
        block=blocks
    )

    gap_meg_task.set_downstream(*filter_tasks)
    dag.add_task(*filter_tasks)

    asm_task = Task(
        id="asm",
        work_dir=work_dir,
        type="sge",
        option="",
        script="""\
export PYTHONPATH={lib_dir}:$PYTHONPATH
{bin}/LAmerge -S filtered {db} {db}.filtered.las
{bin}/OGbuild -t trim1 {db} {db}.filtered.las {db}.graphml
{script}/OGtour.py -c {db} {db}.graphml
{bin}/LAcorrect -j 4 -r {db}.tour.rids {db} {db}.filtered.las {db}.corrected
{bin}/FA2db -c {db}_corrected {db}.corrected.*.fasta
{script}/tour2fasta.py -c {db}_corrected -t trim1 {db} {db}.tour.graphml {db}.tour.paths
        """.format(bin=BIN_DIR, db=db, script=SCRIPT_DIR, lib_dir=LIB_DIR)
    )

    asm_task.set_upstream(*filter_tasks)
    dag.add_task(asm_task)

    do_dag(dag=dag,
           concurrent_tasks=int(cfg["concurrent_tasks"]),
           refresh_time=int(cfg["refresh_time"]))


def run_marvel(config, work_dir):

    work_dir = mkdir(work_dir)

    # 1. patch raw reads

    patch_dir = os.path.join(work_dir, "01_patch")
    mkdir(patch_dir)
    fix_reads = run_patch(config, patch_dir)

    asm_dir = os.path.join(work_dir, "02_asm")
    mkdir(asm_dir)
    run_asm(config, fix_reads, asm_dir)


def set_args():

    args = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter,
                                   description="""
description:
    Pipeline to run MARVEL assembly

author:  fanjunpeng (jpfan@whu.edu.cn)
version: v0.1
    """)

    args.add_argument("config", help=".cfg")
    return args.parse_args()


def main():
    args = set_args()
    cfg = read_config(args.config)

    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s [%(levelname)s]-%(message)s',
                        filename=os.path.join(".", "all.log"),
                        filemode="w"
                        )

    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    formatter = logging.Formatter('[%(levelname)s] %(asctime)s  %(message)s')
    console.setFormatter(formatter)
    logging.getLogger('').addHandler(console)

    run_marvel(cfg["general"], ".")


if __name__ == "__main__":
    main()

