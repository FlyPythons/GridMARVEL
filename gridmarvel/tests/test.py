#!/usr/bin/env python

from gridmarvel.DAGflow import Task

def main():

    a = Task(
        id="test",
        work_dir=".",
        type="sge",
        option="-pe smp 1",
        script="hah"
    )

    print(a.option)

if __name__ == "__main__":
    main()

