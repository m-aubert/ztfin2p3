import os
import subprocess

import pandas as pd
import rich_click as click


def sbatch(
    job_name,
    cmd,
    array=None,
    cpus=1,
    cpu_time="02:00:00",
    mem="4GB",
    account=None,
    partition="batch",
    gpu=False,
    gpu_model=None,
    gpu_number=1,
    output="slurm-%j.log",
    email=None,
    debug=False,
):
    if gpu:
        partition = "gpu"
        gpumod = f":{gpu_model}" if gpu_model else ""
        gpuarg = f"--gres=gpu{gpumod}:{gpu_number}"
    else:
        gpuarg = ""

    sbatch = f"sbatch -p {partition} -J {job_name} {gpuarg}"
    sbatch += f" -c {cpus} -t {cpu_time} --mem {mem}"

    if account:
        sbatch += f" --account={account}"
    if array:
        sbatch += f" --array={array}"
    if email:
        sbatch += f" --mail-user={email} --mail-type=BEGIN,END"
    if output:
        sbatch += f" -o {output}"

    sbatch += f' --wrap "{cmd}"'

    if debug:
        print(sbatch)

    return sbatch


@click.command(context_settings={"show_default": True, "ignore_unknown_options": True})
@click.argument("cmd")
@click.argument("date")
@click.option("--to", help="specify the end of the period to process")
@click.option(
    "--freq",
    default="D",
    help="frequency for date range, D=daily, W=weekly, M=monthly, etc.",
)
@click.option("--logdir", default=".", help="path where logs are stored")
@click.option("--envpath", help="path to the environment where ztfin2p3 is located")
@click.option("--account", default="ztf", help="account to charge resources to")
@click.option("--partition", default="htc", help="partition for resource allocation")
@click.option("--dry-run", is_flag=True, help="partition for the resource allocation")
@click.option("--cpu-time", "-c", default="2:00:00", help="cputime limit")
@click.option("--mem", "-m", default="8GB", help="memory limit")
@click.argument("args", nargs=-1, type=click.UNPROCESSED)
def run(
    cmd,
    date,
    to,
    freq,
    logdir,
    envpath,
    account,
    partition,
    cpu_time,
    mem,
    dry_run,
    args,
):
    """Run d2a for a DAY or a period on a Slurm cluster."""

    if envpath:
        ztfcmd = f"{envpath}/bin/ztfin2p3"
    else:
        ztfcmd = "ztfin2p3"

    def srun(cmdstr, array=None, **kwargs):
        logfile = "slurm-%A-%a.log" if array else "slurm-%j.log"
        sbatch_cmd = sbatch(
            f"ztf_{cmd}_{date.replace('-', '')}",
            cmdstr,
            array=array,
            cpu_time=cpu_time,
            mem=mem,
            account=account,
            partition=partition,
            output=os.path.join(logdir, logfile),
            **kwargs,
        )
        if dry_run:
            print(sbatch_cmd)
        else:
            out = subprocess.check_output(
                sbatch_cmd, shell=True, stderr=subprocess.STDOUT
            )
            print(out.decode().splitlines()[-1])

    if cmd == "parse-cal":
        cmdstr = f"{ztfcmd} {cmd} {date}" + " ".join(args)
        srun(cmdstr)

    elif cmd == "d2a":
        if to is not None:
            days = pd.date_range(date, to, freq=freq)
        else:
            days = pd.date_range(date, date)

        for day in days:
            date = str(day.date())
            cmdstr = rf"{ztfcmd} d2a {date} --ccdid \$SLURM_ARRAY_TASK_ID"
            cmdstr += f" --statsdir {logdir} "
            cmdstr += " ".join(args)
            srun(cmdstr, array="1-16")
    else:
        raise ValueError("unknown command")
