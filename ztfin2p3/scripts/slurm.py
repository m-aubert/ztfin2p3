import os
import subprocess

import pandas as pd
import rich_click as click


def sbatch(
    job_name,
    cmd,
    cpus=1,
    cpu_time="02:00:00",
    mem="4GB",
    account=None,
    partition="batch",
    gpu=False,
    gpu_model=None,
    gpu_number=1,
    output=None,
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
    if email:
        sbatch += f" --mail-user={email} --mail-type=BEGIN,END"
    if output:
        sbatch += f" -o {output}"

    sbatch += f' --wrap "{cmd}"'

    if debug:
        print(sbatch)

    return sbatch


@click.command(context_settings={"show_default": True})
@click.argument("day")
@click.option(
    "--to", help="specify the end of the period to process, default to one day"
)
@click.option("--steps", default="bias,flat,sci,aper", help="steps to run")
@click.option("--statsdir", default=".", help="path where statistics are stored")
@click.option("--envpath", help="path to the environment where ztfin2p3 is located")
@click.option("--account", default="ztf", help="account to charge resources to")
@click.option(
    "--partition", default="htc", help="partition for the resource allocation"
)
@click.option("--dry-run", is_flag=True, help="partition for the resource allocation")
@click.option("--cpu-time", default="3:00:00", help="cputime limit")
@click.option("--mem", default="16GB", help="memory limit")
@click.option("--force", help="force reprocessing all files?", is_flag=True)
def run_d2a(
    day, to, steps, statsdir, envpath, account, partition, cpu_time, mem, dry_run, force
):
    """Run d2a for a DAY or a period on a Slurm cluster."""

    if to is not None:
        days = pd.date_range(day, to)
    else:
        days = pd.date_range(day, day)

    if envpath:
        ztfcmd = f"{envpath}/bin/ztfin2p3"
    else:
        ztfcmd = "ztfin2p3"

    for day in days:
        date = str(day.date())
        for ccdid in range(1, 17):
            cmd = f"{ztfcmd} d2a {date} --ccdid {ccdid} --statsdir {statsdir}"
            cmd += f" --steps {steps}"
            if force:
                cmd += " --force"
            sbatch_cmd = sbatch(
                f"ztf_d2a_{date.replace(" - ", " ")}_ccd{ccdid}",
                cmd,
                cpu_time=cpu_time,
                mem=mem,
                account=account,
                partition=partition,
                output=os.path.join(statsdir, "slurm-%j.log"),
            )
            if dry_run:
                print(sbatch_cmd)
            else:
                out = subprocess.check_output(
                    sbatch_cmd, shell=True, stderr=subprocess.STDOUT
                )
                print(out.decode().splitlines()[-1])
