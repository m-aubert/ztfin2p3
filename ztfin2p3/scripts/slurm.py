import pathlib
import subprocess

import pandas as pd
import rich_click as click
from ztfin2p3.metadata import get_rawmeta


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
@click.option("--date", help="date to process")
@click.option("--to", help="specify the end of the period to process")
@click.option("--freq", default="D", help="frequency: D=daily, W=weekly, M=monthly")
@click.option("--table", help="parquet table with files to process")
@click.option("--dry-run", is_flag=True, help="show slurm command, don't run")
@click.option("--envpath", help="path to the environment where ztfin2p3 is located")
@click.option("--logdir", default=".", help="path where logs are stored")
@click.option("--split-ccds", is_flag=True, help="split CCDs using a job array?")
# slurm
@click.option("--account", default="ztf", help="account to charge resources to")
@click.option("--cpu-time", "-c", default="2:00:00", help="cputime limit")
@click.option("--mem", "-m", default="16GB", help="memory limit")
@click.option("--partition", default="htc", help="partition for resource allocation")
#
@click.argument("args", nargs=-1, type=click.UNPROCESSED)
def run(
    cmd,
    date,
    to,
    freq,
    table,
    dry_run,
    envpath,
    logdir,
    split_ccds,
    account,
    cpu_time,
    mem,
    partition,
    args,
):
    """Run another subcommand on a Slurm cluster. Additional arguments are
    passed to the subcommand.
    """

    logdir = pathlib.Path(logdir)
    logdir.mkdir(exist_ok=True)

    if envpath:
        ztfcmd = f"{envpath}/bin/ztfin2p3"
    else:
        ztfcmd = "ztfin2p3"

    def srun(cmdstr, cpu_time, array=None, ccdid=None, **kwargs):
        logfile = "slurm-%A-%a.log" if array else "slurm-%j.log"
        name = f"ztf_{cmd}"
        if date is not None:
            name = +f"_{date.replace('-', '')}"
        if ccdid is not None:
            name += f"_ccd{ccdid}"

        sbatch_cmd = sbatch(
            name,
            cmdstr,
            array=array,
            cpu_time=cpu_time,
            mem=mem,
            account=account,
            partition=partition,
            output=logdir / logfile,
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
        cmdstr = f"{ztfcmd} {cmd} {date} " + " ".join(args)
        srun(cmdstr, cpu_time)

    elif cmd == "calib":
        if to is not None:
            days = pd.date_range(date, to, freq=freq)
        else:
            days = pd.date_range(date, date)

        for day in days:
            date = str(day.date())
            cmdstr = f"{ztfcmd} {cmd} {date} --statsdir {logdir} "
            cmdstr += " ".join(args)
            if split_ccds:
                cmdstr += r" --ccdid \$SLURM_ARRAY_TASK_ID"
                srun(cmdstr, cpu_time, array="1-16")
            else:
                srun(cmdstr, cpu_time)

    elif cmd == "d2a":
        if date is not None:
            if to is not None:
                meta = get_rawmeta("science", [date, to], use_dask=False)
            else:
                meta = get_rawmeta("science", date, use_dask=False)

            meta.day = pd.to_datetime(meta.day)
            meta = meta.groupby(["day", "ccdid"]).size().unstack(["ccdid"]).fillna(0)
            meta = meta.astype(int)

            for day, row in meta.iterrows():
                # cpu_time: ~25s without pocket, >1min with
                # no pocket: 30s * 200exp = 100min
                # pocket: 80s * 100exp = 130min
                corr_pocket = day >= pd.to_datetime("20191022")
                if corr_pocket:
                    chunk_size = 100
                    cpu_time = "04:00:00"
                else:
                    chunk_size = 200
                    cpu_time = "03:00:00"

                for ccdid, n_files in row.items():
                    n_chunks = n_files // chunk_size + (
                        1 if n_files % chunk_size else 0
                    )

                    date = str(day.date())
                    cmdstr = f"{ztfcmd} {cmd} {date} --statsdir {logdir} "
                    cmdstr += f"-d --aper --use-closest-calib --ccdid {ccdid} "
                    cmdstr += f"--chunk-size {chunk_size} "
                    cmdstr += r"--chunk-id \$SLURM_ARRAY_TASK_ID"
                    cmdstr += " ".join(args)

                    print(f"{date=} {ccdid=} {n_files=} {n_chunks=} {cpu_time=}")
                    srun(cmdstr, cpu_time, array=f"0-{n_chunks-1}", ccdid=ccdid)
        else:
            chunk_size = 100
            cpu_time = "06:00:00"
            # read one column just to get the number of rows
            df = pd.read_parquet(table, columns=["index"])
            n_files = len(df)
            n_chunks = n_files // chunk_size + (1 if n_files % chunk_size else 0)

            cmdstr = f"{ztfcmd} {cmd} --statsdir {logdir} "
            cmdstr += f"-d --aper --use-closest-calib --table {table} "
            cmdstr += f"--chunk-size {chunk_size} "
            cmdstr += r"--chunk-id \$SLURM_ARRAY_TASK_ID "
            cmdstr += " ".join(args)

            print(f"running jobs for {n_files=} {n_chunks=} {cpu_time=}")
            srun(cmdstr, cpu_time, array=f"0-{n_chunks-1}")

    else:
        raise ValueError("unknown command")
