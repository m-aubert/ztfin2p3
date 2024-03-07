import numpy as np
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

    cmd = (
        f"sbatch -p {partition} -J {job_name} {gpuarg}"
        f' -c {cpus} -t {cpu_time} --mem {mem} --wrap "{cmd}"'
    )
    if account:
        cmd += f"--account={account}"
    if email:
        cmd += f" --mail-user={email} --mail-type=BEGIN,END"
    if output:
        cmd += f" -o {output}"

    if debug:
        print(cmd)

    return cmd


@click.command()
@click.argument("day")
@click.option(
    "--period",
    type=int,
    default=1,
    help="number of days to process",
    show_default=True,
)
@click.option(
    "--account",
    default="ztf",
    help="account to charge resources to",
    show_default=True,
)
@click.option(
    "--partition",
    default="htc",
    help="partition for the resource allocation",
    show_default=True,
)
@click.option("--cpu-time", default="2:00:00", help="cputime limit", show_default=True)
@click.option("--mem", default="4GB", help="memory limit", show_default=True)
def run_d2a(day, period, account, partition, cpu_time, mem):
    """Run d2a for a PERIOD of days on a Slurm cluster."""

    dt1d = np.timedelta64(1, "D")

    for i in range(period):
        for ccdid in range(1, 17):
            date = str(np.datetime64(day) + i * dt1d)
            cmd = f"ztfin2p3 d2a {date} --ccdid {ccdid}"
            sbatch(
                f"ztf_d2a_{date}",
                cmd,
                cpu_time=cpu_time,
                mem=mem,
                account=account,
                partition=partition,
            )
