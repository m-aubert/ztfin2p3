import rich_click as click

from ztfin2p3 import __version__
from ztfin2p3.scripts.detrend2aper import d2a
from ztfin2p3.scripts.calib import calib
from ztfin2p3.scripts.parse_cal import parse_cal
from ztfin2p3.scripts.slurm import run
from ztfin2p3.scripts.qa_stats import QA_flat, QA_bias


@click.group()
@click.version_option(__version__)
def cli():
    """CLI for the ZTFIN2P3 pipeline."""


cli.add_command(calib)
cli.add_command(d2a)
cli.add_command(parse_cal)
cli.add_command(run)
cli.add_command(QA_flat)
cli.add_command(QA_bias)


if __name__ == "__main__":
    cli()
