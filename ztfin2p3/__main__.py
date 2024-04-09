import rich_click as click

from ztfin2p3 import __version__
from ztfin2p3.scripts.detrend2aper import d2a
from ztfin2p3.scripts.slurm import run_d2a


@click.group()
@click.version_option(__version__)
def cli():
    """CLI for the ZTFIN2P3 pipeline."""


cli.add_command(d2a)
cli.add_command(run_d2a)


if __name__ == "__main__":
    cli()
