import rich_click as click

from ztfin2p3.scripts.detrend2aper import detrend2aper


@click.group()
def cli():
    """CLI for the ZTFIN2P3 pipeline."""


cli.add_command(detrend2aper)


if __name__ == "__main__":
    cli()
