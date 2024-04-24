import os
import pathlib

import pandas as pd
import rich_click as click
from astropy.io import fits

from ztfin2p3.io import CAL_DIR


def parse_tree(
    path: pathlib.Path, outfile: str = None, verbose: bool = False
) -> pd.DataFrame:
    colnames = (
        "PERIOD",
        "CCDID",
        "IMGTYPE",
        "PTYPE",
        "NFRAMES",
        "PIPETIME",
        "PIPEV",
        "ZTFIMGV",
    )
    if "flat" in str(path):
        colnames = colnames + ("FILTRKEY", "FLTNORM")

    meta = []
    for date in sorted(os.listdir(path)):
        flist = sorted(os.listdir(path / date))
        if verbose:
            print(date, len(flist))
        for fname in flist:
            hdr = fits.getheader(path / date / fname)
            meta.append([hdr[k] for k in colnames])

    df = pd.DataFrame(meta, columns=colnames)
    if outfile:
        df.to_parquet(outfile)
    return df


@click.command(context_settings={"show_default": True})
@click.argument("year", nargs=-1)
def parse_cal(year):
    """Parse calibration folder to produce catalogs."""

    CAL = pathlib.Path(CAL_DIR)
    BIAS = CAL / "bias"
    FLAT = CAL / "flat"

    for y in year:
        parse_tree(BIAS / y, outfile=BIAS / "meta" / f"masterbias_metadata_{y}.parquet")
        parse_tree(FLAT / y, outfile=FLAT / "meta" / f"masterflat_metadata_{y}.parquet")
