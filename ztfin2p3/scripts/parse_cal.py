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
        colnames = colnames + ("FILTRKEY", "FLTNORM", "FLTNORM_FP")

    meta = []
    for date in sorted(os.listdir(path)):
        flist = sorted(os.listdir(path / date))
        if verbose:
            print(date, len(flist))
        for fname in flist:
            hdr = fits.getheader(path / date / fname)
            meta.append([hdr[k] for k in colnames if k in hdr.keys()])
            # Checking for key to catch KeyError

    df = pd.DataFrame(meta, columns=colnames)
    if outfile:
        df.to_parquet(outfile)
    return df


@click.command(context_settings={"show_default": True})
@click.argument("year", nargs=-1)
@click.option("--clean", is_flag=True, help="keep only complete days?")
@click.option("--prefix", is_flag=False, default='',  help="prefix to file")
def parse_cal(year, clean, prefix):
    """Parse calibration folder to produce catalogs."""

    CAL = pathlib.Path(CAL_DIR)
    BIAS = CAL / "bias"
    FLAT = CAL / "flat"

    for y in year:
        bias = parse_tree(BIAS / y)
        flat = parse_tree(FLAT / y)
        print(f"{len(bias)} bias, {len(flat)} flats")

        if clean:
            print("removal incomplete days")
            dates = pd.date_range(f"{y}-01-01", f"{y}-12-31", freq="D")
            df = pd.DataFrame(dates.astype(str).str.replace("-", ""), columns=["date"])

            # count biases
            df2 = pd.DataFrame(bias.groupby("PERIOD").size(), columns=["nbias"])
            df = df.join(df2, "date")

            # count flats
            df2 = flat.pivot_table(
                index="PERIOD", columns="FILTRKEY", aggfunc="count", values="IMGTYPE"
            )
            df = df.join(df2, "date")

            df = df.fillna(0).astype(int)
            df["tot"] = df[["nbias", "zg", "zi", "zr"]].sum(axis=1)

            # Need to think some more on this operation.
            # Especially since in the future we might have no bias but flats for a day.
            to_remove = df[df.tot.lt(64)].date.astype(str).tolist()

            bias = bias[~bias.PERIOD.isin(to_remove)]
            flat = flat[~flat.PERIOD.isin(to_remove)]
            print(f"{len(bias)} bias, {len(flat)} flats")
        
        else : 
            prefix = 'uncut_' 
            #Hard-code prefix to get no cut metadata and prevent accidental overwriting.

        bias.to_parquet(BIAS / "meta" / f"{prefix}masterbias_metadata_{y}.parquet")
        flat.to_parquet(FLAT / "meta" / f"{prefix}masterflat_metadata_{y}.parquet")
