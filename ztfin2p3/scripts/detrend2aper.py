import datetime
import json
import logging
import pathlib
import time

import numpy as np
import rich_click as click
from astropy.io import fits
from rich.logging import RichHandler
from ztfimg import CCD
from ztfquery.buildurl import filename_to_url

from ztfin2p3 import __version__
from ztfin2p3.aperture import get_aperture_photometry, store_aperture_catalog
from ztfin2p3.io import ipacfilename_to_ztfin2p3filepath
from ztfin2p3.metadata import get_raw
from ztfin2p3.pipe import BiasPipe, FlatPipe
from ztfin2p3.science import build_science_image


def daily_datalist(fi):
    # Will probably be implemented in CalibPipe class. Will be cleaner
    datalist = fi.init_datafile.copy()
    datalist["filterid"] = datalist["ledid"]
    for key, items in fi._led_to_filter.items():
        datalist["filterid"] = datalist.filterid.replace(items, key)

    _groupbyk = ["day", "ccdid", "filterid"]
    datalist = datalist.reset_index()
    datalist = datalist.groupby(_groupbyk).ledid.apply(list).reset_index()
    datalist = datalist.reset_index()
    datalist["ledid"] = None
    return datalist


@click.command()
@click.argument("day")
@click.option(
    "-c",
    "--ccdid",
    required=True,
    type=click.IntRange(1, 16),
    help="ccdid in the range 1 to 16",
)
@click.option(
    "--statsdir",
    default=".",
    help="path where statistics are stored",
    show_default=True,
)
@click.option("--suffix", help="suffix for output science files")
def d2a(day, ccdid, statsdir, suffix):
    """Detrending to Aperture pipeline for a given day.

    \b
    Process DAY (must be specified in YYYY-MM-DD format):
    - computer master bias
    - computer master flat
    - for all science exposures, apply master bias and master flat, and run
      aperture photometry.

    """

    logging.basicConfig(
        level="INFO", format="%(message)s", datefmt="[%X]", handlers=[RichHandler()]
    )

    statsdir = pathlib.Path(statsdir)
    # limit period to 1 for now
    period = 1
    dt1d = np.timedelta64(period, "D")
    start, end = day, str(np.datetime64(day) + dt1d)

    now = datetime.datetime.now(datetime.UTC)
    stats = {
        "date": now.isoformat(),
        "start": start,
        "end": end,
        "ccd": ccdid,
        "version": __version__,
    }
    tot = time.time()

    logger = logging.getLogger(__name__)
    logger.info("processing day %s, ccd %s", day, ccdid)
    logger.info("computing bias...")
    t0 = time.time()
    # Need to rework on the skipping method though.
    bi = BiasPipe.from_period(start, end, ccdid=ccdid, skip=10)
    bi.build_daily_ccds(
        corr_nl=True,
        corr_overscan=True,
        use_dask=False,
        axis=0,
        sigma_clip=3,
        mergedhow="nanmean",
        chunkreduction=2,
        clipping_prop=dict(
            maxiters=1, cenfunc="median", stdfunc="std", masked=False, copy=False
        ),
        get_data_props=dict(overscan_prop=dict(userange=[25, 30])),
    )
    outs = bi.store_ccds(periodicity="daily", incl_header=True, overwrite=True)
    timing = time.time() - t0
    logger.info("bias done, %.2f sec.", timing)
    logger.debug("\n".join(outs))
    stats["bias"] = {"time": timing, "files": outs}

    # Generate flats :
    logger.info("computing flat...")
    t0 = time.time()
    fi = FlatPipe.from_period(*bi.period, use_dask=False, ccdid=ccdid)
    fi.build_daily_ccds(
        corr_nl=True,
        use_dask=False,
        corr_overscan=True,
        axis=0,
        apply_bias_period="init",
        bias_data="daily",
        sigma_clip=3,
        mergedhow="nanmean",
        chunkreduction=2,
        clipping_prop=dict(
            maxiters=1, cenfunc="median", stdfunc="std", masked=False, copy=False
        ),
        get_data_props=dict(overscan_prop=dict(userange=[25, 30])),
        bias=bi,
    )
    outs = fi.store_ccds(periodicity="daily_filter", incl_header=True, overwrite=True)
    timing = time.time() - t0
    logger.info("flat done, %.2f sec.", timing)
    logger.debug("\n".join(outs))
    stats["flat"] = {"time": timing, "files": outs}

    # Generate Science :
    # First browse meta data :
    rawsci_list = get_raw("science", fi.period, "metadata", ccdid=ccdid)
    rawsci_list.set_index(["day", "filtercode", "ccdid"], inplace=True)
    rawsci_list = rawsci_list.sort_index()

    flat_datalist = daily_datalist(fi)  # Will iterate over flat filters
    bias = bi.get_daily_ccd(day="".join(day.split("-")), ccdid=ccdid)[
        "".join(day.split("-")), ccdid
    ]

    newfile_dict = dict(new_suffix=suffix)
    stats["science"] = []

    for _, row in flat_datalist.iterrows():
        objects_files = rawsci_list.loc[row.day, row.filterid, row.ccdid]
        nfiles = len(objects_files)
        msg = "processing %s filter=%s ccd=%s: %d files"
        logger.info(msg, row.day, row.filterid, row.ccdid, nfiles)
        sci_info = {
            "day": row.day,
            "filter": row.filterid,
            "ccd": row.ccdid,
            "nfiles": nfiles,
            "files": [],
        }
        flat = CCD.from_data(fi.daily_filter_ccds[row["index"]])

        for i, (_, sci_row) in enumerate(objects_files.iterrows(), start=1):
            raw_file = sci_row.filepath
            logger.info("processing sci %d/%d: %s", i, nfiles, raw_file)
            t0 = time.time()
            quads, outs = build_science_image(
                raw_file,
                flat,
                bias,
                dask_level=None,
                corr_nl=True,
                corr_overscan=True,
                overwrite=True,
                fp_flatfield=False,
                newfile_dict=newfile_dict,
                return_sci_quads=True,
                overscan_prop=dict(userange=[25, 30]),
            )

            # If quadrant level :
            aper_stats = {}
            for quad, out in zip(quads, outs):
                # Not using build_aperture_photometry cause it expects
                # filepath and not images. Will change.
                logger.debug("aperture photometry for quadrant %d", quad.qid)
                fname_mask = filename_to_url(
                    out, suffix="mskimg.fits.gz", source="local"
                )
                quad.set_mask(fits.getdata(fname_mask))
                apcat = get_aperture_photometry(
                    quad,
                    cat="gaia_dr2",
                    dask_level=None,
                    as_path=False,
                    minimal_columns=True,
                    seplimit=20,
                    radius=np.linspace(3, 13),
                    bkgann=None,
                    joined=True,
                    refcat_radius=0.7,
                )
                output_filename = ipacfilename_to_ztfin2p3filepath(
                    out, new_suffix=newfile_dict["new_suffix"], new_extension="parquet"
                )
                out = store_aperture_catalog(apcat, output_filename)
                logger.debug(out)
                aper_stats[f"quad_{quad.qid}"] = {
                    "quad": quad.qid,
                    "naper": len(apcat),
                    "file": output_filename,
                }

            timing = time.time() - t0
            logger.info("sci done, %.2f sec.", timing)
            sci_info["files"].append(
                {
                    "file": raw_file,
                    "expid": sci_row.expid,
                    "time": timing,
                    **aper_stats,
                }
            )

        stats["science"].append(sci_info)

    stats["total_time"] = time.time() - tot
    logger.info("all done, %.2f sec.", stats["total_time"])

    stats_file = statsdir / f"stats_{day}_{ccdid}_{now:%Y%M%dT%H%M%S}.json"
    logger.info("writing stats to %s", stats_file)
    stats_file.write_text(json.dumps(stats))
