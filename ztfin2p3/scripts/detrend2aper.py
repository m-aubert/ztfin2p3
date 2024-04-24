import datetime
import json
import logging
import pathlib
import time
import sys
from typing import Any

import numpy as np
import rich_click as click
from astropy.io import fits
from rich.logging import RichHandler
from ztfquery.buildurl import filename_to_url
from ztfimg import __version__ as ztfimg_version

from ztfin2p3 import __version__ as ztfin2p3_version
from ztfin2p3.aperture import get_aperture_photometry
from ztfin2p3.io import ipacfilename_to_ztfin2p3filepath
from ztfin2p3.metadata import get_raw
from ztfin2p3.pipe.newpipe import BiasPipe, FlatPipe
from ztfin2p3.science import build_science_image


BIAS_PARAMS = dict(
    corr_nl=True,
    corr_overscan=True,
    axis=0,
    sigma_clip=3,
    mergedhow="nanmean",
    clipping_prop=dict(
        maxiters=1, cenfunc="median", stdfunc="std", masked=False, copy=False
    ),
    get_data_props=dict(overscan_prop=dict(userange=[25, 30])),
)
FLAT_PARAMS = dict(
    corr_nl=True,
    corr_overscan=True,
    axis=0,
    sigma_clip=3,
    mergedhow="nanmean",
    clipping_prop=dict(
        maxiters=1, cenfunc="median", stdfunc="std", masked=False, copy=False
    ),
    get_data_props=dict(overscan_prop=dict(userange=[25, 30])),
)
SCI_PARAMS = dict(
    corr_nl=True,
    corr_overscan=True,
    overwrite=True,
    fp_flatfield=False,
    return_sci_quads=True,
    overscan_prop=dict(userange=[25, 30]),
)
APER_PARAMS = dict(
    cat="gaia_dr2",
    as_path=False,
    minimal_columns=True,
    seplimit=20,
    bkgann=None,
    joined=True,
    refcat_radius=0.7,
)


def _run_pdb(type, value, tb) -> None:  # pragma: no cover
    import pdb
    import traceback

    traceback.print_exception(type, value, tb)
    pdb.pm()


def process_sci(raw_file, flat, bias, suffix, radius, pocket, do_aper=True):
    logger = logging.getLogger(__name__)
    quads, outs = build_science_image(
        raw_file,
        flat,
        bias,
        newfile_dict=dict(new_suffix=suffix),
        corr_pocket=pocket,
        **SCI_PARAMS,
    )

    aper_stats = {}
    if not do_aper:
        return aper_stats

    for quad, out in zip(quads, outs):
        # Not using build_aperture_photometry cause it expects
        # filepath and not images. Will change.
        logger.info("aperture photometry for quadrant %d", quad.qid)
        fname_mask = filename_to_url(out, suffix="mskimg.fits.gz", source="local")
        quad.set_mask(fits.getdata(fname_mask))
        apcat = get_aperture_photometry(quad, radius=radius, **APER_PARAMS)
        output_filename = ipacfilename_to_ztfin2p3filepath(
            out, new_suffix=suffix, new_extension="parquet"
        )
        # FIXME: store radius in the catalog file!
        apcat.to_parquet(output_filename)
        aper_stats[f"quad_{quad.qid}"] = {
            "quad": quad.qid,
            "naper": len(apcat),
            "file": output_filename,
        }

    return aper_stats


@click.command(context_settings={"show_default": True})
@click.argument("day")
@click.option(
    "-c",
    "--ccdid",
    required=True,
    type=click.IntRange(1, 16),
    help="ccdid in the range 1 to 16",
)
@click.option("--steps", default="bias,flat,sci,aper", help="steps to run")
@click.option("--statsdir", help="path where statistics are stored")
@click.option("--radius-min", type=int, default=3, help="minimum aperture radius")
@click.option("--radius-max", type=int, default=12, help="maximum aperture radius")
@click.option("--suffix", help="suffix for output science files")
@click.option("--pocket", is_flag=True, help="apply pocket correction?")
@click.option("--force", "-f", is_flag=True, help="force reprocessing all files?")
@click.option("--debug", "-d", is_flag=True, help="show debug info?")
@click.option("--pdb", is_flag=True, help="run pdb if an exception occurs")
def d2a(
    day,
    ccdid,
    steps,
    statsdir,
    radius_min,
    radius_max,
    suffix,
    pocket,
    force,
    debug,
    pdb,
):
    """Detrending to Aperture pipeline for a given day.

    \b
    Process DAY (must be specified in YYYY-MM-DD format):
    - computer master bias
    - computer master flat
    - for all science exposures, apply master bias and master flat, and run
      aperture photometry.

    """

    handler_opts: dict[str, Any] = dict(markup=False, rich_tracebacks=True)
    if debug:
        level = "DEBUG"
        handler_opts.update(dict(show_time=True, show_path=True))
        pdb = True
    else:
        level = "INFO"
        handler_opts.update(dict(show_time=True, show_path=False))

    if pdb:
        sys.excepthook = _run_pdb

    logging.basicConfig(
        level=level,
        format="%(message)s",
        datefmt="[%X]",
        handlers=[RichHandler(**handler_opts)],
    )

    day = day.replace("-", "")
    radius = np.arange(radius_min, radius_max)
    steps = steps.split(",")
    now = datetime.datetime.now(datetime.UTC)
    stats = {
        "date": now.isoformat(),
        "day": day,
        "ccd": ccdid,
        "ztfimg_version": ztfimg_version,
        "ztfin2p3_version": ztfin2p3_version,
    }
    tot = time.time()

    logger = logging.getLogger(__name__)
    logger.info("processing day %s, ccd=%s", day, ccdid)

    bi = BiasPipe(day, ccdid=ccdid, nskip=10)
    if len(bi.df) == 0:
        logger.warning(f"no bias for {day}")
        sys.exit(1)

    fi = FlatPipe(day, ccdid=ccdid, suffix=suffix)
    if len(fi.df) == 0:
        logger.warning(f"no flat for {day}")
        sys.exit(1)

    # Generate master bias:
    if "bias" in steps:
        t0 = time.time()
        bi.build_ccds(reprocess=force, **BIAS_PARAMS)
        timing = time.time() - t0
        logger.info("bias done, %.2f sec.", timing)
        stats["bias"] = {"time": timing}

    # Generate master flats:
    if "flat" in steps:
        t0 = time.time()
        fi.build_ccds(bias=bi, reprocess=force, corr_pocket=pocket, **FLAT_PARAMS)
        timing = time.time() - t0
        logger.info("flat done, %.2f sec.", timing)
        stats["flat"] = {"time": timing}

    stats["science"] = []
    n_errors = 0

    if set(steps) & {"sci", "aper"}:
        do_aper = "aper" in steps

        # Generate Science :
        # First browse meta data :
        rawsci_list = get_raw("science", fi.period, "metadata", ccdid=ccdid)
        rawsci_list.set_index(["day", "filtercode", "ccdid"], inplace=True)
        rawsci_list = rawsci_list.sort_index()
        bias = bi.get_ccd(day=day, ccdid=ccdid)

        # iterate over flat filters
        for _, row in fi.df.iterrows():
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
            flat = fi.get_ccd(day=day, ccdid=ccdid, filterid=row.filterid)

            for i, (_, sci_row) in enumerate(objects_files.iterrows(), start=1):
                raw_file = sci_row.filepath
                logger.info("processing sci %d/%d: %s", i, nfiles, raw_file)
                t0 = time.time()
                try:
                    aper_stats = process_sci(
                        raw_file, flat, bias, suffix, radius, pocket, do_aper=do_aper
                    )
                except Exception as exc:
                    if pdb:
                        raise

                    aper_stats = {}
                    status, error_msg = "error", str(exc)
                    n_errors += 1
                    timing = time.time() - t0
                    logger.error("sci done, status=%s, %.2f sec.", status, timing)
                    logger.error("error was: %s", error_msg)
                else:
                    status, error_msg = "ok", ""
                    timing = time.time() - t0
                    logger.info("sci done, status=%s, %.2f sec.", status, timing)

                sci_info["files"].append(
                    {
                        "file": raw_file,
                        "expid": sci_row.expid,
                        "time": timing,
                        "status": status,
                        "error_msg": error_msg,
                        **aper_stats,
                    }
                )

            stats["science"].append(sci_info)

    stats["total_time"] = time.time() - tot
    logger.info("all done, %.2f sec.", stats["total_time"])

    if statsdir is not None:
        statsdir = pathlib.Path(statsdir)
        stats_file = statsdir / f"stats_{day}_{ccdid}_{now:%Y%M%dT%H%M%S}.json"
        logger.info("writing stats to %s", stats_file)
        stats_file.write_text(json.dumps(stats))

    if n_errors > 0:
        logger.warning("%d sci files failed", n_errors)
