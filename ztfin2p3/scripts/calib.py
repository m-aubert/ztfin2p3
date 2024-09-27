import logging
import sys
import time

import rich_click as click

from ztfin2p3.pipe.newpipe import BiasPipe, FlatPipe
from ztfin2p3.science import compute_fp_norm
from ztfin2p3.scripts.utils import _run_pdb, init_stats, save_stats, setup_logger

CLIPPING_PROP = dict(
    maxiters=1, cenfunc="median", stdfunc="std", masked=False, copy=False
)
BIAS_PARAMS = dict(
    sigma_clip=3,
    mergedhow="nanmean",
    clipping_prop=CLIPPING_PROP,
    get_data_props=dict(overscan_prop=dict(userange=[25, 30])),
)
FLAT_PARAMS = dict(
    corr_pocket=False,
    sigma_clip=3,
    mergedhow="nanmean",
    clipping_prop=CLIPPING_PROP,
    get_data_props=dict(overscan_prop=dict(userange=[25, 30])),
)


@click.command(context_settings={"show_default": True})
@click.argument("day")
@click.option("--statsdir", help="path where statistics are stored")
@click.option("--suffix", help="suffix for output science files")
@click.option("--force", "-f", is_flag=True, help="force reprocessing all files?")
@click.option("--debug", "-d", is_flag=True, help="show debug info?")
@click.option("--pdb", is_flag=True, help="run pdb if an exception occurs")
def calib(
    day,
    statsdir,
    suffix,
    force,
    debug,
    pdb,
):
    """Calibration pipeline for a given day.

    \b
    Process DAY (must be specified in YYYY-MM-DD format):
    - computer master bias
    - computer master flat

    """

    setup_logger(debug=debug)
    if debug or pdb:
        sys.excepthook = _run_pdb

    n_errors = 0
    day = day.replace("-", "")
    tot = time.time()
    logger = logging.getLogger(__name__)

    stats = init_stats(day=day)
    stats["bias"] = []
    stats["flat"] = []

    for ccdid in range(1, 17):
        logger.info("processing day %s, ccd=%s", day, ccdid)

        try:
            bi = BiasPipe(day, ccdid=ccdid, nskip=10)
            if len(bi.df) == 0:
                logger.warning(f"no bias for {day}")
                n_errors += 1
                continue

            # Generate master bias:
            t0 = time.time()
            bi.build_ccds(reprocess=force, **BIAS_PARAMS)
            timing = time.time() - t0
            logger.info("bias done, %.2f sec.", timing)
            stats["bias"].append({"ccd": ccdid, "time": timing})

            fi = FlatPipe(day, ccdid=ccdid, suffix=suffix)
            if len(fi.df) == 0:
                logger.warning(f"no flat for {day}")
                n_errors += 1
                continue

            # Generate master flats:
            t0 = time.time()
            fi.build_ccds(bias=bi, reprocess=force, **FLAT_PARAMS)
            timing = time.time() - t0
            logger.info("flat done, %.2f sec.", timing)
            stats["flat"].append({"ccd": ccdid, "time": timing})
        except Exception as e:
            logger.error("failed: %s", e)
            n_errors += 1

    if n_errors == 0:
        logger.info("compute flat fp norm")
        stats["flat norm"] = {}
        fi = FlatPipe(day, suffix=suffix)
        for filterid, df in fi.df.groupby("filterid"):
            logger.debug("filter=%s, %d flats", filterid, len(df))
            assert len(df) == 16
            stats["flat norm"][filterid] = float(compute_fp_norm(df.fileout.tolist()))

    stats["total_time"] = time.time() - tot
    logger.info("all done, %.2f sec.", stats["total_time"])

    if statsdir is not None:
        save_stats(stats, statsdir, day)

    if n_errors > 0:
        logger.error("%d errors", n_errors)
