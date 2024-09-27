import logging
import sys
import time

import numpy as np
import pandas as pd
import rich_click as click
from ztfquery.buildurl import get_scifile_of_filename

from ztfin2p3.aperture import get_aperture_photometry, store_aperture_catalog
from ztfin2p3.io import ipacfilename_to_ztfin2p3filepath
from ztfin2p3.metadata import get_rawmeta
from ztfin2p3.pipe.newpipe import BiasPipe, FlatPipe
from ztfin2p3.science import build_science_image
from ztfin2p3.scripts.utils import _run_pdb, init_stats, save_stats, setup_logger

SCI_PARAMS = dict(
    fp_flatfield=True,
    overscan_prop=dict(userange=[25, 30]),
    return_sci_quads=True,
    store=False,
    with_mask=True,
)
APER_PARAMS = dict(
    cat="gaia_dr3",
    apply_proper_motion=True,
    as_path=False,
    minimal_columns=True,
    seplimit=20,
    bkgann=None,
    joined=True,
    refcat_radius=0.7,
)


def process_sci(rawfile, flat, bias, suffix, radius, corr_pocket, do_aper=True):
    logger = logging.getLogger(__name__)
    quads = build_science_image(
        rawfile,
        flat,
        bias,
        corr_pocket=corr_pocket,
        newfile_dict=dict(new_suffix=suffix),
        **SCI_PARAMS,
    )

    aper_stats = {}
    if not do_aper:
        return aper_stats

    ipac_filepaths = get_scifile_of_filename(rawfile, source="local")

    for i, (quad, out) in enumerate(zip(quads, ipac_filepaths), start=1):
        logger.info("aperture photometry %d (quadrant %s)", i, quad.qid)
        apcat, error, output_filename = [], "", ""
        if quad.mask is None:
            error = "no mask"
        elif quad.qid is None:
            error = "no sci header"
        else:
            apcat = get_aperture_photometry(quad, radius=radius, **APER_PARAMS)
            output_filename = ipacfilename_to_ztfin2p3filepath(
                out, new_suffix=suffix or "apcat", new_extension="parquet"
            )
            store_aperture_catalog(apcat, output_filename)

        if error:
            logger.error(error)
        aper_stats[f"quad_{i}"] = {
            "quad": quad.qid,
            "naper": len(apcat),
            "file": output_filename,
            "error": error,
        }

    return aper_stats


@click.command(context_settings={"show_default": True})
@click.argument("day")
@click.option("-c", "--ccdid", type=click.IntRange(1, 16), help="ccdid [1-16]")
@click.option("--aper", is_flag=True, help="compute aperture photometry?")
@click.option("--chunk-id", type=int, help="chunk id")
@click.option("--chunk-size", type=int, help="chunk size")
@click.option("--radius-min", type=int, default=3, help="minimum aperture radius")
@click.option("--radius-max", type=int, default=13, help="maximum aperture radius")
@click.option("--statsdir", help="path where statistics are stored")
@click.option("--suffix", help="suffix for output catalogs")
@click.option("--use-closest-calib", is_flag=True, help="use closest calib?")
@click.option("--force", "-f", is_flag=True, help="force reprocessing all files?")
@click.option("--debug", "-d", is_flag=True, help="show debug info?")
@click.option("--pdb", is_flag=True, help="run pdb if an exception occurs")
def d2a(
    day,
    ccdid,
    aper,
    chunk_id,
    chunk_size,
    radius_min,
    radius_max,
    statsdir,
    suffix,
    use_closest_calib,
    force,
    debug,
    pdb,
):
    """Detrending to Aperture pipeline for a given day.

    \b
    Process DAY (YYYY-MM-DD or YYYYMMDD):
    - by default for all CCDs (use --ccdid to process only one).
    - detrending: apply master bias and master flat either from the current day
      or finding the ones from the closest day if --use-closest-calib.
    - aperture photometry (if --aper).

    The list of files to process can be splitted in chunks with --chunk-id and
    --chunk--size.

    """

    setup_logger(debug=debug)
    if debug or pdb:
        sys.excepthook = _run_pdb

    tot = time.time()
    logger = logging.getLogger(__name__)

    n_errors = 0
    day = day.replace("-", "")
    radius = np.arange(radius_min, radius_max)
    stats = init_stats(day=day, ccd=ccdid, science=[])

    rawsci_list = get_rawmeta("science", day, ccdid=ccdid)
    nfiles = len(rawsci_list)
    logger.info("processing day %s, ccd=%s: %d files", day, ccdid, nfiles)

    if chunk_id is not None and chunk_size is not None:
        selection = slice(chunk_id * chunk_size, (chunk_id + 1) * chunk_size)
        rawsci_list = rawsci_list.iloc[selection]
        nfiles = len(rawsci_list)
        logger.info("processing chunk %d, %s, %d files", chunk_id, selection, nfiles)
        stats["chunk"] = [selection.start, selection.stop]

    stats["nfiles"] = nfiles

    # pocket effect correction after 20191022
    corr_pocket = pd.to_datetime(day) >= pd.to_datetime("20191022")
    stats["corr_pocket"] = corr_pocket

    if use_closest_calib:
        bi = fi = None
    else:
        bi = BiasPipe(day, ccdid=ccdid, nskip=10)
        if len(bi.df) == 0:
            raise Exception(f"no bias for {day}")

        fi = FlatPipe(day, ccdid=ccdid)
        if len(fi.df) == 0:
            raise Exception(f"no flat for {day}")

    for i, (_, row) in enumerate(rawsci_list.iterrows(), start=1):
        bias = bi.get_ccd(day=day, ccdid=row.ccdid) if bi is not None else None
        flat = (
            fi.get_ccd(day=day, ccdid=row.ccdid, filterid=row.filtercode)
            if fi is not None
            else None
        )
        raw_file = row.filepath

        msg = "processing sci %d/%d filter=%s ccd=%s pocket=%s: %s"
        logger.info(msg, i, nfiles, row.filtercode, row.ccdid, corr_pocket, raw_file)

        sci_info = {
            "day": day,
            "filter": row.filtercode,
            "ccd": row.ccdid,
            "file": raw_file,
            "expid": row.expid,
        }
        t0 = time.time()
        try:
            aper_stats = process_sci(
                raw_file,
                flat,
                bias,
                suffix,
                radius,
                corr_pocket=corr_pocket,
                do_aper=aper,
            )
        except Exception as exc:
            if pdb:
                raise

            aper_stats = {}
            status, error_msg = "error", str(exc)
            n_errors += 1
            logger.error("failed: %s", error_msg)
        else:
            status, error_msg = "ok", ""

        if aper and any(d["error"] for d in aper_stats.values()):
            n_errors += 1
            status, error_msg = "error", "error in aperture photometry"

        timing = time.time() - t0
        logger.info("sci done, status=%s, %.2f sec.", status, timing)
        sci_info.update({"time": timing, "status": status, "error_msg": error_msg})
        sci_info.update(aper_stats)
        stats["science"].append(sci_info)

    stats["total_time"] = time.time() - tot
    logger.info("all done, %.2f sec.", stats["total_time"])

    if statsdir is not None:
        save_stats(stats, statsdir, day, ccdid=ccdid)

    if n_errors > 0:
        logger.warning("%d sci files failed", n_errors)
