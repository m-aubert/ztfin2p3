import argparse
import logging
import time

import numpy as np
from astropy.io import fits
from ztfimg import CCD
from ztfquery.buildurl import filename_to_url

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


def main():
    parser = argparse.ArgumentParser(
        description="ZTFIN2P3 pipeline : Detrending to Aperture."
    )
    parser.add_argument("day", help="Day to process in YYYY-MM-DD format")
    parser.add_argument(
        "-c",
        "--ccdid",
        required=True,
        type=int,
        choices=range(1, 17),
        help="ccdid in the range 1 to 16",
    )
    parser.add_argument(
        "--period", type=int, default=1, help="Number of days to process, 1 = daily"
    )
    args = parser.parse_args()

    logging.basicConfig(
        level="INFO",
        format="[%(levelname)s] %(asctime)s %(message)s",
        datefmt="%H:%M:%S",
        # handlers=[RichHandler(**handler_opts)],
    )

    ccdid = args.ccdid
    day = args.day  # YYYY-MM-D
    dt1d = np.timedelta64(args.period, "D")

    logger = logging.getLogger(__name__)
    logger.info("processing day %s, ccd %s", day, ccdid)
    logger.info("computing bias...")
    t0 = time.time()
    # Need to rework on the skipping method though.
    bi = BiasPipe.from_period(day, str(np.datetime64(day) + dt1d), ccdid=ccdid, skip=10)
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
    logger.info("bias done, %.2f sec.", time.time() - t0)
    logger.debug("\n".join(outs))

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
    logger.info("flat done, %.2f sec.", time.time() - t0)
    logger.debug("\n".join(outs))

    # Generate Science :
    # First browse meta data :
    rawsci_list = get_raw("science", fi.period, "metadata", ccdid=ccdid)
    rawsci_list.set_index(["day", "filtercode", "ccdid"], inplace=True)
    rawsci_list = rawsci_list.sort_index()

    flat_datalist = daily_datalist(fi)  # Will iterate over flat filters
    bias = bi.get_daily_ccd(day="".join(day.split("-")), ccdid=ccdid)[
        "".join(day.split("-")), ccdid
    ]

    newfile_dict = dict(new_suffix="ztfin2p3-testE2E")

    for i, row in flat_datalist.iterrows():
        objects_files = rawsci_list.loc[
            row.day, row.filterid, row.ccdid
        ].filepath.values

        flat = CCD.from_data(fi.daily_filter_ccds[row["index"]])
        logger.info(
            "processing %s filter=%s ccd=%s: %d files",
            row.day,
            row.filterid,
            row.ccdid,
            len(objects_files),
        )

        for raw_file in objects_files:
            logger.info("processing sci %s", raw_file)
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
            logger.info("sci done, %.2f sec.", time.time() - t0)

            # If quadrant level :
            for quad, out in zip(quads, outs):
                # Not using build_aperture_photometry cause it expects
                # filepath and not images. Will change.
                logger.info("aperture photometry for quadrant %d", quad.qid)
                t0 = time.time()
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
                logger.info(
                    "aperture done, quad %d, %.2f sec.", quad.qid, time.time() - t0
                )
                logger.debug(out)


if __name__ == "__main__":
    main()
