import argparse

import numpy as np

from ztfin2p3.aperture import get_aperture_photometry, store_aperture_catalog
from ztfin2p3.io import ipacfilename_to_ztfin2p3filepath
from ztfin2p3.metadata import get_raw
from ztfin2p3.pipe import BiasPipe, FlatPipe
from ztfin2p3.science import build_science_image


def daily_datalist(cls):
    # Will probably be implemented in CalibPipe class. Will be cleaner
    datalist = cls.init_datafile.copy()
    datalist["filterid"] = dstore_aperture_cataloglist["ledid"]
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
    parser.add_argument(
        "--day",
        type=str,
        help="Day to process in YYYY-MM-DD format",
    )
    parser.add_argument(
        "--ccdid",
        nargs="+",
        type=int,
        help="ccdid or list of ccdid in the range 1 to 16",
    )
    parser.add_argument(
        "--period",
        type=int,
        help="Length of period. Int for daily period. 1 = daily. 2 equal every two days",
    )
    args = parser.parse_args()

    ccdid = args.ccdid
    if min(ccdid) < 1 or max(ccdid) > 16:
        raise ValueError("ccdid must be between 1 and 16")

    day = args.day  # YYYY-MM-D
    dt1d = np.timedelta64(args.period, "D")

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

    # Generate flats :
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

    # Generate Science :
    # First browse meta data :
    rawsci_list = get_raw("science", fi.period, "metadata", ccdid=ccdid)
    rawsci_list.set_index(["day", "filtercode", "ccdid"], inplace=True)

    flat_datalist = daily_datalist(fi)  # Will iterate over flat filters
    bias = bi.get_daily_ccd(day="".join(day.split("-")), ccdid=ccdid)[
        "".join(day.split("-")), ccdid
    ]

    newfile_dict = dict(new_suffix="ztfin2p3-testE2E")

    for i, row in flat_datalist.iterrows():
        objects_files = rawsci_list.loc[
            row.day, row.filterid, row.ccdid
        ].filepath.values

        for raw_file in objects_files:
            quads, outs = build_science_image(
                raw_file,
                fi.daily_filter_ccds[row["index"]],
                bias,
                dask_level=None,
                corr_nl=True,
                corr_overscan=True,
                overwrite=True,
                fp_flatfield=False,
                newfile_dict=dict(new_suffix="ztfin2p3-testE2E"),
                return_sci_quadrants=True,
                overscan_prop=dict(userange=[25, 30]),
            )

            # If quadrant level :
            for quad, out in (quads, outs):
                # Not using build_aperture_photometry cause it expects
                # filepath and not images. Will change.
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


if __name__ == "__main__":
    main()
