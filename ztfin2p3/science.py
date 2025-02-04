""" Module to create the science files """

import logging
import os
import pathlib
from datetime import datetime

import dask
import numpy as np
import pandas as pd
import ztfimg
from astropy.io import fits
from ztfquery.buildurl import filename_to_url, get_scifile_of_filename, parse_filename

from . import __version__
from .io import (
    CAL_DIR,
    get_daily_biasfile,
    get_daily_flatfile,
    ipacfilename_to_ztfin2p3filepath,
)
from .metadata import get_sciheader


def build_science_exposure(rawfiles, flats, biases, dask_level="deep", **kwargs):
    """Top level method to process multiple images.

    Parameters
    ----------
    rawfiles: str
        filenames or filepaths of the ccd images a raw image.

    flats, biases: list
        list of str, ztfimg.CCD or array.
        This list size must match that of rawfiles for they
        are `zip` together.
        These are the ccd data to calibrate the rawimage
        str: filepath
        ccd: ccd object containing the data
        array: numpy or dask array

    dask_level: None, "shallow", "medium", "deep"
        should this use dask and how ?
        - None: dask not used.
        - shallow: delayed at the `get_science_data` (and co) level
        - medium: delayed at the `from_filename` level
        - deep: dasked at the array level (native ztimg)
        note:
        - deep has extensive tasks but handle the memory
        at its minimum ; it is faster to compute a few targets.
        - shallow is faster when processing many files.
        It takes slightly more memory  but maintain the overhead at its minimum.

    **kwargs goes to build_science_image

    Returns
    -------
    list
        list of each build_science_image call's return.
    """
    outs = []
    for raw_, bias_, flat_ in zip(rawfiles, biases, flats):
        delayed_or_not = build_science_image(raw_, bias=bias_, flat=flat_, dask_level=dask_level, **kwargs)
        _ = [outs.append(d_) for d_ in delayed_or_not]

    return outs


def build_science_image(
    rawfile,
    flat=None,
    bias=None,
    *,
    corr_nl=True,
    corr_overscan=True,
    corr_pocket=False,
    dask_level=None,
    flat_coef=None,
    fp_flatfield=False,
    max_timedelta="1w",
    newfile_dict={},
    return_sci_quads=False,
    store=True,
    outpath=None,
    overwrite=True,
    with_mask=False,
    corr_fringes=False,
    **kwargs,
):
    """Top level method to build a single processed image.

    It calls:
    - to get the data: build_science_data()
    - to get the header: build_science_headers()
    - to store those: store_science_image()

    Parameter
    ---------
    rawfile: str
        Filename or filepath of a raw image.

    flat, bias: str, ztfimg.CCD, array, optional
        ccd data to calibrate the rawimage
        str: filepath
        ccd: ccd object containing the data
        array: numpy or dask array
        If not specified, try using the master bias/flat metadata files to
        find the closest master bias/flat that is already processed.

    dask_level: None, "shallow", "medium", "deep"
        Should this use dask and how ?
        - None: dask not used.
        - shallow: delayed at the `get_science_data` (and co) level
        - medium: delayed at the `from_filename` level
        - deep: dasked at the array level (native ztimg)
        note:
        - deep has extensive tasks but handle the memory
        at its minimum ; it is faster to compute a few targets.
        - shallow is faster when processing many files.
        It takes slightly more memory  but maintain the overhead at its minimum.

    corr_overscan: bool
        Should the data be corrected for overscan
        (if both corr_overscan and corr_nl are true,
        nl is applied first)

    corr_nl: bool
        Should data be corrected for non-linearity

    fp_flatfield : bool
        If True, applies a focal plane normalization to the flat.
        Beware, if files are not available, it does not raise and error and use
        files on disk.

    max_timedelta : str
        When finding master bias/flat from already processed ones, specifies
        the maximum time delta to consider the file as valid. Use pandas
        formats, e.g. "1d", "1w" etc.

    newfile_dict : dict
        Kwargs for the ipacfilename_to_ztfin2p3filepath to change easily
        filename and extensions.

    return_sci_quads : bool
        If True, returns a list of ztfimg.ScienceQuadrants and filepaths.
        Otherwise, returns only filepaths

    store : bool
        Should this store produced files ?

    outpath : str

    overwrite : bool
        Should this overwrite existing files ? If False and files exists,
        those will be returned without any processing.

    with_mask : bool
        Read mask file and add it to the ScienceQuadrant object ?

    corr_fringes : bool
        Correct atmospheric fringes for i-band only.

    **kwargs :
        Arguments passed to the ztfimg.RawCCD.get_data of the raw object image.

    Returns
    -------
    list
        results of fits.writeto (or delayed of that, see use_dask)
    """

    info = parse_filename(rawfile)

    if info["kind"] != "raw":
        raise ValueError("file is not a raw file")

    ccdid, filtername = info["ccdid"], info["filtercode"]
    year, month, day = info["year"], info["month"], info["day"]
    date = pd.to_datetime(f"{year}{month}{day}")

    if corr_fringes and filtername != 'zi':
        corr_fringes = False

    if corr_fringes and ccdid == 1 : 
        w_ = "Reverting to no correction for CCDID 1" 
        logging.getLogger(__name__).warn(w_)
        corr_fringes = False

    if bias is None:
        biasfile = find_closest_calib_file(
            year, date, ccdid, kind="bias", max_timedelta=max_timedelta
        )
        bias = ztfimg.CCD.from_filename(biasfile)

    if flat is None:
        flatfile = find_closest_calib_file(
            year,
            date,
            ccdid,
            filtername=filtername,
            kind="flat",
            max_timedelta=max_timedelta,
        )
        flat = ztfimg.CCD.from_filename(flatfile)

    # new of ipac sciimg.
    ipac_filepaths = get_scifile_of_filename(rawfile, source="local")
    new_filenames = [
        ipacfilename_to_ztfin2p3filepath(f, **newfile_dict) for f in ipac_filepaths
    ]
    if outpath is not None:
        new_filenames = [
            os.path.join(outpath, os.path.basename(f)) for f in new_filenames
        ]

    # Here avoid to do all the computation to not do the work in the end.
    # Especially if all quadrants files are created.
    if not overwrite and all(os.path.isfile(newfile) for newfile in new_filenames):
        if dask_level == "shallow":
            # Weird thing to keep consistent
            new_filenames = dask.delayed(new_filenames)
        return new_filenames

    if dask_level == "shallow":  # dasking at the top level method
        dask_level = None
        use_dask = False
        maybe_delayed = dask.delayed
    else:
        use_dask = dask_level is not None
        maybe_delayed = identity

    new_data, biasfile, flatfile = maybe_delayed(build_science_data)(
        rawfile,
        flat,
        bias,
        flat_coef=flat_coef,
        dask_level=dask_level,
        corr_nl=corr_nl,
        corr_overscan=corr_overscan,
        corr_pocket=corr_pocket,
        fp_flatfield=fp_flatfield,
        **kwargs,
    )

    new_header = maybe_delayed(build_science_headers)(
        rawfile,
        ipac_filepaths,
        use_dask=use_dask,
        BIASFILE=biasfile,
        FLATFILE=flatfile,
    )

    if store:
        # note that filenames are not delayed even if dasked.
        maybe_delayed(store_science_image)(
            new_data, new_header, new_filenames, use_dask=use_dask
        )

    if return_sci_quads:
        quads = []

        for data, header, fname in zip(new_data, new_header, new_filenames):
            quad = ztfimg.ScienceQuadrant(data=data, header=header)
            if with_mask:
                quad.set_mask(get_mskdata(fname))

            if corr_fringes : 
                from .utils import correct_fringes_zi
                 # Need custom fringez package. For now optional.
                 # In the future corr_fringes will default to True.
                corr_data = correct_fringes_zi(quad.data, 
                                                mask_data=quad.mask, 
                                                image_path=fname, 
                                                return_img_only=True)[0]

                quad.set_data(corr_data) #Overwrite with data.
                # Could save model and PCA components if needed.

            quads.append(quad)
        return quads
    elif store:
        return new_filenames
    else:
        raise ValueError(
            "either store=True or return_sci_quads=True should be specified"
        )


# ------------- #
#  mid-level    #
# ------------- #
def build_science_data(
    rawfile,
    flat,
    bias,
    flat_coef=None,
    dask_level=None,
    corr_nl=True,
    corr_overscan=True,
    as_path=True,
    fp_flatfield=False,
    **kwargs,
):
    """build a single processed image data

    The function corrects for the sensor effects going from
    raw to "science" images.

    Parameters
    ----------
    rawfile: str
        filename or filepath of a raw image.

    flat, bias: str, ztfimg.CCD
        ccd data to calibrate the rawimage
        str: filepath
        ccd: ccd object containing the data

    dask_level: None, "medium", "deep"
        should this use dask and how ?
        - None: dask not used.
        - medium: delayed at the `from_filename` level
        - deep: dasked at the array level (native ztimg)
        note:
        - deep has extensive tasks but handle the memory
        at its minimum ; it is faster to compute a few targets.
        - shallow is faster when processing many files.
        It takes slightly more memory  but maintain the overhead at its minimum.

    corr_overscan: bool
        Should the data be corrected for overscan
        (if both corr_overscan and corr_nl are true,
        nl is applied first)

    corr_nl: bool
        Should data be corrected for non-linearity

    flat_coef: float
        if given, this will multiply to the flat
        flatused = flat*flatcoef

    Returns
    ----------
    list
        Quadrant data list
    str 
        Master bias filepath
    str
       Master flat filepath

    """
    use_dask = dask_level is not None
    flatfile = biasfile = None

    # Generic I/O for flat and bias
    if isinstance(flat, str):
        flat = ztfimg.CCD.from_filename(flat, as_path=True, use_dask=use_dask)

    if isinstance(flat, ztfimg.CCD):
        flatfile = flat.filepath
        flat_data = flat.get_data()
    else:  # numpy or dask
        raise ValueError(f"Cannot parse the input flat type ({type(flat)})")

    if flat_coef is not None:
        flat_data *= flat_coef

    if fp_flatfield:
        fp_flat_norm = flat.header['HIERARCH FLTNORM_FP'] / flat.header['FLTNORM']

    # bias
    if isinstance(bias, str):
        biasfile = bias
        bias = ztfimg.CCD.from_filename(bias, as_path=True,
                                        use_dask=use_dask).get_data()
    elif isinstance(bias, ztfimg.CCD):
        biasfile = bias.filepath
        bias = bias.get_data()
    elif not is_array(bias):  # numpy or dask
        raise ValueError(f"Cannot parse the input flat type ({type(flat)})")

    # Create the new data
    if isinstance(rawfile, str):
        if dask_level is None:
            rawccd = ztfimg.RawCCD.from_filename(rawfile, as_path=True, use_dask=False)
        elif dask_level == "medium":
            rawccd = dask.delayed(ztfimg.RawCCD.from_filename)(
                rawfile, as_path=True, use_dask=False
            )
        elif dask_level == "deep":
            rawccd = ztfimg.RawCCD.from_filename(rawfile, as_path=as_path, use_dask=True)
        else:
            raise ValueError(f"dask_level should be None, 'medium' or 'deep', {dask_level} given")
    else:
        rawccd = rawfile

    # Step 2. Create new data, header, filename -------- #
    # new science data
    calib_data = rawccd.get_data(corr_nl=corr_nl, corr_overscan=corr_overscan, **kwargs)
    if dask_level == "medium": # calib_data is a 'delayed'.
        calib_data = dask.array.from_delayed(calib_data, dtype="float32",
                                             shape=ztfimg.RawCCD.SHAPE)

    # calib_data = XXX # Pixel bias correction comes here
    calib_data -= bias  # bias correction
    calib_data /= flat_data  # flat correction
    if fp_flatfield:
        calib_data *= fp_flat_norm

    # CCD object to accurately split the data.
    sciccd = ztfimg.CCD.from_data(calib_data) # dask.array if use_dask
    new_data = sciccd.get_quadrantdata(from_data=True, reorder=False) # q1, q2, q3, q4
    return new_data, biasfile, flatfile


def build_science_headers(rawfile, ipac_filepaths, use_dask=False, **kwargs):
    maybe_delayed = dask.delayed if use_dask else identity
    new_headers = []
    rawhdr = fits.getheader(rawfile)
    rawhdr.strip()

    for sciimg_ in ipac_filepaths:
        header = rawhdr.copy()
        scihdr = maybe_delayed(exception_header)(sciimg_)
        if scihdr is not None:
            header.update(scihdr)
        header = maybe_delayed(header_from_quadrantheader)(header)
        for key, val in kwargs.items():
            header[key] = val
        new_headers.append(header)
    return new_headers


def exception_header(file_):
    hdr = get_sciheader(file_)
    if hdr is None:
        try:
            hdr = fits.getheader(file_)
            hdr.strip()
        except Exception as e:
            logging.getLogger(__name__).warn("%s", e)
    return hdr


def store_science_image(
    new_data,
    new_headers,
    new_filenames,
    use_dask=False,
    overwrite=True,
    noneheader=False,
):
    """store data in the input filename.

    this method handles dask.

    Parameters
    ----------
    new_data: list
        list of 2d-array (quadrant format) | numpy or dask
    new_header: list
        list of header (or delayed)
    new_filenames: list
        list of full path where the data shall be stored.
    use_dask: bool
        shall this use dask while storing.
        careful if this is false while data are dask.array
        this will compute them.
    noneheader: bool

    Returns
    -------
    list of str
    """
    maybe_delayed = dask.delayed if use_dask else identity
    for data, header, filepath in zip(new_data, new_headers, new_filenames):
        if header is None and not noneheader:
            continue
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        maybe_delayed(fits.writeto)(filepath, data, header=header, overwrite=overwrite)


# ------------- #
#  low-level    #
# ------------- #
def header_from_quadrantheader(
    header,
    skip=(
        "CID",
        "CAL",
        "CLRC",
        "APCOR",
        "FIXAPERS",
        "NMATCHES",
        "BIT",
        "HISTORY",
        "COMMENT",
        "CHECKSUM",
        "DATASUM",
    ),
):
    """build the new header for a ztf-ipac pipeline science quadrant header

    Parameters
    ----------
    header: fits.Header
        science quadrant header from IPAC's pipeline
    skip: list
        list of header keywords. Any keywork starting by this will
        be ignored

    Returns
    -------
    fits.Header
        a copy of the input header minus the skip plus some more information.
    """
    newheader = fits.Header()

    if header is not None:
        for k in header.keys() :
            if any(k.startswith(key_) for key_ in skip):
                continue
            newheader.set(k, header[k], header.comments[k])

    newheader.set("PIPELINE", "ZTFIN2P3", "image processing pipeline")
    newheader.set("PIPEV", __version__, "ztfin2p3 pipeline version")
    newheader.set("ZTFIMGV", ztfimg.__version__, "ztfimg pipeline version")
    newheader.set("PIPETIME", datetime.now().isoformat(), "ztfin2p3 file creation")
    return newheader


def compute_fp_norm(flat_files):
    fp_flats_norms = []
    for filepath in flat_files:
        ccd = ztfimg.CCD.from_filename(filepath)
        fp_flats_norms.append(ccd.get_data() * ccd.header["FLTNORM"])

    fp_flats_norms = np.median(fp_flats_norms)

    for filepath in flat_files:
        fits.setval(filepath, "HIERARCH FLTNORM_FP", value=fp_flats_norms)

    return fp_flats_norms


def find_closest_calib_file(
    year,
    date,
    ccdid,
    filtername=None,
    kind="bias",
    max_timedelta="1w",
):
    """Use master bias/flat catalogs to find the closest one in time."""

    logger = logging.getLogger(__name__)
    CAL = pathlib.Path(CAL_DIR)
    meta_file = CAL / kind / "meta" / f"master{kind}_metadata_{year}.parquet"
    if not meta_file.exists():
        raise ValueError(f"could not find master{kind} file ({meta_file})")

    df = pd.read_parquet(meta_file)
    date = pd.to_datetime(date)

    df["date"] = pd.to_datetime(df.PERIOD)
    df = df.set_index("date")
    df = df[df.CCDID.eq(ccdid)]
    if kind == "flat":
        df = df[df.FILTRKEY.eq(filtername)]

    # drop duplicates if multiple files with different suffixes
    # TODO: find way to identify those variant and filter on them ?
    df.drop_duplicates('PERIOD', inplace=True)
    idx = df.index.get_indexer([date], method="nearest")

    td = abs(df.index[idx[0]] - date)
    if td > pd.Timedelta(max_timedelta):
        raise ValueError(f"found {kind} but time delta is greater than required ({td})")

    item = df.iloc[idx[0]]
    if kind == "bias":
        filepath = get_daily_biasfile(item.PERIOD, ccdid)
        logger.debug("found master bias: %s", filepath)
        return filepath
    elif kind == "flat":
        filepath = get_daily_flatfile(item.PERIOD, ccdid, filtername=filtername)
        logger.debug("found master flat: %s", filepath)
        return filepath


def get_mskdata(filename):
    mskdata = exception_mask(filename,"mskimg.fits.gz")
    if mskdata is None : 
        mskdata = exception_mask(filename,"mskimg.fits")
    
    return mskdata

def exception_mask(filename, suffix): 
    fname_mask = filename_to_url(filename, suffix=suffix, source="local")
    mskdata = None
    try : 
        mskdata = fits.getdata(fname_mask)   

    except Exception as e:
        logging.getLogger(__name__).warn("%s", e)

    return mskdata

def is_array(x):
    """Test if variable is a Numpy or Dask array."""
    return isinstance(x, (np.ndarray, dask.array.Array))


def identity(x):
    """Noop function for easier dask handling"""
    return x
