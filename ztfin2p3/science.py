""" Module to create the science files """

import os
import warnings
import numpy as np
from datetime import datetime

import dask

from astropy.io import fits

import ztfimg
from ztfquery.buildurl import get_scifile_of_filename
from . import __version__
from .io import ipacfilename_to_ztfin2p3filepath

def build_science_image(rawfile, flat, bias,
                            dask_level=None, 
                            as_path=False,
                            corr_nl=True,
                            corr_overscan=True,
                            overwrite=True):
    """ Top level method to build a science image.

    = dask not implemented yet =

    Parameter
    ---------
    rawfile: str
        filename or filepath of a raw image.

    flat, bias: str, ztfimg.CCD, array
        ccd data to calibrate the rawimage
        str: filepath
        ccd: ccd object containing the data
        array: numpy or dask array

    dask_level: None, "high", "low"
        should this use dask and how ?
        - None: dask not used.
        - shallow: delayed at the `from_filename` level
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

    overwrite: bool
        should this overwirte existing files ?

    Returns
    -------
    list
        results of fits.writeto (or delayed of that, see use_dask)
    """
    # Step 1. Load what is needed ---------------------- #
    # calibration file | flexible input
    # flat
    if type(flat) is str:
        print("flat from str")
        flat = ztfimg.CCD.from_filename(flat, as_path=True,
                                        use_dask=use_dask).get_data()    
    elif ztfimg.CCD in flat.__class__.__mro__:
        flat = flat.get_data()
    elif not "array" in str( type(flat) ): # numpy or dask
        raise ValueError(f"Cannot parse the input flat type ({type(flat)})")
    
    # bias
    if type(bias) is str:
        bias = ztfimg.CCD.from_filename(bias, as_path=True,
                                        use_dask=use_dask).get_data()    
    elif ztfimg.CCD in bias.__class__.__mro__:
        bias = bias.get_data()
    elif not "array" in str( type(flat) ): # numpy or dask
        raise ValueError(f"Cannot parse the input flat type ({type(flat)})")

    
    # at this stage, flat and bias are numpy or dask array
    # raw file to calibrated
    if dask_level is None:
        rawccd = ztfimg.RawCCD.from_filename(rawfile, as_path=True, use_dask=False)
        
    elif dask_level == "shallow":
        rawccd = dask.delayed(ztfimg.RawCCD.from_filename)(rawfile, 
                                                           as_path=True, 
                                                           use_dask=False)
    elif dask_level == "deep":
        rawccd = ztfimg.RawCCD.from_filename(rawfile, as_path=as_path, use_dask=True)
    else:
        raise ValueError(f"dask_level should be None, 'shallow' or 'deep', {dask_level} given")
    
    # -> Reference Science image | needed for header and filename
    ipac_filepaths = get_scifile_of_filename(rawfile, source="local")

    # Step 2. Create new data, header, filename -------- #
    # new science data
    calib_data = rawccd.get_data(corr_nl=corr_nl, corr_overscan=corr_overscan)
    if dask_level == "shallow": # calib_data is a 'delayed'.
        calib_data = dask.array.from_delayed(calib_data, dtype="float32", 
                                             shape=ztfimg.RawCCD.SHAPE)
        
    # calib_data = XXX # Pixel bias correction comes here
    calib_data -= bias # bias correction
    calib_data /= flat # flat correction

    # CCD object to accurately split the data.
    sciccd = ztfimg.CCD.from_data(calib_data) # dask.array if use_dask

    # individual quadrant's data | reoder = False  to get "natural" ztf-ordering
    new_data = sciccd.get_quadrantdata(from_data=True, reorder=False) # q1, q2, q3, q4
    
    # new headers
    new_headers = []
    for sciimg_ in ipac_filepaths:
        if dask_level is not None:
            header = dask.delayed(fits.getheader)(sciimg_)
        else:
            header = fits.getheader(sciimg_)
        new_headers.append(header_from_quadrantheader(header))
    
    # note that filenames are not delayed even if dasked.
    new_filenames = [ipacfilename_to_ztfin2p3filepath(f) for f in ipac_filepaths]

    # Step 3. Storing --------------------------------- #    
    outs = []
    for data_, header_, file_  in zip(new_data, new_headers, new_filenames):
        # make sure the directory exists.        
        os.makedirs( os.path.dirname(file_), mode=777, exist_ok=True)
        # writing data.
        if dask_level is not None:
            out = dask.delayed(fits.writeto)(file_, data_, header=header_, overwrite=overwrite)
        else:
            out = fits.writeto(file_, data_, header=header_, overwrite=overwrite)
            
        outs.append(out)
        
    return outs

def header_from_quadrantheader(header, skip=["CID", "CAL", "MAG", "CLRC", "ZP", "APCOR", 
                                                     "FIXAPERS", "NMATCHES", "BIT", "HISTORY",
                                                     "COMMENT"]):
    """ build the new header for a ztf-ipac pipeline science quadrant header

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
        a copy of the input header minus the skip plus some
        more information.
    """
    if "dask" in str( type(header) ):
        return dask.delayed(header_from_quadrantheader)(header, skip=skip)


    newheader = fits.Header()
    for k in header.keys() :
        if np.any([k.startswith(key_) for key_ in skip]):
            continue
            
        try:
            newheader.set(k, header[k], header.comments[k])
        except:
            warnings.warn(f"header transfert failed for {k}")
            
    newheader.set("PIPELINE", "ZTFIN2P3", "image processing pipeline")
    newheader.set("PIPEV", __version__, "ztfin2p3 pipeline version")
    newheader.set("ZTFIMGV", ztfimg.__version__, "ztfimg pipeline version")
    newheader.set("PIPETIME", datetime.now().isoformat(), "ztfin2p3 file creation")
    return newheader
