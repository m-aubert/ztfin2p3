""" Module to create the science files """

import os
import warnings
import numpy as np

from astropy.io import fits

import ztfimg
from . import __version__

def build_science_image(rawfile, flatfile, biasfile,
                            as_path=False,
                            corr_nl=True, corr_overscan=True,
                            overwrite=True):
    """ Top level method to build a science image.

    = dask not implemented yet =

    Parameter
    ---------
    rawfile: str
        filename or filepath of a raw image.

    flatfile: path
       fullpath to the ztfin2p3 pipeline ccd flatfile

    biasfile: path
        fullpath to the ztfin2p3 pipeline bias flatfile    
    
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
    None
        this writes file.

    """
    # Step 1. Load what is needed ---------------------- #
    # ZTFIN2P3 calibration file.
    flat = ztfimg.CCD.from_filename(flatfile, as_path=True)
    bias = ztfimg.CCD.from_filename(biasfile, as_path=True)
    # -> raw file to calibrated
    rawccd = ztfimg.RawCCD.from_filename(rawfile, as_path=as_path)
    
    # -> Reference Science image | needed header
    ipac_ccd = rawccd.get_sciimage(as_ccd=True)

    # Step 2. Create new data, header, filename -------- #
    # new science data
    calib_data = rawccd.get_data(corr_nl=corr_nl, corr_overscan=corr_overscan)
    # ====== #
    # Pixel bias correction comes here
    # ====== #
    calib_data -= bias.get_data() # bias correction
    calib_data /= flat.get_data() # flat correction

    # CCD object to accurately split the data.
    sciccd = ztfimg.CCD.from_data(calib_data)
    # individual quadrant's data | reoder = False  to get "natural" ztf-ordering
    new_data = sciccd.get_quadrantdata(from_data=True, reorder=False) # q1, q2, q3, q4
    # new headers
    new_headers = [header_from_quadrantheader(ipac_ccd.get_quadrant(i).get_header())
                       for i in range(1,5)]
    # new filename. Replace:
    # dirpath: /sci/bla -> /ztfin2p3/sci/bla 
    # basename: ztf_* -> ztfin2p3_*
    new_filenames = [f.replace("/sci","/ztfin2p3/sci").replace("/ztf_","/ztfin2p3_") for f in ipac_ccd.filepaths]

    # Step 3. Storing --------------------------------- #    
    for data_, header_, file_  in zip(new_data, new_headers, new_filenames):
        # make sure the directory exists.        
        os.makedirs( os.path.dirname(file_), mode=771, exist_ok=True)
        # writing data.
        fits.writeto(file_, data_, header=header_,
                         overwrite=overwrite)

    
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
