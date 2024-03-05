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


def build_science_exposure(rawfiles, flats, biases, dask_level="deep", **kwargs):
    """ Top level method to process multiple images.
    
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

def build_science_image(rawfile, flat, bias,
                            flat_coef=None,
                            dask_level=None, 
                            corr_nl=True,
                            corr_overscan=True,
                            overwrite=True, fp_flatfield=False, newfile_dict = {}, return_sci_quads = False ,
                            **kwargs):
    """ Top level method to build a single processed image.

    It calls:
    - to get the data: build_science_data()
    - to get the header: build_science_headers()
    - to store those: store_science_image()

    Parameter
    ---------
    rawfile: str
        filename or filepath of a raw image.

    flat, bias: str, ztfimg.CCD, array
        ccd data to calibrate the rawimage
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
    
    corr_overscan: bool
        Should the data be corrected for overscan
        (if both corr_overscan and corr_nl are true, 
        nl is applied first)

    corr_nl: bool
        Should data be corrected for non-linearity

    overwrite: bool
        should this overwirte existing files ?
        
    fp_flatfield : bool
        If True, applies a focal plane normalization to the flat. 
        Beware, if files are not available, it does not raise and error and use files on disk.
        
    newfile_dict : dict
        Kwargs for the ipacfilemant_to_ztfin2p3filepath to change easily filename and extensions. 
        
    return_sci_quads : bool
        If True, returns a list of ztfimg.ScienceQuadrants and filepaths. Otherwise, returns only filepaths
        
    **kwargs : 
        Arguments passed to the ztfimg.RawCCD.get_data of the raw object image.
        
    Returns
    -------
    list
        results of fits.writeto (or delayed of that, see use_dask)
    """
    # new of ipac sciimg.
    ipac_filepaths = get_scifile_of_filename(rawfile, source="local")
    new_filenames = [ipacfilename_to_ztfin2p3filepath(f, **newfile_dict) for f in ipac_filepaths]
    
    # Patch
    # Here avoid to do all the computation to not do the work in the end. 
    # Especially if all quadrants files are created.
    if not overwrite : 
        outs = new_filenames
        do =  sum([os.path.isfile(newfile) for newfile in new_filenames])
        if do == 4 : 
            if dask_level == 'shallow': 
                outs = dask.delayed(outs)  #Weird thing to keep consistent
        
            return outs
    
    if dask_level == "shallow": # dasking at the top level method
        new_data = dask.delayed(build_science_data)(rawfile, flat, bias,
                                                        dask_level=None,
                                                        corr_nl=corr_nl,
                                                        corr_overscan=corr_overscan, fp_flatfield=fp_flatfield,
                                                        **kwargs)
    
        new_header = dask.delayed(build_science_headers)(rawfile,
                                                         ipac_filepaths=ipac_filepaths,
                                                            use_dask=False)
    
        # note that filenames are not delayed even if dasked.
        outs = dask.delayed(store_science_image)(new_data, new_header, new_filenames,
                                                    use_dask=False)
    else:
        use_dask = dask_level is not None
        new_data = build_science_data(rawfile, flat, bias,
                                          dask_level=dask_level,
                                          corr_nl=corr_nl,
                                          corr_overscan=corr_overscan,fp_flatfield=fp_flatfield,  **kwargs)
    
        new_header = build_science_headers(rawfile,
                                            ipac_filepaths=ipac_filepaths,
                                            use_dask=use_dask)
    
        # note that filenames are not delayed even if dasked.
        outs = store_science_image(new_data, new_header, new_filenames,
                                   use_dask=use_dask)
    
    if return_sci_quads : 
        return [ztfimg.ScienceQuadrant.from_data(data=data, header=header) for data,header in zip(new_data, new_header)], outs
    else :
        return outs

# ------------- # 
#  mid-level    #
# ------------- #
def build_science_data(rawfile,
                      flat, bias,
                      dask_level=None, 
                      corr_nl=True,
                      corr_overscan=True,
                      as_path=True, fp_flatfield=False, **kwargs):
    """ build a single processed image data

    The function corrects for the sensor effects going from 
    raw to "science" images.
    
    Parameters
    ----------
    rawfile: str
        filename or filepath of a raw image.

    flat, bias: str, ztfimg.CCD, array
        ccd data to calibrate the rawimage
        str: filepath
        ccd: ccd object containing the data
        array: numpy or dask array

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

    Parameters
    ----------
    list
       list of the 2 quadrant data. 

    """
    use_dask = dask_level is not None
    # Generic I/O for flat and bias
    if type(flat) is str:        
        flat = ztfimg.CCD.from_filename(flat, as_path=True,
                                        use_dask=use_dask).get_data()    
    
            

    elif isinstance(flat, ztfimg.CCD):
        flat = flat.get_data()
            
    elif not "array" in str( type(flat) ): # numpy or dask
        raise ValueError(f"Cannot parse the input flat type ({type(flat)})")

    if flat_coef is not None:
        flat *= flat_coef 
    
    
    if fp_flatfield: 
        fp_flat_norm = get_fp_norm(flat.filepath)/flat.header['FLTNORM']

    # bias
    if type(bias) is str:
        bias = ztfimg.CCD.from_filename(bias, as_path=True,
                                        use_dask=use_dask).get_data()    
    elif isinstance(bias, ztfimg.CCD):
        bias = bias.get_data()
    elif not "array" in str( type(flat) ): # numpy or dask
        raise ValueError(f"Cannot parse the input flat type ({type(flat)})")

    # Create the new data
    if type(rawfile) is str:
        if dask_level is None:
            rawccd = ztfimg.RawCCD.from_filename(rawfile, as_path=True, use_dask=False)
        
        elif dask_level == "medium":
            rawccd = dask.delayed(ztfimg.RawCCD.from_filename)(rawfile, 
                                                                   as_path=True, 
                                                                   use_dask=False)
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
    calib_data -= bias # bias correction
    calib_data /= flat # flat correction
    if fp_flatfield : 
        calib_data *= fp_flat_norm
    
    # CCD object to accurately split the data.
    sciccd = ztfimg.CCD.from_data(calib_data) # dask.array if use_dask
    new_data = sciccd.get_quadrantdata(from_data=True, reorder=False) # q1, q2, q3, q4
    return new_data

def build_science_headers(rawfile, ipac_filepaths=None, use_dask=False):
    """ """
    if ipac_filepaths:
        ipac_filepaths = get_scifile_of_filename(rawfile, source="local")

    new_headers = []
    for sciimg_ in ipac_filepaths:
        if use_dask:
            header = dask.delayed(exception_header)(sciimg_)#dask.delayed(fits.getheader)(sciimg_)
            new_headers.append(header_from_quadrantheader(header))

        else:
            header = exception_header(sciimg_) #fits.getheader(sciimg_)
            new_headers.append(header)


    return new_headers

def exception_header(file_):
    try : 
        hdr=fits.getheader(file_)
        return hdr
    except Exception as e : 
        warnings.warn(str(e))
        return None

def store_science_image(new_data, new_headers, new_filenames,
                        use_dask=False,
                        overwrite=True, noneheader=False):
    """ store data in the input filename. 
    
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

    Returns
    -------
    list
        return of individual writeto.
    
    """
    outs = []
    for data_, header_, file_  in zip(new_data, new_headers, new_filenames):
        if header_ is None and not noneheader : 
            continue
        
        # make sure the directory exists.        
        os.makedirs( os.path.dirname(file_), exist_ok=True)
        # writing data.
        if use_dask:
            out = dask.delayed(_store_fits_)(filename=file_, data=data_, 
                                             header=header_, overwrite=overwrite)
        else:
            out = _store_fits_(filename=file_, data=data_, 
                               header=header_, overwrite=overwrite)

        outs.append(out)

    return outs

def _store_fits_(filename, data, header=None, overwrite=False, **kwargs):
    """ simple fits.writeto that returns the filename """
    fits.writeto(filename, data, header=header, overwrite=overwrite, **kwargs)
    return filename

    

# ------------- # 
#  low-level    #
# ------------- #
def header_from_quadrantheader(header, skip=["CID", "CAL", "CLRC", "APCOR", 
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

    if header is not None : 

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



def get_fp_norm(flat_file):
    try : 
        fp_flats_norms = fits.getval(flat_file, 'FLTNORM_FP')
    except KeyError: 
        fp_flats_norms = []
        for i in range(1,17) : 
            tmp = flat_file.split('_')
            tmp[-3] = f'c{i:02d}'
            flat = '_'.join(tmp)
            try :
                fp_flats_norms.append(ztfimg.CCD.from_filename(flat).get_data()*fits.getval(flat_file, 'FLTNORM'))
            except OSError as e:
                continue
        
        fp_flats_norms = np.median(fp_flats_norms)
        for i in range(1,17) : 
            tmp = flat_file.split('_')
            tmp[-3] = f'c{i:02d}'
            flat = '_'.join(tmp)
            try :
                fits.setval(flat, 'FLTNORM_FP', value=fp_flats_norms)
            except OSError as e:
                continue
                
    return fp_flats_norms
