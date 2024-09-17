""" Module to run the aperture photometry """

import os
import numpy as np
import ztfimg
from ztfimg.catalog import get_isolated
from .catalog import get_img_refcatalog
from .io import ipacfilename_to_ztfin2p3filepath
import warnings
import pandas #pandas was not defined in get_aperture
import dask #dask was not defined in get_aperture

_MINIMAL_COLNAMES = {"gaia_dr2": ['id','ra', 'dec',
                                  'phot_g_mean_mag', 'phot_bp_mean_mag', 'phot_rp_mean_mag',
                                  'x', 'y'], 
                    "gaia_dr3": ['source_id','ra', 'dec',
                                  'phot_g_mean_mag', 'phot_bp_mean_mag', 'phot_rp_mean_mag',
                                  'x', 'y']} # index and isolated comes after


def bulk_aperture_photometry(filenames, cat="gaia_dr2",
                                dask_level="shallow",
                                radius=np.linspace(2,10,9), bkgann=[10,11],
                                as_path=True, **kwargs):
    """ run aperture photometry on science images

    
    """
    filenames = np.atleast_1d(filenames) # make

    prop = {**dict(cat=cat,
                    dask_level=dask_level,
                    radius=radius, bkgann=bkgann,
                    as_path=as_path), **kwargs}
        
    # bulk running apeture photometry
    delayed_or_not = [build_aperture_photometry(str(filename), **prop) for filename in filenames]
    return delayed_or_not


def build_aperture_photometry(filename, cat="gaia_dr2",
                                dask_level=None,
                                radius=np.linspace(2,10,9), bkgann=[10,11],
                                as_path=True, new_suffix="apcat", **kwargs):
    """ run and store aperture photometry on a signle science image

    Parameters
    ----------
    filename: str
        name or path (see as_patjh) of a science image to run the aperture photometry on.
    
    cat: str, DataFrame
       catalog to use for the aperture photometry.
        - str: name of a catalog accessible from get_img_refcatalog
        - DataFrame:  actual catalog that contains ra, dec information.
            could be pandas or dask
    
    dask_level: None, str
        = ignored if sciimg is not a str =
        should this use dask and how ?
        - None: dask not used.
        - shallow: delayed at the top level (get_aperture_photometry etc)
        - medium: delayed at the `from_filename` level | careful: could be unsable
        - deep: dasked at the array level (native ztimg)
 
    radius: float, array
        aperture photometry radius (could be 1d-list).
        In unit of pixels. 
        To broadcast, this has [:,None] applied to internally.
        
    bkgann: list
        properties of the annulus [min, max] (in unit of pixels).
        This should broadcast with radius the broadcasted radius.

    as_path: bool
        = ignored if sciimg is not a str =
        Set to True if the filename are path and not just ztf filename.

    **kwargs goes to get_aperture_photometry (seplimit, minimal_columns ...)

    Returns
    -------
    delayed or None
        output of store_aperture_catalog.
    """
    output_filename = ipacfilename_to_ztfin2p3filepath(filename,
                                                       new_suffix=new_suffix,
                                                       new_extension="parquet")

    prop_apcat = {**dict(cat=cat,
                            radius=radius, bkgann=bkgann,
                             as_path=as_path),
                  **kwargs}
    
    if dask_level == "shallow": # dasking at the top level method
        import dask
        # build the catalog
        apcat = dask.delayed(get_aperture_photometry)(filename, dask_level=None, **prop_apcat)
        # and store it
        out = dask.delayed(store_aperture_catalog)(apcat, output_filename)
    else:
        apcat = get_aperture_photometry(filename, dask_level=dask_level, **prop_apcat)
        out = store_aperture_catalog(apcat, output_filename)
        
    return out
    
            
# ------------- # 
#  mid-level    #
# ------------- #    
def get_aperture_photometry(sciimg, cat="gaia_dr2", 
                                dask_level=None, as_path=True,
                                minimal_columns=True,
                                seplimit=20,
                                radius=np.linspace(2,10,9),
                                bkgann=[10,11], 
                                joined=True,
                                refcat_radius=0.7,
                                apply_proper_motion=False):
    """ run  aperture photometry on science image given input catalog.
    
    Parameters
    ----------
    sciimg: str, ztfimg.ScienceQuadrant
        science image to run the aperture photometry on.
        - str: filename of the science image (see as_path)
        - ScienceQuadrant: actual ztfimg object.
    
    cat: str, DataFrame
       catalog to use for the aperture photometry.
        - str: name of a catalog accessible from get_img_refcatalog
        - DataFrame:  actual catalog that contains ra, dec information.
            could be pandas or dask
    
    dask_level: None, str
        = ignored if sciimg is not a str =
        should this use dask and how ?
        - None: dask not used.
        - medium: delayed at the `from_filename` level
        - deep: dasked at the array level (native ztimg)        
        ** WARNING dask_level='medium' & joined=True may failed due to serialization issues **

    as_path: bool
        = ignored if sciimg is not a str =
        Set to True if the filename are path and not just ztf filename.

    minimal_columns: bool
        = ignored if cat is not a str =
        should this use the minimal catalog entry columns 
        as defined in catalog.get_refcatalog ?
        
    seplimit: float
        separation in arcsec to define the (self-) isolation
 
    radius: float, array
        aperture photometry radius (could be 1d-list).
        In unit of pixels. 
        To broadcast, this has [:,None] applied to internally.
        
    bkgann: list
        properties of the annulus [min, max] (in unit of pixels).
        This should broadcast with radius the broadcasted radius.
        
    joined: bool
        should the returned aperture photometry catalog be joined
        with the input catalog ?
        ** WARNING dask_level='medium' & joined=True may failed due to serialization issues **
        
    Returns
    -------
    DataFrame
        following column format: f_i, f_i_e, f_i_f 
        for flux, flux error and flag for each radius. 
        pandas or dask depending on sciimg / dask_level.
    """
    use_dask = dask_level is not None
    
    # flexible input
    if type(sciimg) is str:
        # dasking (or not) `ztfimg.ScienceQuadrant.from_filename`
        if not use_dask:
            sciimg = ztfimg.ScienceQuadrant.from_filename(sciimg, as_path=as_path)
        elif dask_level == "deep": # dasking inside ztfimg
            sciimg = ztfimg.ScienceQuadrant.from_filename(sciimg, as_path=as_path, 
                                                          use_dask=True)
        elif dask_level == "medium": # dasking outside ztfimg
            sciimg = dask.delayed(ztfimg.ScienceQuadrant.from_filename)(sciimg, as_path=as_path)
        else:
            raise ValueError(f"Cannot parse dask_level {dask_level} | medium or deep accepted.")
        
    if 'CCD' in str(type(sciimg)) :
        coord = 'ij'
        rm_bkgd = 'quadrant'
    else : 
        coord = 'xy'
        rm_bkgd=True
        
    if type(cat) is str:
        if minimal_columns:
            if cat in _MINIMAL_COLNAMES:
                columns = _MINIMAL_COLNAMES[cat]
            else:
                warnings.warn(f"no minimal columns implemented for {cat}")
                columns = None                
        else:
            columns = None
            

        if apply_proper_motion:
            # Not compatible with delayed sciimg
            from astropy.time import Time
            mjd_cat=Time(sciimg.get_header()['OBSMJD'], format='mjd')
        else: 
            mjd_cat=None
    
        cat = get_img_refcatalog(sciimg, cat, coord=coord, radius=refcat_radius, 
                                 apply_proper_motion=apply_proper_motion, 
                                 mjd_cat=mjd_cat) # this handles dask.
        
        if columns is not None: #
            if coord == 'ij' : 
                columns = columns[:-2]+['i','j']
            cat = cat[columns]

    if "isolated" not in cat:
        # add to cat the (self-)isolation information
        cat = cat.join( get_isolated(cat, seplimit=seplimit) ) # this handles dask.
        
    # data
    data = sciimg.get_data(apply_mask=True, rm_bkgd=rm_bkgd) # cleaned image
    mask = sciimg.get_mask()
    err = sciimg.get_noise("rms")

    # run aperture
    radius = np.atleast_1d(radius)[:,None] # broadcasting
    x = cat[coord[0]].values
    y = cat[coord[1]].values
    ap_dataframe = sciimg.get_aperture(x, y, 
                                        radius=radius,
                                        bkgann=bkgann,
                                        data=data,
                                        mask=mask,
                                        err=err,
                                        as_dataframe=True)
    
    radcols = [f'r_{k}' for k in range(len(radius))]
    radius = radius.ravel() #Remove broadcasting

    if "dask" in str( type(sciimg) ):
        import dask.dataframe as dd
        colnames  = [f'f_{k}' for k in range(len(radius))]
        colnames += [f'f_{k}_e' for k in range(len(radius))]
        colnames += [f'f_{k}_f' for k in range(len(radius))]

        meta = pandas.DataFrame(columns=colnames, dtype="float32")
        ap_dataframe = dd.from_delayed(ap_dataframe, meta=meta)
        ap_dataframe = ap_dataframe.assign(**{key : val for key, val in zip(radcols, radius.astype(np.int32))})
        # Inplace Column asssignment does not work with Dask-Dataframe. 
        
    else : 
        ap_dataframe = ap_dataframe.astype('float32') #Limit memory usage when not dask.
        ap_dataframe[radcols] = radius.astype(np.int32) 
        
    ap_dataframe = ap_dataframe.astype({f'f_{k}_f': 'int32' for k in range(len(radius))})
        
    if joined:
        cat_ = cat.reset_index()
        cat_ = cat_.astype({col : 'float32' for col in cat_.columns[cat_.dtypes == 'float64']})
        merged_cat = cat_.join(ap_dataframe)#.set_index("index")
        return merged_cat

    return ap_dataframe

def store_aperture_catalog(cat, new_filename, **kwargs):
    """ store the given catalog into the new filename 
    
    **kwargs goes to pandas.DataFrame.to_parquet
    """
    # Comments for developpers:
    # a function might be a bit too much just for this
    # but it sets the structure for more complexe storage
    # if we want to at some point.
    dirname = os.path.dirname(new_filename)
    if not os.path.isdir(dirname):
        os.makedirs(dirname, exist_ok=True)
    
    out = cat.to_parquet(new_filename, **kwargs)
    return out
    
