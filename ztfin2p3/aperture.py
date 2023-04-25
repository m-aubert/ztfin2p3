""" Module to run the aperture photometry """

import numpy as np
import ztfimg
from ztfimg.catalog import get_isolated
from .catalog import get_img_refcatalog

def get_aperture_photometry(sciimg, cat="gaia_dr2", dask_level="deep", as_path=True,
                                seplimit=20,
                                radius=np.linspace(2,10,9),
                                bkgann=[10,11], 
                                joined=True):
    """ run the aperture photometry on science image given input catalog.
    
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
        Set to True if the filename are path and not just ztf filename.
    
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
        
    if type(cat) is str:
        cat = get_img_refcatalog(sciimg, cat) # this handles dask.

    if "isolated" not in cat:
        # add to cat the (self-)isolation information
        cat = cat.join( get_isolated(cat, seplimit=seplimit) ) # this handles dask.
        
    # data
    data = sciimg.get_data(apply_mask=True, rm_bkgd=True) # cleaned image
    mask = sciimg.get_mask()
    err = sciimg.get_noise("rms")

    # run aperture
    radius = np.atleast_1d(radius)[:,None] # broadcasting
    x = cat["x"].values
    y = cat["y"].values
    ap_dataframe = sciimg.get_aperture(x, y, 
                                        radius=radius,
                                        bkgann=bkgann,
                                        data=data,
                                        mask=mask,
                                        err=err,
                                        as_dataframe=True)
    if "dask" in str( type(sciimg) ):
        import dask.dataframe as dd
        colnames  = [f'f_{k}' for k in range(len(radius))]
        colnames += [f'f_{k}_e' for k in range(len(radius))]
        colnames += [f'f_{k}_f' for k in range(len(radius))]
        meta = pandas.DataFrame(columns=colnames, dtype="float32")
        ap_dataframe = dd.from_delayed(ap_dataframe, meta=meta)
        
    if joined:
        cat_ = cat.reset_index()
        merged_cat = cat_.join(ap_dataframe)#.set_index("index")
        return merged_cat

    return ap_dataframe

