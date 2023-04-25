""" module to handle catalog in the ztfin2p3 pipeline 



Usage
-----
import ztfimg
from ztfin2p3 import catalog

filename = "ztf_20190331168461_700500_zg_c13_o_q2_sciimg.fits"
# set use_dask if access to dask cluster.
sci = ztfimg.ScienceQuadrant.from_filename(filename, as_path=False, use_dask=False)
cat = catalog.get_img_refcatalog(sci, "gaia_dr2")
# if sci is delayed or use_dask cat will be a dask.dataframe
"""

import os
import pandas
import numpy as np

IN2P3_LOCATION = "/sps/lsst/datasets/refcats/htm/v1/"
IN2P3_CATNAME = {"ps1":"ps1_pv3_3pi_20170110",
                 "gaia_dr2":"gaia_dr2_20190808",
                 "sdss":"sdss-dr9-fink-v5b"}


_KNOWN_COLUMNS = {"gaia_dr2": ['id', 'coord_ra', 'coord_dec',
                               'phot_g_mean_flux','phot_bp_mean_flux', 'phot_rp_mean_flux' ,
                               'phot_g_mean_fluxErr', 'phot_bp_mean_fluxErr',
                               'phot_rp_mean_fluxErr', 'coord_raErr', 'coord_decErr', 'epoch',
                               'pm_ra', 'pm_dec', 'pm_raErr', 'pm_decErr', 'parallax', 'parallaxErr',
                               'astrometric_excess_noise'],
                  "ps1": ['id', 'coord_ra', 'coord_dec', 'parent', 'g_flux', 'r_flux', 'i_flux',
                          'z_flux', 'y_flux', 'i_fluxErr', 'y_fluxErr', 'r_fluxErr', 'z_fluxErr',
                          'g_fluxErr', 'coord_ra_err', 'coord_dec_err', 'epoch', 'pm_ra',
                          'pm_dec', 'pm_ra_err', 'pm_dec_err'],
                  "sdss":['id', 'coord_ra', 'coord_dec', 'parent', 'U_flux', 'G_flux', 'R_flux',
                          'I_flux', 'Z_flux', 'I_fluxErr', 'R_fluxErr', 'Z_fluxErr', 'U_fluxErr',
                          'G_fluxErr']
                  }


def get_img_refcatalog(img, which, radius=0.7, in_fov=True,
                           enrich=True, **kwargs):
    """ fetch an lsst refcats catalog stored at the cc-in2p3 for a given a
    ztfimg image. 

    Parameters
    ----------
    img: ztfimg.Image, dask.delayed
       a ztfimg.Image. dask is supported either if
       ztfimg.Image.use_dask is True or 
       if ztfimg.Image is delayed.
    
    which: str
        Name of the catalog. 
        currently available catalogs:
        - ps1 (pv3_3pi_20170110)
        - gaia_dr2 (20190808)
        - sdss (dr9-fink-v5b)

    radius: float
        radius of circle in degrees

    **kwargs goes to get_refcatalog (columns etc.)

    Returns
    -------
    pandas.DataFrame
    """
    # dasked ?
    is_delayed = ("dask" in str( type(img)) )
    use_dask = is_delayed or img.use_dask

    if which not in _KNOWN_COLUMNS:
        colnames = None
    else:
        colnames = _KNOWN_COLUMNS[which].copy()

    if not use_dask:
        ra, dec = img.get_center("radec") # centroid of the image
        cat = get_refcatalog(ra, dec, radius=radius,
                                 which=which, enrich=enrich,
                                 colnames=colnames,
                                 **kwargs) # catalog
    else:
        import dask
        import dask.dataframe as dd
        ra_dec = dask.delayed(img.get_center)("radec") # centroid of the image
        # columns
        # cat delayed
        cat_delayed = dask.delayed(get_refcatalog)(ra_dec[0], ra_dec[1],
                                                    radius=radius,
                                                    which=which, enrich=enrich,
                                                    colnames=colnames,
                                                       **kwargs) # catalog
        if enrich:
            colnames += ["ra", "dec"]
            colnames += [col.replace("_flux","_mag") for col in colnames
                             if col.endswith("_flux") or col.endswith("_fluxErr")]

        if not is_delayed: # delayed is made after the xy_added
            meta = pandas.DataFrame(columns=colnames, dtype="float32")
            cat = dd.from_delayed(cat_delayed, meta=meta)

    # adding x,y position to catalog
    if is_delayed:
        cat_delayed = img.add_xy_to_catalog(cat_delayed, in_fov=in_fov)
        colnames += ["x", "y"]
        meta = pandas.DataFrame(columns=colnames, dtype="float32")
        cat = dd.from_delayed(cat_delayed, meta=meta)
        
    else: # work for both no dask or img.use_dask=True
        cat = img.add_xy_to_catalog(cat, in_fov=in_fov)
        
    return cat


def get_refcatalog(ra, dec, radius, which, enrich=True,
                       colnames=None):
    """ fetch an lsst refcats catalog stored at the cc-in2p3.

    Parameters
    ----------
    ra, dec: float
        central point coordinates in decimal degrees or sexagesimal
    
    radius: float
        radius of circle in degrees

    which: str
        Name of the catalog. 
        currently available catalogs:
        - ps1 (pv3_3pi_20170110)
        - gaia_dr2 (20190808)
        - sdss (dr9-fink-v5b)
    
    enrich: bool
        IN2P3 catalog have ra,dec coordinates stored in radian
        as coord_ra/dec and flux in nJy
        Shall this add the ra, dec keys coords (in deg) in degree and the magnitude ?

    colnames: list
        names of columns to be considered.

    Returns
    -------
    DataFrame
    """
    from .utils.tools import get_htm_intersect, njy_to_mag
    from astropy.table import Table
    
    if which not in IN2P3_CATNAME:
        raise NotImplementedError(f" Only {list(IN2P3_CATNAME.keys())} CC-IN2P3 catalogs implemented ; {which} given")
    
    hmt_id = get_htm_intersect(ra, dec, radius, depth=7)
    dirpath = os.path.join(IN2P3_LOCATION, IN2P3_CATNAME[which])
    # all tables
    tables = [Table.read(os.path.join(dirpath, f"{htm_id_}.fits")) for htm_id_ in hmt_id]
    # table.to_pandas() only accepts single-value columns.
    if colnames is None:
        t_ = tables[0] # test table 
        colnames = [name for name in t_.colnames if len(t_[name].shape) <= 1] # assume all tables have the same format.
        
    cat = pandas.concat([t_[colnames].to_pandas() for t_ in tables]).reset_index(drop=True)
    if enrich:
        # - ra, dec in degrees
        cat[["ra","dec"]] = cat[["coord_ra","coord_dec"]]*180/np.pi
        # - mags
        fluxcol = [col for col in  cat.columns if col.endswith("_flux")]
        fluxcolerr = [col for col in  cat.columns if col.endswith("_fluxErr")]
        magcol = [col.replace("_flux","_mag") for col in fluxcol]
        magcolerr = [col.replace("_flux","_mag") for col in fluxcolerr]
        cat[magcol], cat[magcolerr] = njy_to_mag( cat[fluxcol].values,cat[fluxcolerr].values )
        
    return cat
