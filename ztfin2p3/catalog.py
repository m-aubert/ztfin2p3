""" module to handle catalog in the ztfin2p3 pipeline """

import os
import pandas
import numpy as np

IN2P3_LOCATION = "/sps/lsst/datasets/refcats/htm/v1/"
IN2P3_CATNAME = {"ps1":"ps1_pv3_3pi_20170110",
                 "gaia_dr2":"gaia_dr2_20190808",
                 "sdss":"sdss-dr9-fink-v5b"}


def get_refcatalog(ra, dec, radius, which, enrich=True):
    """  fetch an lsst refcats catalog stored at the cc-in2p3.

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
    tables = [Table.read(os.path.join(dirpath, f"{htm_id_}.fits"), format="fits") for htm_id_ in hmt_id]
    # table.to_pandas() only accepts single-value columns.
    t_ = tables[0] # test table 
    ok_names = [name for name in t_.colnames if len(t_[name].shape) <= 1] # assume all tables have the same format.
    cat = pandas.concat([t_[ok_names].to_pandas() for t_ in tables]).reset_index(drop=True)
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
