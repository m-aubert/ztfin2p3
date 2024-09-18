import numpy as np

# ================ #
#                  #
#    NUMPY         #
#                  #
# ================ # 

def get_in_squares(squares, xy):
    """ """
    if np.shape(squares) == (4,):
        # Accepts both 
        return get_in_squares(squares[None,:], xy)[0]
    
    return (  (xy[:, 0, None] >= squares[:, 0]) 
            & (xy[:, 0, None] <= squares[:, 1]) 
            & (xy[:, 1, None] >= squares[:, 2]) 
            & (xy[:, 1, None] <= squares[:, 3])).T

# ================ #
#                  #
#    PARSER        #
#                  #
# ================ # 

def parse_singledate(date):
    """ parse the input 'date' into a datetime start and end
    date could have the following format:
        - yyyy: get  the full yearx
        - yyyymm: get the full month 
                 (stars at day 1, ends day 1 next month)
        - yyyywww: get the full calendar week
                 (stars the monday of day 1, ends the next monday)
        - yyyymmdd: get the full day
                 (stars the day, ends the next)
    """
    import datetime
    date = str(date)
    if len(date) == 4: # year
        date_start = datetime.date.fromisoformat(f"{date}-01-01")
        date_end   = datetime.date.fromisoformat(f"{int(date)+1}-01-01")
        
    elif len(date) == 6: # year+month
        from calendar import monthrange
        year, month = int(date[:4]),int(date[4:])
        date_start = datetime.date.fromisoformat(f"{year}-{month:02d}-01")
        # Doing this avoid to think about end of year issues
        date_end   = date_start + datetime.timedelta(days=monthrange(year, month)[1]) # +1 month = N(28-31) days

    elif len(date) == 7: # year+week
        year, week = int(date[:4]),int(date[4:])
        date_start = datetime.date.fromisocalendar(year, week, 1)
        date_end   = date_start + datetime.timedelta(weeks=1) # +1 week

    elif len(date) == 8: # year+month+day
        year, month, day = int(date[:4]),int(date[4:6]),int(date[6:])
        date_start = datetime.date.fromisoformat(f"{year:04d}-{month:02d}-{day:02d}")
        date_end   = date_start + datetime.timedelta(days=1)
    else:
        raise ValueError(f"Cannot parse the input single date format {date}, size 6(yyyymm), 7(yyyywww) or 8(yyyymmdd) expected")
        
    return date_start, date_end

def header_from_files(files, keys, refheader_id=0, inputkey="INPUT"):
    """ """
    from astropy.io import fits
    
    header = fits.getheader(files[refheader_id])
    newheader = fits.Header()
    for k_ in keys:
        newheader.set(k_, header.get(k_,""), header.comments[k_])
        
    basenames = [l.split("/")[-1] for l in files]
    for i, basename_ in enumerate(basenames):
        newheader.set(f"{inputkey}{i:02d}",basename_, "input image")
        
    return newheader


# ----------------------------- #
#  Hierarchical Triangular Mesh #
# ----------------------------- #
def get_htm_intersect(ra, dec, radius, depth=7, **kwargs):
    """ Module to get htm overlap (ids)  
    = Based on HMpTy (pip install HMpTy) =

    Parameters
    ----------
    ra, dec: [floatt]
        central point coordinates in decimal degrees or sexagesimal
    
    radius: [float]
        radius of circle in degrees

    depth: [int] -optional-
        depth of the htm
        
    **kwags goes to HMpTy.HTM.intersect 
         inclusive:
             include IDs of triangles that intersect the circle as well as 
             those completely inclosed by the circle. Default True

    Returns
    -------
    list of ID (htm ids overlapping with the input circle.)
    """
    from HMpTy import HTM
    return HTM(depth=depth).intersect(ra, dec, radius, **kwargs)

# --------------------------- #
# - Conversion Tools        - #
# --------------------------- #
def njy_to_mag(njy_, njyerr_=None):
    """ get AB magnitudes corresponding to the input nJy fluxes.
    Returns
    -------
    mags (or mags, dmags if njyerr_ is not None)
    """
    with np.errstate(divide="ignore"):
        # ignore "divide by zero encountered in log10" warning for null fluxes
        mags = -2.5 * np.log10(1e-9 / 3631 * njy_)
    if njyerr_ is None:
        return mags
    dmags = 2.5 / np.log(10) * njyerr_ / njy_
    return mags, dmags



# ----------------------------- #
#             Healpix           #
# ----------------------------- #
def get_healpix_intersect(ra, dec, radius, nside=64, **kwargs):
    """ Module to get healpix overlap (ids)  

    Parameters
    ----------
    ra, dec: [floatt]
        central point coordinates in decimal degrees
    
    radius: [float]
        radius of circle in degrees

    nside: [int] -optional-
        The healpix nside parameter, must be a power of 2, less than 2**30
        
    **kwags goes to HMpTy.HTM.intersect 
         inclusive:
             include IDs of triangles that intersect the circle as well as 
             those completely inclosed by the circle. Default True

    Returns
    -------
    list of healpix pixels overlapping with the input circle
    """
    import healpy as hp
    # getting the position in the sky
    vec=hp.ang2vec((90-dec)*np.pi/180,ra*np.pi/180)
    # note that inclusive might return too many pixels, 
    # but otherwise we would often miss some
    return hp.query_disc(nside,vec,radius*np.pi/180,inclusive=True,nest=True,**kwargs)
