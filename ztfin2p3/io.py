""" I/O for the IN2P3 pipeline """

import os
from ztfquery.io import LOCALSOURCE
BASESOURCE = os.path.join(LOCALSOURCE, "ztfin2p3")


def get_rawfile(which, date, ccdid=None, fid=None,
                client=None, as_dask="computed", whatfile='file',
                **kwargs):
    """ 
    which: [string]
        - flat
        - bias
        - starflat [not implemented yet]
        - science
        
    date: [string (or list of)]
            date can either be a single string or a list of two dates in isoformat.
            - two dates format: date=['start','end'] is isoformat
              e.g. date=['2019-03-14','2019-03-25']
            
            - single string: four format are then accepted, year, month, week or day:
                - yyyy: get the full year. (string of length 4)
                       e.g. date='2019'
                - yyyymm: get the full month (string of length 6)
                       e.g. date='201903'
                - yyyywww: get the corresponding week of the year (string of length 7)
                       e.g. date='2019045'  
                - yyyymmdd: get the given single day (string of length 8)
                       e.g. date='20190227'
            
    ccdid, fid: [int or list of] -optional-
        value or list of ccd (ccdid=[1->16]) or filter (fid=[1->3]) you want
        to limit to.


    client: [Dask client] -optional-
        provide the client that hold the dask cluster.

    as_dask: [string] -optional-
        what format of the data do you want.
        - delayed
        - futures
        - computed (normal data)


    **kwargs goes to get_metadata()
       option examples:
       - which='flat': 
           - ledid
       - which='science':
           - field

    Returns
    -------
    list

    Example:
    --------
    #
    # - Flat (with LEDID)
    #
    Get the rawflat image file of ledid #2 for the 23th week of
    2020. Limit this to ccd #4
    files = get_rawfile('flat', '2020023', ledid=2, ccdid=4)

    """
    from .metadata import get_rawmeta
    return get_rawmeta(which=which, date=date, ccdid=ccdid, fid=fid,
                           as_dask=as_dask, client=client,
                           getwhat='file', **kwargs)

def get_filepath(which, date, ccdid=None, **kwargs):
    """ provides the path to the ztfin2p3 pipeline product. 
    See get_rawfile() for raw data input.

    which: [string]
        - flat
        - bias
        - starflat [not implemented yet]
        - science [not implemented yet]
        
    date: [string (or list of)]
            date should be a single string
            
            - three format are accepted corresponding to monthly, weekly or dayly:
                - yyyymm: get the full month (string of length 6)
                       e.g. date='201903'
                - yyyywww: get the corresponding week of the year (string of length 7)
                       e.g. date='2019045'  
                - yyyymmdd: get the given single day (string of length 8)
                       e.g. date='20190227'
            
    ccdid, fid: [int or list of] -optional-
        value or list of ccd (ccdid=[1->16]) or filter (fid=[1->3]) you want
        to limit to.
    """
    if len(date)==6:
        timeprop = dict(yyyy=int(date[:4]),mm=int(date[4:]))
        timekind = "monthly"
    elif len(date)==7:
        timeprop = dict(yyyy=int(date[:4]),www=int(date[4:]))
        timekind = "weekly"        
    elif len(date)==8:
        timeprop = dict(yyyy=int(date[:4]),mm=int(date[4:6]), dd=int(date[6:]))
        timekind = "daily"
    else:
        raise ValueError(f"Cannot parse the inpout date format {date} ; yyyymm, yyyywww or yyyymmdd expected.")

    prop = {**dict(ccdid=ccdid),**kwargs}
    return eval(f"get_{timekind}_{which}file")(**{**timeprop,**prop})

    


#########################
#                       #
#                       #
#    CALIBRATION        #
#                       #
#                       #
#########################
# Calibration
BIAS_DIR = os.path.join(BASESOURCE, "cal/bias")
FLAT_DIR = os.path.join(BASESOURCE, "cal/flat")
STARFLAT_DIR = os.path.join(BASESOURCE, "cal/starflat")
# Science
SCIENCE_DIR = os.path.join(BASESOURCE, "sci")

def get_directory(kind, subkind):
    """ """
    if subkind == "flat":
        return FLAT_DIR
    if subkind == "bias":
        return BIAS_DIR
    if subkind == "starflat":
        return STARFLAT_DIR
    if subkind in ["object","science"]:
        return SCIENCE_DIR
    
# =========== #
#  BIAS       #
# =========== #
def get_daily_biasfile(yyyy, mm, dd, ccdid):
    """ 
    format: cal/bias/yyyy/mmdd/ztfin2p3_yyyymmdd_000000_bi_ccdid_bias.fits
    
    """
    filestructure = f"ztfin2p3_{yyyy:04d}{mm:02d}{dd:02d}_000000_bi_c{ccdid:02d}_bias.fits" 
    return os.path.join(BIAS_DIR, f"{yyyy:04d}",f"{mm:02d}{dd:02d}", 
                        filestructure)

def get_weekly_biasfile(yyyy, www, ccdid):
    """ 
    format: cal/bias/yyyy/www/ztfin2p3_yyyywww_000000_bi_ccdid_bias.fits
    
    """
    filestructure = f"ztfin2p3_{yyyy:04d}{www:03d}_000000_bi_c{ccdid:02d}_bias.fits" 
    return os.path.join(BIAS_DIR, f"{yyyy:04d}",f"{www:03d}", 
                        filestructure)

def get_monthly_biasfile(yyyy, mm, ccdid):
    """ 
    format: cal/bias/yyyy/mm/ztfin2p3_yyyymm_000000_bi_ccdid_bias.fits
    
    """
    filestructure = f"ztfin2p3_{yyyy:04d}{mm:02d}_000000_bi_c{ccdid:02d}_bias.fits" 
    return os.path.join(BIAS_DIR, f"{yyyy:04d}",f"{mm:02d}", 
                        filestructure)

# =========== #
#  FLAT       #
# =========== #
def get_daily_flatfile(yyyy, mm, dd, ccdid, filtername=None, ledid=None):
    """ 
    
    ledid: [int]
        number of the LED.
        if 0, this will be the best combination (see header)


    format: cal/flat/yyyy/mmdd/ztfin2p3_yyyymmdd_000000_filtername_ccdid_ledid_flat.fits
    """
    if ledid is None:
        ledid = 0
        if filtername is None:
            raise ValueError("ledid and filtername  cannot but both None")
        
    if filtername is None:
        from .calibration.flat import ledid_to_filtername
        filtername = ledid_to_filtername(ledid)
    
    filestructure = f"ztfin2p3_{yyyy:04d}{mm:02d}{dd:02d}_000000_{filtername}_c{ccdid:02d}_l{ledid:02d}_flat.fits" 
    return os.path.join(FLAT_DIR, f"{yyyy:04d}",f"{mm:02d}{dd:02d}", 
                        filestructure)

def get_weekly_flatfile(yyyy, www, ccdid, filtername=None, ledid=None):
    """ 
    www: [int]
        Week number. 
        While there are only 52 weeks in 1 year, this is going to pad it to 3
        to avoid confusion with months.
        
    ledid: [int]
        number of the LED.
        if 0 or None, this will be the best combination (see header)


    format: cal/flat/yyyy/www/ztfin2p3_yyyywww_000000_filtername_ccdid_ledid_flat.fits
    """
    if ledid is None:
        ledid = 0
        if filtername is None:
            raise ValueError("ledid and filtername  cannot but both None")
        
    if filtername is None:
        from .calibration.flat import ledid_to_filtername
        filtername = ledid_to_filtername(ledid)
        
    
    filestructure = f"ztfin2p3_{yyyy:04d}{www:03d}_000000_{filtername}_c{ccdid:02d}_l{ledid:02d}_flat.fits" 
    return os.path.join(FLAT_DIR, f"{yyyy:04d}",f"{www:03d}", 
                        filestructure)

def get_monthly_flatfile(yyyy, mm, ccdid, filtername=None, ledid=None):
    """ 
    mm: [int]
        Month number. (1->12)
        
    ledid: [int]
        number of the LED.
        if 0 or None, this will be the best combination (see header)


    format: cal/flat/yyyy/www/ztfin2p3_yyyywww_000000_filtername_ccdid_ledid_flat.fits
    """
    if ledid is None:
        ledid = 0
        if filtername is None:
            raise ValueError("ledid and filtername  cannot but both None")
        
    if filtername is None:
        from .calibration.flat import ledid_to_filtername
        filtername = ledid_to_filtername(ledid)
        
    filestructure = f"ztfin2p3_{yyyy:04d}{mm:02d}_000000_{filtername}_c{ccdid:02d}_l{ledid:02d}_flat.fits" 
    return os.path.join(FLAT_DIR, f"{yyyy:04d}",f"{mm:02d}", 
                        filestructure)

# =========== #
#  StarFlat   #
# =========== #
def get_monthly_starflatfile(yyyy, mm, ccdid, filtername):
    """ 

    format: cal/starflat/yyyy/mm/ztfin2p3_yyyymm_000000_filtername_ccdid_starflat.fits
    """
    filestructure = f"ztfin2p3_{yyyy:04d}{mm:02d}_000000_{filtername}_c{ccdid:02d}_starflat.fits" 
    return os.path.join(STARFLAT_DIR, f"{yyyy:04d}",f"{mm:02d}", 
                        filestructure)

#########################
#                       #
#                       #
#    SCIENCE IMAGE      #
#                       #
#                       #
#########################
def get_sciencefile(yyyy, mm, dd, fracday, field, filtername, ccdid, qid):
    """ 
    Following the same format as main this's IPAC. Only changing
    ztf->ztfin2p3

    e.g. ztfin2p3/sci/2018/0221/328009/ztfin2p3_20180221328009_700426_zg_c01_o_q1_sciimg.fits'

    """
    filestructure = f"ztfin2p3_{yyyy:04d}{mm:02d}{dd:02d}{fracday:06d}_{field:06d}_{filtername}_c{ccdid:02d}_o_q{qid:1d}_sciimg.fits"
    return os.path.join(BASESOURCE, "sci", f"{yyyy:04d}",f"{mm:02d}{dd:02d}", f"{fracday:06d}",
                        filestructure)
    
