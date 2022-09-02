""" I/O for the IN2P3 pipeline """

import os
from ztfquery.io import LOCALSOURCE
BASESOURCE = os.path.join(LOCALSOURCE, "ztfin2p3")

PACKAGE_PATH = os.path.dirname(os.path.realpath(__file__))

def get_config(which):
    """ 
    which: string
        - calibration

    return
    ------
    dict
    """
    import tomli
    if which in ["cal","calib","calibration"]:
        configfile = os.path.join(PACKAGE_PATH, "config", "calibration.tolm")
    else:
        raise NotImplementedError(f"getting config for {which} is not implemented")
    
    with open(configfile, 'rb') as file_:
        config = tomli.load(file_)
        
    return config
    
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
    from .metadata import get_rawfile
    return get_rawfile(which=which, date=date, ccdid=ccdid, fid=fid,
                           as_dask=as_dask, client=client, **kwargs)

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

# =========== #
#  FLAT       #
# =========== #
def get_flat_for_exposure(yyyymmdd, filtername):
    """ """
    raise NotImplementedError("to be implemented with config.")
    
def get_daily_flatfile(day, ccdid, filtername=None, ledid=None):
    """ 
    day: 
        accepted formats:
        - yyyy-mm-dd
        - yyyymmdd
    ledid: [int]
        number of the LED.
        if 0, this will be the best combination (see header)


    format: cal/flat/yyyy/mmdd/ztfin2p3_yyyymmdd_000000_filtername_ccdid_ledid_flat.fits
    """
    day = str(day).replace("-","") # to accept both yyyy-mm-dd and yyyymmdd
    yyyy,mm,dd = day[:4],day[4:6],day[6:]
    
    if ledid is None:
        ledid = 0
        if filtername is None:
            raise ValueError("ledid and filtername cannot be both None")
        
    if filtername is None:
        from .calibration.flat import ledid_to_filtername
        filtername = ledid_to_filtername(ledid)
    
    filestructure = f"ztfin2p3_{yyyy:04d}{mm:02d}{dd:02d}_000000_{filtername}_c{ccdid:02d}_l{ledid:02d}_flat.fits" 
    return os.path.join(FLAT_DIR, f"{yyyy:04d}",f"{mm:02d}{dd:02d}", 
                        filestructure)

def get_period_flatfile(start, end, ccdid, filtername=None, ledid=None):
    """
    start, end:
        period boundaries
        accepted formats
         - yyyy-mm-dd
         - yyyymmdd
    
    format: cal/flat/yyyy/mmdd/ztfin2p3_yyyymmddyyyymmdd_000000_filtername_ccdid_ledid_flat.fits
    """
    noled= (ledid is None)
    if noled and filtername is None:
        raise ValueError("ledid and filtername cannot be both None")
      
    elif filtername is None:
        from .calibration.flat import ledid_to_filtername
        filtername = ledid_to_filtername(ledid)
        
    start = str(start).replace("-","") # so it accepts this format yyyy-mm-ddand yyyymmdd
    end = str(end).replace("-","") # so it accepts this format yyyy-mm-dd and yyyymmdd

    filestructure = f"ztfin2p3_{start}{end}_000000_{filtername}_c{ccdid:02d}"
    if noled:
        filestructure +="_flat.fits" 
    else:
        filestructure +=f"_l{ledid:02d}_flat.fits" 
    return filestructure

    
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
def get_sciencefile(yyyy, mm, dd, fracday, field, filtername, ccdid,
                        qid, suffix="sciimg.fits"):
    """ 
    Following the same format as main this's IPAC. Only changing
    ztf->ztfin2p3

    e.g. ztfin2p3/sci/2018/0221/328009/ztfin2p3_20180221328009_700426_zg_c01_o_q1_sciimg.fits'

    """
    filestructure = f"ztfin2p3_{yyyy:04d}{mm:02d}{dd:02d}{fracday:06d}_{field:06d}_{filtername}_c{ccdid:02d}_o_q{qid:1d}_{suffix}"
    return os.path.join(BASESOURCE, "sci", f"{yyyy:04d}",f"{mm:02d}{dd:02d}", f"{fracday:06d}",
                        filestructure)
    
