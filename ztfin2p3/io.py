""" I/O for the IN2P3 pipeline """

import os
from ztfquery.io import LOCALSOURCE
BASESOURCE = os.path.join(LOCALSOURCE, "ztfin2p3")



# =========== #
#  BIAS       #
# =========== #
def get_daily_biasfile(yyyy, mm, dd, ccdid):
    """ 
    format: cal/bias/yyyy/mmdd/ztfin2p3_yyyymmdd_000000_bi_ccdid_bias.fits
    
    """
    filestructure = f"ztfin2p3_{yyyy:04d}{mm:02d}{dd:02d}_000000_bi_c{ccdid:02d}_bias.fits" 
    return os.path.join(BASESOURCE, "cal/bias", f"{yyyy:04d}",f"{mm:02d}{dd:02d}", 
                        filestructure)

def get_weekly_biasfile(yyyy, www, ccdid):
    """ 
    format: cal/bias/yyyy/www/ztfin2p3_yyyywww_000000_bi_ccdid_bias.fits
    
    """
    filestructure = f"ztfin2p3_{yyyy:04d}{www:03d}_000000_bi_c{ccdid:02d}_bias.fits" 
    return os.path.join(BASESOURCE, "cal/bias", f"{yyyy:04d}",f"{www:03d}", 
                        filestructure)

def get_monthly_biasfile(yyyy, mm, ccdid):
    """ 
    format: cal/bias/yyyy/mm/ztfin2p3_yyyymm_000000_bi_ccdid_bias.fits
    
    """
    filestructure = f"ztfin2p3_{yyyy:04d}{mm:02d}_000000_bi_c{ccdid:02d}_bias.fits" 
    return os.path.join(BASESOURCE, "cal/bias", f"{yyyy:04d}",f"{mm:02d}", 
                        filestructure)

# =========== #
#  FLAT       #
# =========== #
def get_daily_flatfile(yyyy, mm, dd, ccdid, filtername, ledid=None):
    """ 
    
    ledid: [int]
        number of the LED.
        if 0, this will be the best combination (see header)


    format: cal/flat/yyyy/mmdd/ztfin2p3_yyyymmdd_000000_filtername_ccdid_ledid_flat.fits
    """
    if ledid is None:
        ledid = 0

    filestructure = f"ztfin2p3_{yyyy:04d}{mm:02d}{dd:02d}_000000_{filtername}_c{ccdid:02d}_l{ledid:02d}_flat.fits" 
    return os.path.join(BASESOURCE, "cal/flat", f"{yyyy:04d}",f"{mm:02d}{dd:02d}", 
                        filestructure)

def get_weekly_flatfile(yyyy, www, ccdid, filtername, ledid=None):
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
    
    filestructure = f"ztfin2p3_{yyyy:04d}{www:03d}_000000_{filtername}_c{ccdid:02d}_l{ledid:02d}_flat.fits" 
    return os.path.join(BASESOURCE, "cal/flat", f"{yyyy:04d}",f"{www:03d}", 
                        filestructure)

def get_monthly_flatfile(yyyy, mm, ccdid, filtername, ledid=None):
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
    
    filestructure = f"ztfin2p3_{yyyy:04d}{mm:02d}_000000_{filtername}_c{ccdid:02d}_l{ledid:02d}_flat.fits" 
    return os.path.join(BASESOURCE, "cal/flat", f"{yyyy:04d}",f"{mm:02d}", 
                        filestructure)

# =========== #
#  StarFlat   #
# =========== #
def get_monthly_starflatfile(yyyy, mm, ccdid, filtername):
    """ 

    format: cal/starflat/yyyy/mm/ztfin2p3_yyyymm_000000_filtername_ccdid_starflat.fits
    """
    filestructure = f"ztfin2p3_{yyyy:04d}{mm:02d}_000000_{filtername}_c{ccdid:02d}_starflat.fits" 
    return os.path.join(BASESOURCE, "cal/starflat", f"{yyyy:04d}",f"{mm:02d}", 
                        filestructure)
