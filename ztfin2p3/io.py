""" I/O for the IN2P3 pipeline """

import os
from ztfquery.io import LOCALSOURCE
BASESOURCE = os.path.join(LOCALSOURCE, "ztfin2p3")

# ==================== #
#                      #
#   Calibration        #
#                      #
# ==================== #
def get_daily_biasfile(yyyy, mm, dd, ccdid):
    """ """
    filestructure = f"ztfin2p3_{yyyy:04d}{mm:02d}{dd:02d}_000000_bi_c{ccdid:02d}_bias.fits" 
    return os.path.join(BASESOURCE, "cal/bias", f"{yyyy:04d}",f"{mm:02d}{dd:02d}", 
                        filestructure)

def get_daily_flatfile(yyyy, mm, dd, ccdid, filtername, ledid):
    """ 
    
    ledid: [int]
        number of the LED.
        if 0, this will be the best combination (see header)

    """
    filestructure = f"ztfin2p3_{yyyy:04d}{mm:02d}{dd:02d}_000000_{filtername}_c{ccdid:02d}_l{ledid:02d}_flat.fits" 
    return os.path.join(BASESOURCE, "cal/flat", f"{yyyy:04d}",f"{mm:02d}{dd:02d}", 
                        filestructure)

def get_monthly_flatfile(yyyy, mm, ccdid, filtername):
    """ """
    filestructure = f"ztfin2p3_{yyyy:04d}{mm:02d}_000000_{filtername}_c{ccdid:02d}_starflat.fits" 
    return os.path.join(BASESOURCE, "cal/starflat", f"{yyyy:04d}",f"{mm:02d}", 
                        filestructure)
