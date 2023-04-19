""" I/O for the IN2P3 pipeline """

import os
from ztfquery.io import LOCALSOURCE
import numpy as np
from ztfquery import buildurl
BASESOURCE = os.path.join(LOCALSOURCE, "ztfin2p3")
CAL_DIR = os.path.join(BASESOURCE, "cal")
PACKAGE_PATH = os.path.dirname(os.path.realpath(__file__))


# ================ #
#                  #
#  USED            #
#                  #
# ================ #
def ipacfilename_to_ztfin2p3filepath(filename):
    """ convert an ipac-format filename into a filepath for the ztfin2p3 pipeline """
    kind = buildurl.filename_to_kind(filename)
    # Science
    if kind == "sci":
        ipac_filepath = buildurl.filename_to_url(filename, source="local")
        # new filename. Replace:
        # dirpath: /sci/bla -> /ztfin2p3/sci/bla 
        # basename: ztf_* -> ztfin2p3_*
        filepath = ipac_filepath.replace("/sci","/ztfin2p3/sci").replace("/ztf_","/ztfin2p3_")
        
    else:
        raise NotImplementedError("only 'sci' object filename to filepath implemented")
        
    return filepath

def get_flatfiles(date, filtername, ccdid=None):
    """ get the flat filepath for the given date and filtername

    Parameters
    ----------
    date: str
        date in the YYYYMMDD or YYYY-MM-DD format.

    filtername: str
        name of the filter (zg, zr or zi)

    ccdid: int, list
        number of the ccd [1->16] or list of.
        If None, all 16 ccd assumed.

    Returns
    -------
    list
        list of flat filepath.
    """
    if ccdid is None:
        ccdid = np.arange(1,17)

    date = date.replace("-","") # generic format
    # this will change once we move to period, not daily
    year, month, day = date[:4], date[4:6], date[6:]
    flatdir_ = os.path.join(CAL_DIR, "flat", year, f"{month}{day}")
    filenames = [os.path.join(flatdir_, f"ztfin2p3_{date}_000000_{filtername}_c{ccdid_:02d}_l00_flat.fits")
                 for ccdid_ in np.atleast_1d(ccdid)]
    
    return filenames

def get_biasfiles(date, ccdid=None):
    """ get the bias filepath for the given date

    Parameters
    ----------
    date: str
        date in the YYYYMMDD or YYYY-MM-DD format.

    ccdid: int, list
        number of the ccd [1->16] or list of.
        If None, all 16 ccd assumed.

    Returns
    -------
    list
        list of bias filepath.
    """
    if ccdid is None:
        ccdid = np.arange(1,17)

    date = date.replace("-","") # generic format        
    # this will change once we move to period, not daily        
    year, month, day = date[:4], date[4:6], date[6:]
    biasdir_ = os.path.join(CAL_DIR, "bias", year, f"{month}{day}")
    filenames = [os.path.join(biasdir_, f"ztfin2p3_{date}_000000_bi_c{ccdid_:02d}_bias.fits")
                 for ccdid_ in np.atleast_1d(ccdid)]
    
    return filenames












# ================ #
#                  #
#  To Be Checked   #
#                  #
# ================ #


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
# Calibration #
# =========== #
# ------- #
#  Daily  #
# ------- #
def get_daily_biasfile(day, ccdid):
    """ 
    day: 
        accepted formats:
        - yyyy-mm-dd
        - yyyymmdd


    format: cal/bias/yyyy/mmdd/ztfin2p3_yyyymmdd_000000_bi_ccdid_bias.fits
    
    """
    day = str(day).replace("-","") # to accept both yyyy-mm-dd and yyyymmdd
    yyyy,mm,dd = int(day[:4]),int(day[4:6]),int(day[6:])

    
    filestructure = f"ztfin2p3_{yyyy:04d}{mm:02d}{dd:02d}_000000_bi_c{ccdid:02d}_bias.fits" 
    return os.path.join(BIAS_DIR, f"{yyyy:04d}",f"{mm:02d}{dd:02d}", 
                        filestructure)


# ------- #
# Period  #
# ------- #
def get_period_biasfile(start, end, ccdid, mkdirs=False):
    """ get the period bias filepath

    
    format: cal/bias/yyyymmddyyyymmdd/ztfin2p3_yyyymmddyyyymmdd_000000_bi_ccdid_bias.fits

    Parameters
    ----------
    start: str
        period start. Accepted formats
         - yyyy-mm-dd
         - yyyymmdd
         
    end: str
        period end. Accepted formats
         - yyyy-mm-dd
         - yyyymmdd 

    ccdid: int
        ccd id (1->16)

    Returns
    -------
    str
        fullpath to the period bias flat for the ccdid.

    """
    # so it accepts this format yyyy-mm-ddand yyyymmdd
    start = str(start).replace("-","") 
    end = str(end).replace("-","") # so it accepts this format yyyy-mm-dd and yyyymmdd

    period = f"{start}{end}"
    filestructure = f"ztfin2p3_{period}_000000_bi_c{ccdid:02d}_bias.fits"
    
    return os.path.join(BIAS_DIR, period, filestructure)

    
def get_period_flatfile(start, end, ccdid, filtername=None, ledid=None):
    """ get the period flat filepath

    format: cal/flat/yyyymmddyyyymmdd/ztfin2p3_yyyymmddyyyymmdd_000000_filtername_ccdid_[ledid_]flat.fits

    Parameters
    ----------
    start: str
        period start. Accepted formats
         - yyyy-mm-dd
         - yyyymmdd
         
    end: str
        period end. Accepted formats
         - yyyy-mm-dd
         - yyyymmdd 

    ccdid: int
        ccd id (1->16)

    filtername: str or None
       = must be given if ledid is None =
        name of the filter (zg, zr, zi)

    ledid: int or None
       = must be given if filtername is None =
       id of the LED. 

    Returns
    -------
    str
        fullpath to the period bias flat for the ccdid.

    """
    noled = (ledid is None)
    if noled and filtername is None:
        raise ValueError("ledid and filtername cannot be both None")
      
    elif filtername is None:
        from .calibration.flat import ledid_to_filtername
        filtername = ledid_to_filtername(ledid)
        
        
    start = str(start).replace("-","") # so it accepts this format yyyy-mm-ddand yyyymmdd
    end = str(end).replace("-","") # so it accepts this format yyyy-mm-dd and yyyymmdd

    period = f"{start}{end}"


    filestructure = f"ztfin2p3_{period}_000000_{filtername}_c{ccdid:02d}"
    
    if noled:
        filestructure +="_flat.fits" 
    else:
        filestructure +=f"_l{ledid:02d}_flat.fits"


        
    return os.path.join(FLAT_DIR, period, filestructure)


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
    yyyy,mm,dd = int(day[:4]),int(day[4:6]),int(day[6:])
    
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





class CCIN2P3(object):
    """ """

    def __init__(self, auth=None, connect=True):
        """ """
        if self.running_at_cc:
            self._connected = True
        else:
            self.load_ssh(auth=auth)
            self._connected = False
            if connect:
                self.connect(auth=auth)
        self.logger = logging.getLogger(__name__)

    def load_ssh(self, auth=None):
        from paramiko import SSHClient

        self._auth = auth
        self._ssh = SSHClient()
        self._ssh.load_system_host_keys()

    def connect(self, auth=None):
        """ """
        if auth is None:
            auth = _load_id_("ccin2p3")

        username, password = auth
        try:
            self._ssh.connect("cca.in2p3.fr", username=username, password=password)
        except:
            raise IOError("Cannot connect to cca.in2p3.fr with given authentification")

        self._connected = True

    @classmethod
    def scp(cls, fromfile, tofile, auth=None):
        """ """
        if fromfile.startswith(CCIN2P3_SOURCE):
            method = "scp_get"
        elif tofile.startswith(CCIN2P3_SOURCE):
            method = "scp_put"
        else:
            raise ValueError(
                f"None of fromfile or tofile stars with {CCIN2P3_SOURCE}. Cannot use scp(), see scp_get or scp_put"
            )

        this = cls(auth=auth, connect=True)
        return getattr(this, method)(fromfile, tofile)

    def scp_get(self, remotefile, localfile, auth=None, overwrite=False):
        """ """
        from scp import SCPClient

        if not self._connected:
            self.connect(auth)

        directory = os.path.dirname(localfile)
        oldmask = os.umask(0o002)

        if not os.path.exists(directory):
            self.logger.debug(f"scp_get(): creating {directory}")

            os.makedirs(directory, exist_ok=True)

        with SCPClient(self.ssh.get_transport()) as scp:
            scp.get(remotefile, localfile)

    def scp_put(self, localfile, remotefile, auth=None):
        """ """
        from scp import SCPClient

        if not self._connected:
            self.connect(auth)

        with SCPClient(self.ssh.get_transport()) as scp:
            scp.put(localfile, remotefile)

    def query_catalog(self, ra, dec, radius, catname="gaia", depth=7, **kwargs):
        """query catalog ; works only when logged at the CCIN2P3"""
        if not self.running_at_cc:
            raise IOError("Only works if running from the ccin2p3")

        import os
        from htmcatalog import htmquery

        LSST_REFCAT_DIR = "/sps/lsst/datasets/refcats/htm/v1"
        KNOW_REFCAT = {
            "gaiadr1": "gaia_DR1_v1",
            "gaiadr2": "gaia_dr2_20190808",
            "ps1dr1": "ps1_pv3_3pi_20170110",
            "sdssdr9": "sdss-dr9-fink-v5b",
        }
        KNOW_REFCAT["gaia"] = KNOW_REFCAT["gaiadr2"]
        KNOW_REFCAT["ps1"] = KNOW_REFCAT["ps1dr1"]
        if catname not in KNOW_REFCAT:
            raise ValueError(
                f"unknown catalog {catname}. Aviability: "
                + ", ".join(list(KNOW_REFCAT.keys()))
            )

        hq = htmquery.HTMQuery(
            depth, os.path.join(LSST_REFCAT_DIR, KNOW_REFCAT[catname])
        )
        return hq.fetch_cat(ra, dec, radius, **kwargs)

    # ============= #
    #  Properties   #
    # ============= #
    @property
    def ssh(self):
        """ """
        return self._ssh

    @property
    def running_at_cc(self):
        """ """
        hostname = os.uname()[1]
        return "cca" in hostname or "ccwige" in hostname

