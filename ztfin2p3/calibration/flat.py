
""" library to build the ztfin2p3 pipeline screen flats """
import os
import pandas
import numpy as np
import dask
import dask.array as da
import warnings
from astropy.io import fits


from ztfimg.base import _Image_

LED_FILTER = {"zg":[2,3,4,5],
              "zr":[7,8,9,10],
              "zi":[11,12,13,14],
                }
    
def ledid_to_filtername(ledid):
    """ """
    for f_,v_ in LED_FILTER.items():
        if int(ledid) in v_:
            return f_
    raise ValueError(f"Unknown led with ID {ledid}")



def bulk_buildflat(dates, ledid=None, ccdid="*",  persist_file=False, **kwargs):
    """ """
    dates = np.atleast_1d(dates)

    prop = dict()
    return [build_flat(str(date_),  delay_store=True, persist_file=persist_file,
                        ccdid=ccdid, ledid=ledid, **kwargs)
                   for date_ in dates]

def build_flat(date, ccdid, ledid, delay_store=False, overwrite=True, persist_file=True, **kwargs):
    """ 
    **kwargs goes to build()
    """
    from ..io import get_rawfile, get_filepath
    #
    # Input
    files = get_rawfile("flat", date, ccdid=ccdid, ledid=ledid, as_dask="persist" if persist_file else "delayed") # input (raw data)
    if len(files)==0:
        warnings.warn(f"No file for {date}")
        return

    filtername = ledid_to_filtername(ledid)
    fileout = get_filepath("flat", date, ccdid=ccdid, ledid=ledid, filtername=filtername) # output

    # 
    # Data 
    bflat = FlatBuilder.from_rawfiles(files, persist=False)
    data, header = bflat.build(set_it=False, **kwargs)
    
    # 
    # Output 
    os.makedirs(os.path.dirname(fileout), exist_ok=True) # Make sure the directory exists
    if delay_store:
        return dask.delayed(fits.writeto)(fileout, data, header=header, overwrite=overwrite)
    return fits.writeto(fileout, data, header=header, overwrite=overwrite)
    
    

class Flat( _Image_ ):
    SHAPE = 6160, 6144
    QUADRANT_SHAPE = 3080, 3072
    def __init__(self, data, header=None, use_dask=True):
        """ """
        _ = super().__init__(use_dask=use_dask)
        self.set_data(data)
        if header is not None:
            self.set_header(header)
            
    # ============== #
    #  I/O           # 
    # ============== #
    @classmethod
    def from_filename(cls, filename, use_dask=True):
        """ this first uses ztfquery.io.get_file() then call read_fits() 
        directly use read_fits() if you have the input data already is a full path.
        """
        from ztfquery import io
        fitsfile = io.get_file(filename)
        return cls.read_fits(fitsfile)
    
    @classmethod
    def read_fits(cls, fitsfile, use_dask=True):
        """ """
        if use_dask:
            data = da.from_delayed( dask.delayed(fits.getdata)(fitsfile),
                                shape=cls.SHAPE, dtype="float")
            header= dask.delayed(fits.getheader)(fitsfile)
        else:
            data = fits.getdata(fitsfile)
            header= fits.getheader(fitsfile)

        this = cls(data=data, header=header, use_dask=use_dask)
        this._filename = fitsfile
        return this

    @classmethod
    def build_from_rawfiles(cls, rawfiles, **kwargs):
        """ """
        bflat = FlatBuilder.from_rawfiles(rawfiles, persist=False)
        data, header = bflat.build(set_it=False, **kwargs)
        return cls(data, header=None, use_dask=True)

    # ============== #
    #  Method        # 
    # ============== #
    def get_quadrant_data(self, qid, **kwargs):
        """ **kwargs goes to get_data() this then split the data """
        qid = int(qid)
        dataccd = self.get_data(**kwargs)
        # this accounts for all rotation and rebin did before
        qshape = np.asarray(dataccd.shape)/2. 

        if qid == 0:
            data_ = dataccd[qshape[0]:, qshape[1]:]
        elif qid == 1:
            data_ = dataccd[qshape[0]:, :qshape[1]]
        elif qid == 2:
            data_ = dataccd[:qshape[0], :qshape[1]]
        elif qid == 3:
            data_ = dataccd[:qshape[0], qshape[1]:]
        else:
            raise ValueError(f"qid must be 0,1,2 or 3 {qid} given")
        
        return data_
        
# ==================== #
#                      #
#   Flat Builder       #
#                      #
# ==================== #    
class FlatBuilder( object ): # /day /week /month

    def __init__(self, rawflatcollection):
        """ """
        self.set_imgcollection(rawflatcollection)

    
    # ============== #
    #  I/O           # 
    # ============== #
    @classmethod
    def from_rawfiles(cls, rawfiles, **kwargs):
        """ """
        from ztfimg import raw
        flatcollection = raw.RawFlatCCDCollection.from_filenames(rawfiles, **kwargs)
        return cls(flatcollection)

    def to_fits(self, fileout, header=None, overwrite=True):
        """ Store the data in fits format """

        from astropy.io import fits
        if header is None:
            if not self.has_header():
                raise AttributeError("no header set and no header given.")
            header = self.header

        dirout = os.path.dirname(fileout)
        if not os.path.isdir(dirout):
            os.makedirs(dirout, exist_ok=True)

        fits.writeto(fileout, self.data, header=header,
                         overwrite=overwrite, **kwargs)
        
    # ============== #
    #  Methods       # 
    # ============== #
    # -------- # 
    #  SETTER  #
    # -------- #
    def set_imgcollection(self, imgcollection):
        """ """
        self._imgcollection = imgcollection

    def set_data(self, data):
        """ """
        self._data = data

    def set_header(self, header):
        """ """
        self._header = header
        
    # -------- # 
    # BUILDER  #
    # -------- #
    def build(self, corr_nl=True, corr_overscan=True, clipping=True,
                  set_it=False, inclheader=True, **kwargs):
        """ """
        prop = {**dict(corr_overscan=corr_overscan, corr_nl=corr_nl, clipping=True),
                **kwargs}
        data = self.imgcollection.get_data_mean(**prop)
        if inclheader:
            header = self.build_header()
        else:
            header = None
        if set_it:
            self.set_data(data)
            self.set_header(header)
            
        return data, header

    def build_header(self, keys=None, refid=0, inclinput=False):
        """ """
        from astropy.io import fits

        if keys is None:
            keys = ["ORIGIN","OBSERVER","INSTRUME","IMGTYPE","EXPTIME",
                    "CCDSUM","CCD_ID","CCDNAME","PIXSCALE","PIXSCALX","PIXSCALY",
                    "FRAMENUM","ILUM_LED", "ILUMWAVE", "PROGRMID","FILTERID",
                    "FILTER","FILTPOS","RA","DEC", "OBSERVAT"]

        header = self.imgcollection.get_singleheader(refid, as_serie=True)
        if type(header) == dask.dataframe.core.Series:
            header = header.compute()

        header = header.loc[keys]
        
        newheader = fits.Header(header.loc[keys].to_dict())
        newheader.set(f"NINPUTS",self.imgcollection.nimages, "num. input images")
        
        if inclinput:
            basenames = self.imgcollection.filenames
            for i, basename_ in enumerate(basenames):
                newheader.set(f"INPUT{i:02d}",basename_, "input image")
            
        return newheader
    
    # ============== #
    #  Properties    # 
    # ============== #
    @property
    def imgcollection(self):
        """  """
        if not hasattr(self, "_imgcollection"):
            return None
        
        return self._imgcollection
    
    @property
    def data(self):
        """ """
        if not hasattr(self, "_data"):
            return None
        
        return self._data

    def has_data(self):
        """ """
        return self.data is not None
    
    @property
    def header(self):
        """ """
        if not hasattr(self, "_header"):
            return None
        
        return self._header
    
    def has_header(self):
        """ """
        return self.header is not None
