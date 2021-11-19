
""" library to build the ztfin2p3 pipeline screen flats """
import os
import pandas
import numpy as np
import dask


def bulk_buildflat(dates, ccdid, filtername, ledid=None):
    """ """
    dates = np.atleast_1d(dates)

    prop = dict(ccdid=ccdid, filtername=filtername, ledid=ledid)
    outputs = []
    for date in dates:
        if len(date)==7:
            outputs.append( build_weeklyflat( date[:4],date[4:], **prop) )
        elif len(date)==8:
            outputs.append( build_dailyflat( date[:4],date[4:6],date[6:], **prop) )
        else:
            warnings.warn(f"Cannot parse date={date} ; yyyywww or yyyymmdd | ignored.")
            
    return outputs


def build_dailyflat(year, month, day, ccdid, filtername, ledid=None,
                    delay_store=False,
                   **kwargs):
    """ """
    from ..io import get_rawfile, get_daily_flatfile
    year, month, day = int(year), int(month), int(day)
    files = get_rawfile("flat", f"{year:04d}{month:02d}{day:02d}", ccdid=ccdid, ledid=ledid)
    fileout = get_daily_flatfile(year, month, day, ccdid, filtername, ledid=ledid)
    # Actual builds
    return buildflat_from_files(files, fileout,
                                    delay_store=delay_store, **kwargs)

def build_weeklyflat(year, week, ccdid, filtername, ledid=None,
                    delay_store=False,
                   **kwargs):
    """ """
    from ..io import get_rawfile, get_daily_flatfileget_weekly_flatfile
    year, week = int(year), int(week)
    files = get_rawfile("flat", f"{year:04d}{week:03d}", ccdid=ccdid, ledid=ledid)
    fileout = get_weekly_flatfile(year, week, ccdid, filtername, ledid=ledid)
    # Actual builds
    return buildflat_from_files(files, fileout,
                                    delay_store=delay_store, **kwargs)

def buildflat_from_files(files, fileout, delay_store=False, **kwargs):
    """ """
    bflat = FlatBuilder.from_rawfiles(files)
    data, header = bflat.build(**kwargs)
    #
    # - dir if possible
    dirout = os.path.dirname(fileout)
    if not os.path.isdir(dirout):
        os.makedirs(dirout, exist_ok=True)

    if delay_store:
        return dask.delayed(fits.writeto)(fileout, data, header=header,
                                              overwrite=True)
    return fits.writeto(fileout, data, header=header, overwrite=True)
    
    

    
    

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
                  set_it=False, **kwargs):
        """ """
        prop = {**dict(corr_overscan=corr_overscan, corr_nl=corr_nl, clipping=True),
                **kwargs}
        data = self.imgcollection.get_data_mean(**prop)
        header = self.build_header()
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
