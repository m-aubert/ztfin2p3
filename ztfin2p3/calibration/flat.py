
""" library to build the ztfin2p3 pipeline screen flats """
import os
import pandas
import numpy as np



def build_dailyflat(year, month, day, ccdid, filtername, ledid=None, 
                   **kwargs):
    """ """
    from ..io import get_rawfile, get_daily_flatfile
    year, month, day = int(year), int(month), int(day)
    files = get_rawfile("flat", f"{year:04d}{month:02d}{day:02d}", ccdid=ccdid, ledid=ledid)
    fileout = get_daily_flatfile(year, month, day, ccdid, filtername, ledid=ledid)
    # Actual builds
    return buildflat_from_files(files, fileout, **kwargs)

def build_weeklyflat(year, week, ccdid, filtername, ledid=None, 
                   **kwargs):
    """ """
    from ..io import get_rawfile, get_daily_flatfileget_weekly_flatfile
    year, week = int(year), int(week)
    files = get_rawfile("flat", f"{year:04d}{week:03d}", ccdid=ccdid, ledid=ledid)
    fileout = get_weekly_flatfile(year, week, ccdid, filtername, ledid=ledid)
    # Actual builds
    return buildflat_from_files(files, fileout, **kwargs)

def buildflat_from_files(files, fileout, **kwargs):
    """ """
    flat = FlatBuilder.from_rawfiles(files)
    flat.build(**kwargs)
    flat.to_fits(fileout)

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

    def to_fits(self, fileout, overwrite=True):
        """ Store the data in fits format """

        from astropy.io.fits import HDUList, PrimaryHDU
        fitsheader = self.build_header()
        
        hdul = []
        # -- Data saving
        hdul.append( PrimaryHDU(self.data, fitsheader) )            
        hdulist = HDUList(hdul)
        hdulist.writeto(fileout, overwrite=overwrite)
        
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
        
    # -------- # 
    # BUILDER  #
    # -------- #
    def build(self, corr_nl=True, corr_overscan=True, clipping=True, set_it=False, **kwargs):
        """ """
        prop = {**dict(corr_overscan=corr_overscan, corr_nl=corr_nl, clipping=True),
                **kwargs}
        data = self.imgcollection.get_data_mean(**prop)
        self.set_data(data)
        return data

    def build_header(self, keys=None, refid=0):
        """ """
        from astropy.io import fits

        if keys is None:
            keys = ["ORIGIN","OBSERVER","INSTRUME","IMGTYPE","EXPTIME",
                    "CCDSUM","CCD_ID","CCDNAME","PIXSCALE","PIXSCALX","PIXSCALY",
                    "FRAMENUM","ILUM_LED", "ILUMWAVE", "PROGRMID","FILTERID",
                    "FILTER","FILTPOS","RA","DEC", "OBSERVAT"]
                
        header = self.imgcollection.images[refid].header.compute()
        newheader = fits.Header()
        for k_ in keys:
            newheader.set(k_, header.get(k_,""), header.comments[k_])
            
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
    
