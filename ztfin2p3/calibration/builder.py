""" Top level calibration builder class """


class CalibrationBuilder( object ): # /day /week /month

    def __init__(self, rawcollection):
        """ """
        self.set_imgcollection( rawcollection )

    
    # ============== #
    #  I/O           # 
    # ============== #
    @classmethod
    def from_rawfiles(cls, rawfiles, **kwargs):
        """ """
        from ztfimg import raw
        flatcollection = raw.RawCCDCollection.from_filenames(rawfiles, **kwargs)
        return cls(flatcollection)

    def to_fits(self, fileout, header=None, overwrite=True):
        """ Store the data in fits format """
        import os        
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
        # This could be updated in the calibration function #
        
        prop = {**dict(corr_overscan=corr_overscan, corr_nl=corr_nl, clipping=clipping),
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
                    "FRAMENUM",
                    #"ILUM_LED", "ILUMWAVE",
                    "PROGRMID","FILTERID",
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

