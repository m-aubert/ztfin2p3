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

    def to_fits(self, fileout, header=None, overwrite=True, **kwargs):
        """ Store the data in fits format """
        return self._to_fits(fileout, self.data, header=self.header,
                                 overwrite=overwrite,
                                 **kwargs)

    @staticmethod
    def _to_fits(fileout, data, header=None,  overwrite=True,
                     **kwargs):
        """ """
        import os        
        from astropy.io import fits
        dirout = os.path.dirname(fileout)
        if not os.path.isdir(dirout):
            os.makedirs(dirout, exist_ok=True)

        fits.writeto(fileout, data, header=header,
                         overwrite=overwrite, **kwargs)
        return fileout
        
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
    def build_and_store(self, fileout, overwrite=True, 
                        corr_nl=True, corr_overscan=True,
                        set_it=False, inclheader=True, **kwargs):
        """ """
        data, header = self.build(corr_nl=corr_nl, corr_overscan=corr_overscan,
                                 set_it=False, inclheader=True, **kwargs)
        
        if "dask" in str(type(data)): # is a dask object
            import dask
            to_fits = dask.delayed(self._to_fits)
        else:
            to_fits = self._to_fits
            
        return to_fits(fileout, data, header=header, overwrite=overwrite)
    
    def build(self, corr_nl=True, corr_overscan=True,
                  set_it=False, inclheader=True,
                  dask_on_header=False, **kwargs):
        """ **kwargs goes to imgcollection.get_data_mean """
        # This could be updated in the calibration function #
        
        prop = {**dict(corr_overscan=corr_overscan, corr_nl=corr_nl),
                **kwargs}
        data = self.imgcollection.get_data_mean(**prop)
        if inclheader:
            header = self.build_header(use_dask=dask_on_header)
        else:
            header = None
            
        if set_it:
            self.set_data(data)
            self.set_header(header)
            
        return data, header
    
    def build_header(self, keys=None, refid=0, inclinput=False, use_dask=None):
        """ """
        import copy
        from astropy.io import fits
        header = self.imgcollection.get_singleheader(refid, as_serie=False, 
                                                    use_dask=use_dask)
        if "dask" in str(type(header)):
            print("used dasked so compute")
            header = header.compute()
            
        if keys is not None:
            newheader = header.__class__([ copy.copy(header.cards[k]) for k in np.atleast_1d(keys)])
        else:
            newheader = copy.copy(header)
            
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

