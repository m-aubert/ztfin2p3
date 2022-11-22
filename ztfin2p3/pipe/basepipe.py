


class BasePipe( object ):
    _KIND = "_base_"
    
    def __init__(self, use_dask=True):
        """ """
        self._use_dask = use_dask

    # ============== #
    #   Methods      #
    # ============== #
    def set_config(self, config):
        """ """
        return self._config

    def set_datafile(self, datafile):
        """ """
        self._datafile = datafile
    
    # ============== #
    #   Parameters   #
    # ============== #
    @property
    def config(self):
        """ configuration to use for the pipeline """
        return self._config

    @property
    def datafile(self):
        """ dataframe of files use to build the flats """
        return self._datafile
    
    @property
    def use_dask(self):
        """ shall this pipeline use dask (you have delayed object) """
        return self._use_dask
    
    @property
    def pipekind(self):
        """ what kind of pipeline is this object ? """
        return self._KIND



class CalibPipe( BasePipe ):
    _KIND = None
    
    def __init__(self, period, use_dask=True):
        """ """
        super().__init__(use_dask=use_dask) # __init__ of BasePipe
        self._period = period
        
    @classmethod
    def from_period(cls, start, end, use_dask=True, **kwargs):
        """ 
        
        start, end: 
            the period concerned by this flat. 
            format: yyyy-mm-dd
            
        """
        this = cls([start, end], use_dask=use_dask)
        # Load the associated metadata
        this.load_metadata(**kwargs)
        return this
    
    # ============== #
    #   Methods      #
    # ============== #
    def load_metadata(self, period=None, **kwargs):
        """ """
        from ztfin2p3 import metadata        
        if period is None and self._period is None:
            raise ValueError("no period given and none known")
        datafile = metadata.get_rawmeta(self._KIND, self.period, add_filepath=True, **kwargs)
        self.set_datafile(datafile) 
        
    
    def get_init_datafile(self):
        """ """
        return self.datafile.groupby(["day","ccdid"])["filepath"].apply(list).reset_index()
    
    def run_perday(self, datafile=None, raw=True, **kwargs):
        """ """
        datafile = datafile.copy()
        header_keys = ["ORIGIN","OBSERVER","INSTRUME","IMGTYPE","EXPTIME",
                           "CCDSUM","CCD_ID","CCDNAME","PIXSCALE","PIXSCALX","PIXSCALY",
                           "FRAMENUM", "PROGRMID","FILTERID",
                           "FILTER","FILTPOS","RA","DEC", "OBSERVAT"]
        if self._KIND == "flat":
            header_keys += ["ILUM_LED", "ILUMWAVE"]
            
        
        
        if datafile is None:
            datafile = self.get_init_datafile()
        
        files_out = []
        for i_, s_ in datafile.iterrows():
            # loop over entires (per led, per day per CCD)
            # - raw files in
            filesint = s_["filepath"] 
            # - where to store
            if self._KIND == "flat":
                prop =  dict(ccdid=s_["ccdid"], ledid=s_["ledid"])
            else:
                prop =  dict(ccdid=s_["ccdid"])

            filepathout = getattr(io,f"get_daily_{self.KIND}file")(s_["day"], **prop)
            
                
            # - loads the builder for these files in
            fbuilder = CalibrationBuilder.from_filenames(filesint, raw=raw,
                                                             as_path=False,
                                                             persist=False)
            # - build the merged image and store it, returning the storing path
            fileout_ = fbuilder.build_and_store(filepathout, incl_header=True, 
                                                header_keys=header_keys, **kwargs)
            # - append the storing path
            files_out.append(fileout_)
        
        datafile["path_daily"] = files_out
        return datafile
    
    def merge_daily(self, daily_datafile, **kwargs):
        """ """
        if self._KIND == "flat": # incl LED
            datafile = daily_datafile.groupby(["ccdid","ledid"]
                                             )["path_daily"].apply(list).reset_index()
        else:
            datafile = daily_datafile.groupby("ccdid")["path_daily"].apply(list).reset_index()
        
        files_out = []
        for i_, s_ in datafile.iterrows():
            # loop over entires (per led, per day per CCD)
            filesint = s_["path_daily"] # raw files in
            if self._KIND == "flat":
                prop =  dict(ccdid=s_["ccdid"], ledid=s_["ledid"])
            else:
                prop =  dict(ccdid=s_["ccdid"])
                
            filepathout = io.get_period_flatfile(*self.period, **prop) # where to store
            fbuilder = CalibrationBuilder.from_filenames(filesint, as_path=True,
                                                             persist=False, raw=False) # loads the builder
            fileout_ = fbuilder.build_and_store(filepathout, incl_header=False,  # header not ready
                                                **kwargs) # build and store | but delayed
            files_out.append(fileout_)
        
        datafile["path_period"] = files_out
        return datafile

        
        
    def run(self, use_dask=True):
        """ """
        #
        # For N days in the period
        #
        datafile = self.get_init_datafile()
        # --------
        # Step 1.
        # build from per day, per led and per ccd
        #    = N x 11 x 16 flats
        #    --> N x 11 x 16
        daily_outputs = self.run_perday(datafile)

        # --------        
        # Step 2.        
        # merge flat per period, per led and per ccd
        #    = 11 x 16 flats x 2 (i.e. per norm)
        #  - normed per CCD 
        #  - normed per focal plane
        #    --> 11 x 16 x 2 stored (per quadrant)
        periodled_outputs = self.merge_daily(daily_outputs)
        return periodled_outputs
        
        # --------        
        # Step 3.        
        # merge flat per period, per led and per ccd
        #    = 11 x 16 flats x 2 (i.e. per norm)
        #  - normed per CCD 
        #  - normed per focal plane
        #    --> 11 x 64 x 2 stored (per quadrant)
        
        
        # --------
        # Step 3.        
        # merge led per filter
        #    = 3 * 16 flats x 2 (i.e. per norm)
        #  - per ccd
        #  - per focal plane
        #    --> 3 x 64 (stored per quadrant)
        
    # ============== #
    #  Property      #
    # ============== #
    @property
    def period(self):
        """ """
        if not hasattr(self, "_period"):
            return None
        
        return self._period
