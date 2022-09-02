
from .basepipe import BasePipe
from ..builder import CalibrationBuilder
from .. import io
# BasePipe has
#     - config and co.
#     - datafile and co.
#     - use_dask

class FlatPipe( BasePipe ):
    _KIND = "flat"
    
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
        return self.datafile.groupby(["day","ccdid","ledid"])["filepath"].apply(list).reset_index()
    
    def run_perday(self, datafile=None, **kwargs):
        """ """
        header_keys = ["ORIGIN","OBSERVER","INSTRUME","IMGTYPE","EXPTIME",
                       "CCDSUM","CCD_ID","CCDNAME","PIXSCALE","PIXSCALX","PIXSCALY",
                       "FRAMENUM","ILUM_LED", "ILUMWAVE", "PROGRMID","FILTERID",
                       "FILTER","FILTPOS","RA","DEC", "OBSERVAT"]

        
        
        if datafile is None:
            datafile = self.get_init_datafile()
        
        files_out = []
        for i_, s_ in datafile.iterrows():
            # loop over entires (per led, per day per CCD)
            # - raw files in
            filesint = s_["filepath"] 
            # - where to store
            filepathout = io.get_daily_flatfile(s_["day"],ccdid=s_["ccdid"], ledid=s_["ledid"]) 
            # - loads the builder for these files in
            fbuilder = CalibrationBuilder.from_rawfiles(filesint, as_path=False, persist=False)
            # - build the merged image and store it, returning the storing path
            fileout_ = fbuilder.build_and_store(filepathout, incl_header=True, 
                                                header_keys=header_keys, **kwargs)
            # - append the storing path
            files_out.append(fileout_)
        
        datafile["path_dailyflat"] = files_out
        return datafile
    
    def merge_dailyflats(self, daily_datafile, **kwargs):
        """ """
        datafile = daily_datafile.groupby(["ccdid","ledid"])["path_dailyflat"].apply(list).reset_index()
        
        files_out = []
        for i_, s_ in datafile.iterrows():
            # loop over entires (per led, per day per CCD)
            filesint = s_["path_dailyflat"] # raw files in
            filepathout = io.get_period_flatfile(*self.period, ccdid=s_["ccdid"], ledid=s_["ledid"]) # where to store
            fbuilder = CalibrationBuilder.from_rawfiles(filesint, as_path=True, persist=False) # loads the builder
            fileout_ = fbuilder.build_and_store(filepathout, incl_header=False,  # header not ready
                                                **kwargs) # build and store | but delayed
            files_out.append(fileout_)
        
        datafile["path_periodledflat"] = files_out
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
        periodled_outputs = self.merge_dailyflats(daily_outputs)
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
