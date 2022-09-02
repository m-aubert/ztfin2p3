
from .basepipe import BasePipe
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
        
    
    def run_perday(self, **kwargs):
        """ """
        datafile = self.datafile.groupby(["day","ccdid","ledid"])["filepath"].apply(list).reset_index()
        
        files_out = []
        for i_, s_ in datafile.iterrows():
            filesint = s_["filepath"]
            filepathout = io.get_daily_flatfile(s_["day"],ccdid=s_["ccdid"], ledid=s_["ledid"])
            fbuilder = FlatBuilder.from_rawfiles(filesint)
            fileout_ = fbuilder.build_and_store(filepathout, **kwargs)
            files_out.append(fileout_)
        
        return files_out


        
    def run(self, use_dask=True):
        """ """
        #
        # For N days in the period
        #
        # --------
        # Step 1.
        # build from per day, per led and per ccd
        #    = N x 11 x 16 flats
        #    --> N x 11 x 64 (stored per quadrant)
        daily_outputs = self.run_perday()

        # --------        
        # Step 2.        
        # merge flat per period, per led and per ccd
        #    = 11 x 16 flats x 2 (i.e. per norm)
        #  - normed per CCD 
        #  - normed per focal plane
        #    --> 11 x 64 x 2 stored (per quadrant)
        ledccd_outputs = daily_outputs.groupby(["ccdid","ledid"])["path_dailyflat"].apply(list)
        
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

    


from ..builder import CalibrationBuilder
class FlatBuilder( CalibrationBuilder ):
    
    # -------- # 
    # BUILDER  #
    # -------- #
    def build_header(self, keys=None, refid=0, inclinput=False, use_dask=None):
        """ """
        from astropy.io import fits

        if keys is None:
            keys = ["ORIGIN","OBSERVER","INSTRUME","IMGTYPE","EXPTIME",
                    "CCDSUM","CCD_ID","CCDNAME","PIXSCALE","PIXSCALX","PIXSCALY",
                    "FRAMENUM","ILUM_LED", "ILUMWAVE", "PROGRMID","FILTERID",
                    "FILTER","FILTPOS","RA","DEC", "OBSERVAT"]

        return super().build_header(keys=keys, refid=refid, inclinput=inclinput, use_dask=use_dask)
    

    
