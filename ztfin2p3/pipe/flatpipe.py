

from .basepipe import BasePipe

# BasePipe has
#     - config and co.
#     - datafile and co.
#     - use_dask

class FlatPipe( BasePipe ):


    @classmethod
    def from_period(cls, config):
        """ """
        
        
    def run(self, use_dask=True):
        """ """
        #
        # For N days in the period
        #
        # --------
        # Step 1.
        # build from per day, per led and per ccd
        #    = N x 11 x 16 flats
        #    --> N x 11 x 16


        
        # --------        
        # Step 2.        
        # merge flat per period, per led and per ccd
        #    = 11 x 16 flats x 2 (i.e. per norm)
        #  - normed per CCD 
        #  - normed per focal plane
        #    --> 11 x 16 x 2 stored 
        
        
        # --------
        # Step 3.        
        # merge led per filter
        #    = 3 * 16 flats x 2 (i.e. per norm)
        #  - per ccd
        #  - per focal plane
        #    --> 3 x 16 x2 
        
    # ============== #
    #  DataFile      #
    # ============== #
    


from ..builder import CalibrationBuilder

class FlatBuilder( CalibrationBuilder ):
    
    # -------- # 
    # BUILDER  #
    # -------- #
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
    
