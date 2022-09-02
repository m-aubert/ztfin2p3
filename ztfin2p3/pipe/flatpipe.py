

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
        #    --> N x 11 x 64 (stored per quadrant)


        
        # --------        
        # Step 2.        
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
    #  DataFile      #
    # ============== #
    
