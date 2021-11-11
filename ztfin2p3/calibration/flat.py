
""" library to build the ztfin2p3 pipeline screen flats """
import os
import pandas
from .io import FLAT_DIR 

class RawFlatMeta( object ):
    """ 
    Access the yearly IRSA metadata associated to the raw flat data.
    
    Usage:
    ------
    ```
    data = RawFlatMeta.get_yearly_metadata(2020)
    ```
    This will download the metadata if not already stored locally 
    and return the corresponding dataframe.
    If this needs to download the data it may take couple of minutes.
    Data stored as parquet.

    """
    # ============== #
    #  Core Methods  #
    # ============== #
    @classmethod
    def get_yearly_metadata(cls, year, force_dl=False, **kwargs):
        """ """
        filepath = cls.get_yearly_metadatafile(year)
        if force_dl or not os.path.isfile(filepath):
            cls.build_yearly_metadata(year)

        return pandas.read_parquet(filepath, **kwargs)

    # ============== #
    #  INTERNAL      #
    # ============== #
    @staticmethod
    def get_yearly_metadatafile(year):
        """ """
        return os.path.join(FLAT_DIR, "meta", f"rawflat_metadata{year:04d}.parquet")

    @classmethod
    def build_yearly_metadata(cls, year):
        """ """
        from astropy import time
        from ztfquery import query
        zquery = query.ZTFQuery()

        start = time.Time(f"{year}-01-01")
        end = time.Time(f"{year}-12-31")

        zquery.load_metadata("raw", sql_query=f"obsjd between {start.jd} and {end.jd} and imgtypecode = 'f'")
        if len(zquery.data)>10:
            zquery.data.to_parquet(cls.get_yearly_metadatafile(year))
            
        return 
    

        


    
class FlatBuilder( object ):

    @classmethod
    def from_rawfiles(self, rawfiles):
        """ """
        
    
    
        
