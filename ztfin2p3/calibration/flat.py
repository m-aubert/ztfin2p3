
""" library to build the ztfin2p3 pipeline screen flats """
import os
import pandas
import numpy as np
from ..io import FLAT_DIR 

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

    @classmethod
    def get_metadata(cls, date, format=None):
        """ General method to access the IRSA metadata given a date or a daterange. 

        The format of date is very flexible to quickly get what you need:

        Parameters
        ----------
        date: [string (or list of)]
            date can either be a single string or a list of two dates in isoformat.
            - two dates format: date=['start','end'] is isoformat
              e.g. date=['2019-03-14','2019-03-25']
            
            - single string: four format are then accepted, year, month, week or day:
                - yyyy: get the full year. (string of length 4)
                       e.g. date='2019'
                - yyyymm: get the full month (string of length 6)
                       e.g. date='201903'
                - yyyywww: get the corresponding week of the year (string of length 7)
                       e.g. date='2019045'  
                - yyyymmdd: get the given single day (string of length 8)
                       e.g. date='20190227'
            
        Returns
        -------
        dataframe (IRSA metadata)
        """
        if date is None:
            raise ValueError("date cannot be None, could be string, float, or list of 2 strings")
        if not hasattr(date, "__iter__"): # int/float given, convert to string
            date = str(date)
            
        if type(date) is str and len(date) == 4:
            return cls.get_yearly_metadata(date)
        
        elif type(date) is str:
            from ..utils.tools import parse_singledate
            start, end = parse_singledate(date) # -> start, end
        else:
            from astropy import time 
            start, end = time.Time(date, format=format).datetime
        # 
        # Now we have start and end in datetime format.
        if start.year == end.year:
            data = cls.get_yearly_metadata(start.year)
        else:
            delta_year = end.year-start.year
            data = pandas.concat([cls.get_yearly_metadata(start.year+i) for i in range(delta_year+1)])
                
        datecol = data["obsdate"].astype('datetime64')
        return data[datecol.between(start.isoformat(), end.isoformat())]
    
                
    @classmethod
    def get_zquery(cls, date, force_dl=False):
        """ """
        from ztfquery import query
        import datetime

        if date is None:
            raise ValueError("date cannot be None, could be string, float, or list of 2 strings")
        if not hasattr(date, "__iter__"): # int/float given, convert to string
            date = str(date)
            
        if type(date) is str:
            if len(date) == 4:
                data = cls.get_yearly_metadata(date)
            else:
                from ..utils.tools import parse_singledate
                
                start, end = parse_singledate(date)
                if start.year == end.year:
                    data = cls.get_yearly_metadata(start.year)
                else:
                    delta_year = end.year-start.year
                    data = pandas.concat([cls.get_yearly_metadata(start.year+i) for i in range(delta_year+1)])
                
                datecol = data["obsdate"].astype('datetime64')
                data = data[datecol.between(start.isoformat(), end.isoformat())]
        
                
                
        
        

        


        
        data = cls.get_yearly_metadata(year, force_dl=force_dl)
        # - DateRange
        if daterange is not None:
            dstart, dend = daterange
            if dstart is not None and dend is not None:
                data = data[data["obsdate"].between(dstart,dend)]
            elif dstart is not None:
                data = data[data["obsdate"]>=dstart]
            elif dend is not None:
                data = data[data["obsdate"]<=dend]
            else:
                # both dstart and dend are None
                pass

        return query.ZTFQuery(data, "raw")
        

    @classmethod
    def get_rawflatfile(cls, year, ccdid):
        """ """
        zquery = query.ZTFQuery(data, "raw")
        
        indexes_ccds = zquery.data[zquery.data["ccdid"].isin( np.atleast_1d(ccdid) ) ].index
        files_to_dl = [l.split("/")[-1] for l in zquery.get_data_path(indexes=indexes_ccds)]
        future_files = io.bulk_get_file(files_to_dl, client=client, as_dask="futures")
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
        
    
    
        
