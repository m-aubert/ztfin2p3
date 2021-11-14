""" Handling metadata """

import os
import pandas
from .utils.tools import parse_singledate
            
class MetaDataHandler( object ):
    _KIND = None # IRSA kind: raw, sci, cal
    _SUBKIND = None # subkind (raw/{flat,bias,science,starflat}, etc.)
    
    # ================= #
    #   To IMPLEMENT    #
    # ================= #
    @classmethod
    def build_monthly_metadata(cls, year, month):
        """ """
        raise NotImplementedError("You must implement build_monthly_metadata()")
        
    # ================= #
    #   MetaData        #
    # ================= #
    @classmethod
    def get_monthly_metadatafile(cls, year, month):
        """ """
        from .io import get_directory
        if cls._KIND is None or cls._SUBKIND is None:
            raise AttributeError(f"_KIND {cls._KIND} or _SUBKIND {cls._SUBKIND} is None. Please define them")

        directory = get_directory(cls._KIND, cls._SUBKIND)
        return os.path.join(directory, "meta", f"{cls._KIND}{cls._SUBKIND}_metadata_{year:04d}{month:02d}.parquet")
    
    @classmethod
    def get_monthly_metadata(cls, year, month, force_dl=False, **kwargs):
        """ """
        filepath = cls.get_monthly_metadatafile(year, month)
        if force_dl or not os.path.isfile(filepath):
            cls.build_monthly_metadata(year, month)

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
            
        if type(date) is str:
            start, end = parse_singledate(date) # -> start, end
        else:
            from astropy import time 
            start, end = time.Time(date, format=format).datetime
        # 
        # Now we have start and end in datetime format.
        starting_month = "-".join(start.isoformat().split("-")[:2])
        extra_months = pandas.date_range(start.isoformat(),
                                         end.isoformat(), freq='MS'
                                        ).strftime("%Y-%m").astype('str').str.split("-").to_list()
        if len(extra_months)>0:
            months = starting_month+extra_months
            data = pandas.concat([cls.get_monthly_metadata(int(yyyy),int(mm)) for yyyy,mm in months])
        else:
            data = cls.get_monthly_metadata(int(starting_month[0]),int(starting_month[1]))
                
        datecol = data["obsdate"].astype('datetime64')
        return data[datecol.between(start.isoformat(), end.isoformat())]
    
    @classmethod
    def get_zquery(cls, date, force_dl=False):
        """ get the ZTFQuery object associated to the metadata
        corresponding to the input date. 
        """
        from ztfquery import query
        data = cls.get_metadata(date)
        return query.ZTFQuery(data, cls._KIND)
        
                
class RawFlatMetaData( MetaDataHandler ):
    _KIND = "raw"
    _SUBKIND = "flat"
    @classmethod
    def build_monthly_metadata(cls, year, month):
        """ """
        from astropy import time
        from ztfquery import query
        fileout = cls.get_monthly_metadatafile(year, month)
        
        zquery = query.ZTFQuery()
        start, end = parse_singledate(f"{year:04d}{month:02d}")
        start = time.Time(start.isoformat())
        end = time.Time(end.isoformat())

        zquery.load_metadata("raw", sql_query=f"obsjd between {start.jd} and {end.jd} and imgtypecode = 'f'")
        if len(zquery.data)>5:
            zquery.data.to_parquet(fileout)
            
        return fileout

class RawBiasMetaData( MetaDataHandler ):
    _KIND = "raw"
    _SUBKIND = "bias"
    @classmethod
    def build_monthly_metadata(cls, year, month):
        """ """
        from astropy import time
        from ztfquery import query
        fileout = cls.get_monthly_metadatafile(year, month)
        
        zquery = query.ZTFQuery()
        start, end = parse_singledate(f"{year:04d}{month:02d}")
        start = time.Time(start.isoformat())
        end = time.Time(end.isoformat())

        zquery.load_metadata("raw", sql_query=f"obsjd between {start.jd} and {end.jd} and imgtypecode = 'b'")
        if len(zquery.data)>5:
            zquery.data.to_parquet(fileout)
            
        return fileout

    
    
