""" Handling metadata """

import os
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

        if cls._KIND is None or cls._SUBKIND is None:
            raise AttributeError(f"_KIND {cls._KIND} or _SUBKIND {cls._SUBKIND} is None. Please define them")

        directory = get_directory(cls._KIND, cls._SUBKIND)
        return os.path.join(directory, "meta", f"{cls._KIND}{cls._SUBKIND}_metadata_{year:04d}{month:02d}.parquet")
    
    @classmethod
    def get_monthly_metadata(cls, year, month, force_dl=False):
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
            
        if type(date) is str and len(date) == 4:
            return cls.get_yearly_metadata(date)
        
        elif type(date) is str:
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
        zquery = query.ZTFQuery()
        start, end = parse_singledate(f"{year:04d}{month:02d}")
        start = time.Time(start)
        end = time.Time(end)

        zquery.load_metadata("raw", sql_query=f"obsjd between {start.jd} and {end.jd} and imgtypecode = 'f'")
        if len(zquery.data)>10:
            zquery.data.to_parquet(cls.get_yearly_metadatafile(year))
            
        return 

    
