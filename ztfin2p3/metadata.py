""" Handling metadata """

import os
import numpy as np
import pandas
import warnings
from .utils.tools import parse_singledate


__all__ = ["get_raw"]

def get_rawfile(which, date, ccdid=None, fid=None,
                **kwargs):
    """ shortcut to get_raw(what='file',) """
    return get_raw(which, date, ccdid=ccdid, fid=fid,
                   what='file', **kwargs)

def get_rawmeta(which, date, ccdid=None, fid=None,
                **kwargs):
    """ shortcut to get_raw(what='metadata',) """
    return get_raw(which, date, ccdid=ccdid, fid=fid,
                   what='metadata', **kwargs)

def get_rawzquery(which, date, ccdid=None, fid=None,
                **kwargs):
    """ shortcut to get_raw(what='zquery') """
    return get_raw(which, date, ccdid=ccdid, fid=fid,
                   what='zquery', **kwargs)

def get_raw(which, date, what, ccdid=None, fid=None,
                **kwargs):
    """ 
    which: [string]
        - flat
        - bias
        - starflat [not implemented yet]
        - science
        
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
    what: [string] -optional-
        what do you want to get. get_{what} must be an existing
        method. You have for instance:
        - file
        - metadata/meta
        - zquery


    ccdid, fid: [int or list of] -optional-
        value or list of ccd (ccdid=[1->16]) or filter (fid=[1->3]) you want
        to limit to.

        
    **kwargs goes to get_metadata()
       option examples:
       - which='flat': 
           - ledid
       - which='science':
           - field

       - what = 'file':
           - client, 
           - as_dask

    Returns
    -------
    list

    Example:
    --------
    #
    # - Flat (with LEDID)
    #
    Get the rawflat image file of ledid #2 for the 23th week of
    2020. Limit this to ccd #4
    files = get_rawfile('flat', '2020023', ledid=2, ccdid=4)

    """
    prop = dict(ccdid=ccdid, fid=fid)
    method = f"get_{what}"

    if which == "flat":
        class_ = RawFlatMetaData
    elif which == "bias":
        class_ = RawBiasMetaData
    elif which in ["object","science"]:
        class_ = RawScienceMetaData
    else:
        raise NotImplementedError(f"which = {which} has not been")

    return getattr(class_, method)(date, **{**prop, **kwargs})


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
    def get_filepath(cls, date, in_meta=False, **kwargs):
        """ get the local path where the data are """
        from ztfquery.query import metatable_to_url
        metadata = cls.get_metadata(date, **kwargs)
        filepath = metatable_to_url(metadata, source="local")
        if in_meta:
            metadata["filepath"] = filepath
            return metadata
        
        return filepath
    
    @classmethod
    def get_file(cls, date, getpath=False, client=None, as_dask="futures", **kwargs):
        """ get the file associated to the input metadata limits. 

        **kwargs goes to get_metadata, it contains selection options like ccdid or fid.
        """
        from ztfquery import io
        files = cls.get_filepath(date, **kwargs)
        return io.bulk_get_file(files, client=client, as_dask=as_dask)
        
    @classmethod
    def get_metadata(cls, date, ccdid=None, fid=None):
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
            
        ccdid, fid: [int or list of]
            value or list of ccd (ccdid=[1->16]) or filter (fid=[1->3]) you want
            to limit to.

        Returns
        -------
        dataframe (IRSA metadata)
        """
        #          #
        #  Date    #
        #          #
        if date is None:
            raise ValueError("date cannot be None, could be string, float, or list of 2 strings")
        if not hasattr(date, "__iter__"): # int/float given, convert to string
            date = str(date)

        if type(date) is str and len(date) == 6: # means per month as stored.
            return cls.get_monthly_metadata(date[:4],date[4:])
        elif type(date) is str:
            start, end = parse_singledate(date) # -> start, end
        else:
            from astropy import time 
            start, end = time.Time(date).datetime

        months = cls._daterange_to_monthlist_(start, end)
        data = pandas.concat([cls.get_monthly_metadata(yyyy,mm) for yyyy,mm in months])
        
        datecol = data["obsdate"].astype('datetime64')
        data = data[datecol.between(start.isoformat(), end.isoformat())]
        #          #
        #  CCDID   #
        #          #
        if ccdid is not None:
            data = data[data["ccdid"].isin(np.atleast_1d(ccdid))]
        #          #
        #  FID     #
        #          #
        if fid is not None:
            data = data[data["fid"].isin(np.atleast_1d(fid))]

        return data
    
    @classmethod
    def get_zquery(cls, date, force_dl=False, **kwargs):
        """ get the ZTFQuery object associated to the metadata
        corresponding to the input date. 

        **kwargs goes to get_metadata() like ccdid, fid 
        
        """
        from ztfquery import query
        data = cls.get_metadata(date, **kwargs)
        return query.ZTFQuery(data, cls._KIND)
    
    @classmethod
    def bulk_build_metadata(cls, date, client=None, as_dask="delayed", force_dl=False, format=None):
        """ uses Dask to massively download metadata in the given time range.
        Data will be stored using the usual monthly based format. 

        Example:
        --------
        To run the parallel downloading between May-12th of 2019 and June-3rd of 2020:
        filesout = bulk_build_metadata(['2019-05-12','2020-06-03'], as_dask='computed')
        
        """
        import dask
        #
        # - Test Dask input
        if client is None:
            if as_dask == "futures":
                raise ValueError("Cannot as_dask=futures with client is None.")
            if as_dask in ["gather","gathered"]:
                as_dask = "computed"
        # end test dask input
        #

        if not hasattr(date, "__iter__"): # int/float given, convert to string
            date = str(date)

        if type(date) is str and len(date) == 6: # means per month as stored.
            return cls.get_monthly_metadata(date[:4],date[4:])
        elif type(date) is str:
            start, end = parse_singledate(date) # -> start, end
        else:
            from astropy import time 
            start, end = time.Time(date, format=format).datetime

        months = cls._daterange_to_monthlist_(start, end)
        delayed_data = [dask.delayed(cls._load_or_download_)(yyyy,mm, force_dl=force_dl)
                    for yyyy,mm in months]

        # Returns
        if as_dask == "delayed":
            return delayed_data
        if as_dask in ["compute","computed"]:
            return dask.delayed(list)(delayed_data).compute()
        
        if as_dask == "futures": # client has been tested already
            return client.compute(delayed_data)
        
        if as_dask in ["gather","gathered"]:
            return client.gather(client.compute(delayed_data))
        
        raise ValueError("Cannot parse the given as_dask")
        
    @classmethod
    def get_monthly_metadatafile(cls, year, month):
        """ """
        from .io import get_directory
        year, month = int(year), int(month)
        if cls._KIND is None or cls._SUBKIND is None:
            raise AttributeError(f"_KIND {cls._KIND} or _SUBKIND {cls._SUBKIND} is None. Please define them")

        directory = get_directory(cls._KIND, cls._SUBKIND)
        return os.path.join(directory, "meta", f"{cls._KIND}{cls._SUBKIND}_metadata_{year:04d}{month:02d}.parquet")

    @classmethod
    def _load_or_download_(cls, year, month, force_dl=False, **kwargs):
        """ """
        filepath = cls.get_monthly_metadatafile(year, month)
        if force_dl or not os.path.isfile(filepath):
            filepath = cls.build_monthly_metadata(year, month)
            
        return filepath
        
    @classmethod
    def get_monthly_metadata(cls, year, month, force_dl=False, **kwargs):
        """ """
        filepath = cls._load_or_download_(year, month, force_dl=force_dl)
        return pandas.read_parquet(filepath, **kwargs)
    

    # --------------- #
    #  INTERNAL       #
    # --------------- #
    @staticmethod
    def _daterange_to_monthlist_(start, end):
        """ """
        # 
        # Now we have start and end in datetime format.
        starting_month = [start.isoformat().split("-")[:2]]
        extra_months = pandas.date_range(start.isoformat(),
                                                 end.isoformat(), freq='MS'
                                                 ).strftime("%Y-%m").astype('str').str.split("-").to_list()
        # All individual months
        return np.unique(np.asarray(starting_month+extra_months, dtype="int"), axis=0)
    
class RawMetaData( MetaDataHandler ):
    _KIND = "raw"
    _SUBKIND = None
    @classmethod
    def build_monthly_metadata(cls, year, month):
        """ """
        if cls._SUBKIND is None:
            raise NotImplementedError("you must define cls._SUBKIND")
        
        year, month = int(year), int(month)
        from astropy import time
        from ztfquery import query
        fileout = cls.get_monthly_metadatafile(year, month)
        
        zquery = query.ZTFQuery()
        start, end = parse_singledate(f"{year:04d}{month:02d}")
        start = time.Time(start.isoformat())
        end = time.Time(end.isoformat())

        zquery.load_metadata("raw", sql_query=f"obsjd between {start.jd} and {end.jd} and imgtype = '{cls._SUBKIND}'")
        if len(zquery.data)>5:
            dirout = os.path.dirname(fileout)
            if not os.path.isdir(dirout):
                os.makedirs(dirout, exist_ok=True)
                
            zquery.data.to_parquet(fileout)
            
        return fileout
    
class RawFlatMetaData( RawMetaData ):
    _SUBKIND = "flat"
    # ================= #
    #   Super It        #
    # ================= #    
    @classmethod
    def get_metadata(cls, date, ccdid=None, fid=None, ledid=None):
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
            
        ccdid, fid, ledid: [int or list of]
            value or list of ccd (ccdid=[1->16]), filter (fid=[1->3]) or LED (2->13 | but 6)
            to limit to.

        Returns
        -------
        dataframe (IRSA metadata)
        """
        data = super().get_metadata(date, ccdid=ccdid, fid=fid)
        if ledid is not None:
            data = data[data["ledid"].isin(np.atleast_1d(ledid))]
            
        return data
    
    # ================= #
    #   Additional      #
    # ================= #    
    @classmethod
    def add_ledinfo_to_metadata(cls, year, month, use_dask=True, update=False):
        """ """
        year, month = int(year), int(month)
        from ztfquery import io
        from astropy.io import fits
        def getval_from_header(filename, value, ext=None, **kwargs):
            """ """
            return fits.getval(io.get_file(filename, **kwargs),  value, ext=ext)

        zquery = cls.get_zquery(f"{year:04d}{month:02d}")
        if "ledid" in zquery.data.columns:
            warnings.warn("ledid already in data. update=False so nothing to do")
            return
        
        # Only get the first filefracday index, since all filefactday have the same LED.
        filefracdays= zquery.data[zquery.data["fid"].isin([1,2,3])].groupby("filefracday").head(1)
        # Get the LEDID
        files = [l.split("/")[-1] for l in zquery.get_data_path(indexes=filefracdays.index)]
        if use_dask:
            import dask
            ilum_delayed = [dask.delayed(getval_from_header)(file_, "ILUM_LED")  for file_ in files]
            ilum = dask.delayed(list)(ilum_delayed).compute()
        else:
            ilum = [getval_from_header(file_, "ILUM_LED")  for file_ in files]

        # merge that with the initial data.            
        filefracdays.insert(len(filefracdays.columns), "ledid", ilum)
        data = zquery.data.merge(filefracdays[["filefracday","ledid"]], on="filefracday")
        # and store it back.
        fileout = cls.get_monthly_metadatafile(year, month)
        data.to_parquet(fileout)
        return
        
        
class RawBiasMetaData( RawMetaData ):
    _SUBKIND = "bias"

class RawScienceMetaData( RawMetaData ):
    _SUBKIND = "object"
    
    @classmethod
    def get_metadata(cls, date, ccdid=None, fid=None, field=None):
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
            
        ccdid, fid: [int or list of]
            value or list of ccd (ccdid=[1->16]) or filter (fid=[1->3])
            to limit to.

        field: [int or list of]
            requested (list of) field(s)
 
        Returns
        -------
        dataframe (IRSA metadata)
        """
        data = super().get_metadata(date, ccdid=ccdid, fid=fid)
        if field is not None:
            data = data[data["field"].isin(np.atleast_1d(field))]
            
        return data
