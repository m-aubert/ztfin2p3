from ..builder import CalibrationBuilder
from .. import io


import numpy as np
import dask.array as da

import ztfimg

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
    def get_fileout(self, ccdid, **kwargs):
        """ get the file to be written to store the period ccd product
        (see get_ccd)

        Parameters
        ----------
        ccdid: int
            id of the ccd (1->16)

        **kwargs goes to io.get_period_{kind}file

        Returns
        -------
        str
            fullpath

        See also
        --------
        get_ccd: get a ztfimg.ccd object for the given ccdid(s).
        """
        if self.pipekind == "bias":
            fileout = io.get_period_biasfile(*self.period, ccdid=ccdid)
        elif self.pipkind == "flat":
            fileout = io.get_period_flatfile(*self.period, ccdid=ccdid, **kwargs)
        else:
            raise NotImplementedError(f"only bias and flat kinds implemented ; this is {self.pipekind}")

        return fileout
    
    # ----------------- #
    #  High-Level build #
    # ----------------- #
    def get_ccd(self, ccdid=None, mergestats="mean", **kwargs):
        """ """
        # list of stacked CCD array Nx6000x6000
        ccdids, stacked_ccds = self.get_stacked_ccdarray(ccdid=ccdid, **kwargs)
        ccds = [ztfimg.CCD.from_data( getattr(da, mergestats)(stacked_ccd_, axis=0) )
                    for stacked_ccd_ in stacked_ccds]
        
        return ccdids, ccds

    def get_focalplane(self, mergestats="mean"):
        """ """
        ccdids, ccds = self.get_ccd()
        focal_plane = ztfimg.FocalPlane(ccds=ccds, ccdids=ccdid)
        return focal_plane
    
    # ----------------- #
    #  Mid-Level build  #
    # ----------------- #        
    def get_stacked_ccdarray(self, ccdid=None):
        """ """
        datalist = self.init_datafile.reset_index().groupby("ccdid")["index"].apply(list)
        
        if ccdid is not None:
            datalist = datalist.loc[np.atleast_1d(ccdid)]
        
        darray_ = self._ccddata_from_datalist_(datalist)
        
        return datalist.index.values, darray_


    def get_daily_ccd(self, day=None, ccdid=None):
        """ """
        datalist = self.init_datafile.copy()
        if day is not None:
            day = np.atleast_1d(day)
            datalist = datalist[datalist["day"].isin(day)]

        if ccdid is not None:
            ccdid = np.atleast_1d(ccdid)
            datalist = datalist[datalist["ccdid"].isin(ccdid)]

        # to keep the same format as the other get_functions:
        datalist = datalist.reset_index().set_index(["day","ccdid"])["index"]

        ccds = [ztfimg.CCD.from_data(self.daily_ccds[i])
                     for i in datalist.values]

        return pandas.Series(data=ccds, dtype="object",
                          index=datalist.index)
        
    def get_daily_focalplane(self, day=None):
        """ """
        ccds_df = self.get_daily_ccd(day)
        days = ccds_df.index.levels[0]
        ccdids = np.arange(1,17)
        # the follows crashes (in purpose) if there are missing ccds
        fps = [ccds_df.loc[day, ccdids].values
                  for day in days]
        return fps
        
        
    # ----------------- #
    #  Internal         #
    # ----------------- #        
    def _ccddata_from_datalist_(self, datalist, merged=None):
        """ loops over datalist rows to get the daily_ccds 

        Parameters
        ----------
        datalist: pandas.Series
            serie containing list of indexes.

        merged: None, str
            if merged is not None, it is assumed to be 
            a dask.array function used to merge the data.
            e.g. 'mean', 'median', 'std' etc.

        Returns
        -------
        list
            list of dask.array
        """
        arrays_ = [da.stack([self.daily_ccds[i] for i in list_id]) 
                    for list_id in datalist.values]

        if merged is not None:
            arrays_ = [getattr(da, merged)(a_, axis=0)
                            for a_ in arrays_]
        
        return arrays_        

    
    # ----------------- #
    #   Structural      #
    # ----------------- #
    def get_init_datafile(self):
        """ """
        groupby_ = ["day","ccdid"]
        if self.pipekind == "flat":
            groupby_ += ["ledid"]
            
        return self.datafile.groupby(groupby_)["filepath"].apply(list).reset_index()

    def load_metadata(self, period=None, **kwargs):
        """ """
        from ztfin2p3 import metadata        
        if period is None and self._period is None:
            raise ValueError("no period given and none known")
        datafile = metadata.get_rawmeta(self.pipekind, self.period, add_filepath=True, **kwargs)
        self.set_datafile(datafile) 
        
    def build_daily_ccds(self, corr_overscan=True, corr_nl=True, chunkreduction=None):
        """ """

        prop = dict(corr_overscan=corr_overscan, corr_nl=corr_nl, chunkreduction=chunkreduction)

        data_outs = []
        for i_, s_ in self.init_datafile.iterrows():
            filesin = s_["filepath"]
            fbuilder = CalibrationBuilder.from_filenames(filesin,
                                                         raw=True,
                                                         as_path=True,
                                                         persist=False)
            data, _ = fbuilder.build(**prop)
            data_outs.append(data)

        self._daily_ccds = data_outs
    

    # ============== #
    #  Property      #
    # ============== #
    @property
    def period(self):
        """ """
        if not hasattr(self, "_period"):
            return None
        
        return self._period

    @property
    def daily_ccds(self):
        """ """
        if not hasattr(self, "_daily_ccds"):
            raise AttributeError("_daily_ccds not available. run 'build_daily_ccds' ")
        
        return self._daily_ccds
        
    @property
    def init_datafile(self):
        """ """
        if not hasattr(self,"_init_datafile") or self._init_datafile is None:
            self._init_datafile = self.get_init_datafile()
            
        return self._init_datafile
