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
    def get_init_datafile(self):
        """ """
        return self.datafile.groupby(["day","ccdid"])["filepath"].apply(list).reset_index()

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
    
    def load_metadata(self, period=None, **kwargs):
        """ """
        from ztfin2p3 import metadata        
        if period is None and self._period is None:
            raise ValueError("no period given and none known")
        datafile = metadata.get_rawmeta(self.pipekind, self.period, add_filepath=True, **kwargs)
        self.set_datafile(datafile) 
        
    # ================= #
    #   High-Level      #
    # ================= #
    def get_ccd(self, ccdid=None, as_dict=False, mergestats="mean"):
        """ """
        if ccdid is None:
            ccdid = np.arange(1,17)
        else:
            ccdid = np.atleast_1d(ccdid)

        # list of stacked CCD array Nx6000x6000
        stacked_ccds = self.get_stacked_ccdarray(ccdid=ccdid, as_dict=False)
        ccds = [ztfimg.CCD.from_data( getattr(da,mergestats)(stacked_ccd_, axis=0) )
                    for stacked_ccd_ in stacked_ccds]

        if as_dict:
            return dict(zip(ccdid, ccds))
        
        return ccds

    def get_focalplane(self, mergestats="mean"):
        """ """
        ccdid = np.arange(1,17)
        ccds = self.get_ccd(ccdid=ccdid, as_dict=False)
        focal_plane = ztfimg.FocalPlane(ccds=ccds, ccdids=ccdid)
        return focal_plane

    def get_stacked_ccdarray(self, ccdid=None, as_dict=False):
        """ """
        ccdid_list = self.init_datafile.reset_index().groupby("ccdid")["index"].apply(list)

        if ccdid is None:
            ccdid = np.arange(1,17)
        else:
            ccdid = np.atleast_1d(ccdid)

        arrays_ = [da.stack([self.daily_ccds[i] for i in list_id]) 
                      if (list_id:=ccdid_list.get(ccdid_)) is not None else None
                      for ccdid_ in ccdid]
        if as_dict:
            return dict(zip(ccdid,arrays_))

        return arrays_

    def get_daily_focalplane(self, day=None, as_dict=False):
        """ """
        day_list = self.init_datafile.reset_index().groupby("day")["index"].apply(list)

        if day is None:
            days = day_list.index
        else:
            days = np.atleast_1d(day)

        focal_planes = []
        for day_ in days:
            day_index = day_list.loc[day_]
            ccdids = self.init_datafile.loc[day_index]["ccdid"].values
            ccds = [ztfimg.CCD.from_data(self.daily_ccds[i]) for i in day_index]
            focal_plane = ztfimg.FocalPlane(ccds=ccds, ccdids=ccdids)
            focal_planes.append(focal_plane)

        if as_dict:
            return dict(zip(days, focal_planes) )
        
        return focal_planes
    
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
