from ..builder import CalibrationBuilder
from .. import io

import pandas
import numpy as np
import dask.array as da
from astropy.io import fits
import os
import ztfimg 
import dask

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
    def from_period(cls, start, end, skip=None , use_dask=True, **kwargs):
        """ 
        
        Parameters
        ----------
        start, end: 
            the period concerned by this flat. 
            format: yyyy-mm-dd
            
        skip : int or None
            Number of bias to be skipped in the initial datafile.
            
        """
        this = cls([start, end], use_dask=use_dask)
        # Load the associated metadata
        this.load_metadata(**kwargs)
        if not skip : 
            return this
        else : 
            this.init_datafile #Instantiate
            this._init_datafile["filepath"] = this.init_datafile["filepath"].map(lambda x: x[skip:]) 
            return this
            
    
    # ============== #
    #   Methods      #
    # ============== #
    def get_fileout(self, ccdid, periodicity="period", day=None, **kwargs):
        """ get the filepath where the ccd data should be stored

        Parameters
        ----------
        ccdid: int
            id of the ccd (1->16)

        periodicity : 
            should path point to "daily" or "period" bias.
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
            if periodicity=="period":
                fileout = io.get_period_biasfile(*self.period, ccdid=ccdid)
            else :
                if day is not None: 
                    fileout = io.get_daily_biasfile(day,ccdid) 
                else : 
                    raise ValueError(f"Daily is requested but is not defined")
            
        elif self.pipekind == "flat":
            if periodicity=="period":

                fileout = io.get_period_flatfile(*self.period, ccdid=ccdid, **kwargs)
            
            else :
                if day is not None: 
                    fileout = io.get_daily_flatfile(day,ccdid,**kwargs) 
                else : 
                    raise ValueError(f"Daily is requested but is not defined")
            
        else:
            raise NotImplementedError(f"only bias and flat kinds implemented ; this is {self.pipekind}")

        return fileout
    
    # ----------------- #
    #  I/O functions    #
    # ----------------- # 
    
    def store_ccds(self, periodicity="period", band=None, fits_kwargs = {}, **kwargs):
        """
        Function to store created period_ccds
        
        Parameters 
        ----------
        
        ccdid : int (list of int) , default None
            id of the ccdid. If None, will store all ccds. 
            
        **kwargs 
            Extra arguments to pass to the fits.writeto function.
        
        Returns
        -------
        list 
            List of filenames to which where written the data.
        
        """
            
        datalist = self.init_datafile.copy()
        
        if periodicity == 'period' : 
            _groupbyk = 'ccdid'
            ids = datalist.reset_index().groupby("ccdid").last().index.sort_values()
            index = np.column_stack([ids, [None]*len(ids)])
        else : 
            _groupbyk = ['day', 'ccdid']
            ids = datalist.reset_index().set_index(_groupbyk)["index"].values
            index = datalist.reset_index().set_index(_groupbyk).index
        
        pdata = getattr(self, periodicity+'_ccds')
        #if not ccdid : 
        #    ids = datalist.reset_index().groupby("ccdid").last().index.sort_values()
        #else : 
        #    ids = np.atleast_1d(ccdid)
            
        outs = []
        if "dask" in str(type(pdata[0])): 
            for ccdid in ids : 
                    fileout = self.get_fileout(ccdid=index[ccdid][1], periodicity=periodicity, day=index[ccdid][0], **kwargs)
                    data = pdata[ccdid-1].compute() 
                    out = self._to_fits(fileout, data, **fits_kwargs)
                    outs.append(out)
                    #Compute iteratively for low memory management. 
                    #Check if delaying the compute call might work 
                    #(that is compute remotely on worker then save)
                    #. TBD.
                
        else : 
            for ccdid in ids : 
                fileout = self.get_fileout(ccdid=ccdid, periodicity=periodicity, **kwargs)
                data = getattr(self, periodicity+'_ccds')[ccdid-1].compute() 
                out = self._to_fits(fileout, data, **kwargs)
                outs.append(out)
                
        return outs
    
    #def get_period_ccd_fromfile(self, ccdid=None, use_dask=True):
    #    """ load the period ccd for a given ccdid. 
    #    The period ccd filename is the one provided by get_fileout.
    #    
    #    Parameters 
    #    ----------
    #    ccdid : int (list of int) , default None
    #        id of the ccdid. If None, will load all ccds. 
    #        
    #    use_dask = bool , default True
    #        Whether to use dask to load the ccds. 
    #        
    #    Returns
    #    -------
    #    pandas.Series
    #        indexe as ccdid and values as ztfimg.CCD objects
    #    """
    #    if ccdid is not None : 
    #        ids = np.sort(np.atleast_1d(ccdid).tolist())
    #    else : 
    #        datalist = self.init_datafile.copy()
    #        ids = datalist.reset_index().groupby("ccdid").last().index.sort_values()
                    
    #    ccds = [ztfimg.RawCCD.from_data(self._from_fits(self.get_fileout(ccdid=val), use_dask=use_dask)) for val in ids]
        
    #    outp = pandas.Series(data=ccds, dtype="object", index=ids)
    #    return outp
    
    def _to_fits(self, fileout, data, header=None, overwrite=True, **kwargs):
        """ Store the data in fits format 

        Parameters
        ----------
        fileout: str
            filepath where the data should be stored

        header: `fits.Header`
            header. If None self.header will be used

        overwrite: bool
            if fileout already exist, should this overwrite it ?

        **kwargs goes to fits.writeto()
        

        Returns
        -------
        str
            The input fileout (to check all works fine)
        """
        dirout = os.path.dirname(fileout) 
        if dirout not in ["", "."] and not os.path.isdir(dirout):
                os.makedirs(dirout, exist_ok=True)
                
        fits.writeto(fileout, data, header=header,
                                 overwrite=overwrite,
                                 **kwargs)
        return fileout
    
    
    def _from_fits(self,filename, use_dask=True, ext=None):
        """ assuming fits format."""
        from astropy.io.fits import getdata
        if use_dask:
            # - Data
            data = da.from_delayed( dask.delayed(getdata) (filename, ext=ext),
                                   shape=ztfimg.CCD.SHAPE, dtype="float32")
        else:
            data = getdata(filename, ext=ext)

        return data
        
    
    # ----------------- #
    #  High-Level build #
    # ----------------- #        
    def get_period_ccd(self,rebuild=False, ccdid=None, **kwargs):
        """ get a list of ztfimg.CCD object for each requested ccdid.
        
        Parameters
        ----------
        ccdid: int (or list of) , default None
            id(s) of the ccd. (1->16). 
            If `None` all 16 will be assumed.
        
        from_file : bool , default False
            If True, will load the requested ccdids from file. 
            Filename is assumed to be given by the `self.get_fileout` function

        rebuild : bool, default False
            If True, will reset the `period_ccds` attribute by calling  `build_period_ccd`.
        
        **kwargs : 
            Keyword arguments passed to build_period_ccd.
            (e.g merging, sigma_clipping etc) to control the merging of the daily ccds.
                
        Returns
        -------
        pandas.Series
            indexe as ccdid and values as ztfimg.CCD objects

        """
        if not hasattr(self, "_period_ccds") or rebuild : 
            self.build_period_ccds(**kwargs)

        datalist = self.init_datafile.copy()
        if ccdid is not None:
            ccdid = np.atleast_1d(ccdid)
            datalist = datalist[datalist["ccdid"].isin(ccdid)]

        ids = datalist.reset_index().groupby(["ccdid"]).last().index.sort_values() 

        ccds_im = [ztfimg.CCD.from_data(self.period_ccds[i-1]) for i in ids]
        
        ccds = pandas.Series(data=ccds_im, dtype="object", index=ids)

        return ccds
                  
    def get_ccd(self, ccdid=None, mergedhow="mean", **kwargs):
        """ get a list of ztfimg.CCD object for each requested ccdid.
        These will merge all daily_ccds corresponding to this ccdid.
        
        Parameters
        ----------
        ccdid: int (or list of)
            id(s) of the ccd. (1->16). 
            If None all 16 will be assumed.

        mergedhow: str
           name of the dask.array method used to merge the daily
           ccd data (e.g. 'mean', 'median', 'std' etc.) 

        Returns
        -------
        pandas.Series
            indexe as ccdid and values as ztfimg.CCD objects

        See also
        --------
        get_focalplane: get the full merged focalplane object
        """
        # list of stacked CCD array Nx6000x6000
        indexes, ccddata = self.get_ccdarray(ccdid=ccdid, mergedhow=mergedhow, **kwargs)
        ccds = [ztfimg.CCD.from_data(ccddata_) for ccddata_ in ccddata]
        return pandas.Series(data=ccds, dtype="object", index=indexes)

    def get_period_focalplane(self, **kwargs):
        """ get the fully merged focalplane.
        It combines all 16 CCDs from get_period_ccd()

        Parameters
        ----------
        **kwargs : 
            kwargs to be passed to the get_period_ccd

        Returns
        -------
        ztfimg.FocalPlane
            the full merged focalplane.
        """
        ccds = self.get_period_ccd(**kwargs)
        focal_plane = ztfimg.FocalPlane(ccds=ccds.values, ccdids=ccds.index)
        return focal_plane
    
    def get_focalplane(self, mergedhow="mean"):
        """ get the fully merged focalplane.
        It combines all 16 CCDs from get_ccd()

        Parameters
        ----------
        mergedhow: str
           name of the dask.array method used to merge the daily
           ccd data (e.g. 'mean', 'median', 'std' etc.) 

        Returns
        -------
        ztfimg.FocalPlane
            the full merged focalplane.
        """
        ccds = self.get_ccd(mergedhow=mergedhow)
        focal_plane = ztfimg.FocalPlane(ccds=ccds.values, ccdids=ccdids.index)
        return focal_plane
    
    # ----------------- #
    #  Mid-Level build  #
    # ----------------- #        
    def get_ccdarray(self, ccdid=None, mergedhow=None):
        """ get the dask.array for the given ccdids.
        The data are either 2d or 3d if merged is given.

        Parameters
        ----------
        ccdid: int (or list of)
            id(s) of the ccd. (1->16). 
            If None all 16 will be assumed.

        mergedhow: str
           name of the dask.array method used to merge the daily
           ccd data (e.g. 'mean', 'median', 'std' etc.) 

        Returns
        -------
        list
            list of dask array (one per given ccdid).

        See also
        --------
        get_ccd: get a this of ztfimg.CCD (uses get_ccdarray)
        get_daily_ccd: get the ztfimg.CCD of a given day.
        """
        datalist = self.init_datafile.reset_index().groupby("ccdid")["index"].apply(list)
        
        if ccdid is not None:
            datalist = datalist.loc[np.atleast_1d(ccdid)]

        array_ = self._ccdarray_from_datalist_(datalist, mergedhow=mergedhow)
        return datalist.index.values, array_

    def get_daily_ccd(self, day=None, ccdid=None, **kwargs):
        """ get the ztfimg.CCD object(s) for the given day(s) and ccd(s)

        Parameters
        ----------
        day: str (or list of)
            day (format YYYYMMDD).
            If None, all known days from init_datafile will be assumed.

        ccdid: int (or list of)
            id(s) of the ccd. (1->16). 
            If None all 16 will be assumed.
            
        from_file : bool , default False
            Whether to load the daily bias from file.
            
        **kwargs : 
            Argument to pass to get data routine

        Returns
        -------
        pandas.Serie
            MultiIndex (day, ccdid) of the corresponding ztfimg.CCD

        See also
        --------
        get_daily_focalplane: get the full ztf.img.FocalPlane for the given day(s)
        """
        datalist = self.init_datafile.copy()
        if day is not None:
            day = np.atleast_1d(day)
            datalist = datalist[datalist["day"].isin(day)]

        if ccdid is not None:
            ccdid = np.atleast_1d(ccdid)
            datalist = datalist[datalist["ccdid"].isin(ccdid)]

        #if not from_file : 
        if not hasattr(self, '_daily_ccds'):
            self.build_daily_ccds(**kwargs)
            
        # to keep the same format as the other get_functions:
        datalist = datalist.reset_index().set_index(["day","ccdid"])["index"]
        ccds = [ztfimg.CCD.from_data(self.daily_ccds[i])
                     for i in datalist.values]
 
        return pandas.Series(data=ccds, dtype="object",
                          index=datalist.index)
    
    
    def get_daily_focalplane(self, day=None):
        """ get the ztfimg.FocalPlane object gathering ccds
        for the given date.

        Parameters
        ----------
        day: str (or list of)
            day (format YYYYMMDD).
            If None, all known days from init_datafile will be assumed.

        Returns
        -------
        pandas.Serie
            indexes are day, value are the ztfimg.FocalPlane objects.

        See also
        --------
        get_daily_ccd: gets the ccd object for the given date. (used by get_daily_focalplane)
        """
        ccds_df = self.get_daily_ccd(day=day)
        days = ccds_df.index.levels[0]
        ccdids = np.arange(1,17)
        # the follows crashes (in purpose) if there are missing ccds
        fps = [ ztfimg.FocalPlane( ccds=ccds_df.loc[day, ccdids].values,
                                   ccdids=ccdids)
               for day in days]
            
        return pandas.Series(data=fps, dtype="object", index=days)
        
        
    # ----------------- #
    #  Internal         #
    # ----------------- #
    def _ccdarray_from_datalist_(self, datalist, mergedhow=None):
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

        if mergedhow is not None:
            arrays_ = [getattr(da, mergedhow)(a_, axis=0)
                            for a_ in arrays_]
        # do not set this in pandas.Series as it compiles it. (must call np.asarray somewhere)
        return arrays_
    
    # ----------------- #
    #   Structural      #
    # ----------------- #
    
    def build_period_ccds(self,corr_overscan=False, corr_nl=False, chunkreduction=None, rebuild=False, use_dask=None, from_file=None, daily_ccds_opts={}, _groupbyk="ccdid", **kwargs):
        """ loads the period CalibrationBuilder based on the computed daily_ccds.

        Parameters
        ----------
        corr_overscan: bool
            Should the data be corrected for overscan
            (if both corr_overscan and corr_nl are true, 
            nl is applied first)

        corr_nl: bool
            Should data be corrected for non-linearity
        
        corr_bias : bool
            Should data be corrected for master bias

        chunkreduction: int or None
            rechunk and split of the image.
            If None, no rechunk

        rebuild : bool , default False.
            If `True`, reset `daily_ccds` by calling `build_daily_ccds` prior to creating the period. 

        use_dask: bool or None
            should dask be used ? (faster if there is a client open)
            if None, this will guess if a client is available.

        daily_ccds_opt: dict or None
            Instruction to build the daily bias. Passe to `build_daily_ccds`.
            Used only if `daily_ccds` attribute is not build or if `rebuild`is True.
           
        **kwargs
            Instruction to average the data
            The keyword arguments are passed to ztfimg.collection.ImageCollection.get_meandata() 

        Returns
        -------
        None
            sets self.period_ccds
                
        """
        
        if use_dask : 
            npda = da
        else : 
            npda = np
        
        
        if not from_file : 
            if not hasattr(self, "_daily_ccds") or rebuild : 
                    self.build_daily_ccds(**daily_ccds_opts)

            datalist = self.init_datafile.copy()
            ccds_dailycol= datalist.reset_index().groupby(_groupbyk).index.apply(list) 

            prop = {**dict(corr_overscan=corr_overscan, corr_nl=corr_nl, 
                        chunkreduction=chunkreduction, incl_header=False), 
                    **kwargs}

            calib_from_data = CalibrationBuilder.build_from_data
            
            if type(self.daily_ccds) == str(list):
                
                data_outs = []
                for _, ccd_idx in ccds_dailycol.iteritems(): 
                    outi = calib_from_data(npda.stack([self.daily_ccds[ccd_idxi] for ccdixi in ccd_idx], axis=0) , 
                                                                                 **prop)
                    data_outs.append(outi)
                    
                self._period_ccds = npda.stack(data_outs, axis=0)
                
            else : 
                
                self._period_ccds = npda.stack([calib_from_data(self.daily_ccds[ccd_idx,:,:], 
                                                                                 **prop) 
                                              for _, ccd_idx in ccds_dailycol.iteritems()], 
                                             axis=0)    
            
        else : 
            
            data_outs = []
            for ccdid in range(1,17) : 
                data = self._from_fits(self.get_fileout(ccdid=val), use_dask=use_dask)
                data_outs.append(data)
            datalist = self.init_datafile.copy()

            self._period_ccds =data_outs

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
        
    def build_daily_ccds(self, corr_overscan=True, corr_nl=True, chunkreduction=None,
                         use_dask=None, from_file=None, **kwargs):
        """ loads the daily CalibrationBuilder based on init_datafile.

        Parameters
        ----------
        corr_overscan: bool
            Should the data be corrected for overscan
            (if both corr_overscan and corr_nl are true, 
            nl is applied first)

        corr_nl: bool
            Should data be corrected for non-linearity

        chunkreduction: int or None
            rechunk and split of the image.
            If None, no rechunk

        use_dask: bool or None
            should dask be used ? (faster if there is a client open)
            if None, this will guess if a client is available.
            
        **kwargs
            Instruction to average the data
            The keyword arguments are passed to ztfimg.collection.ImageCollection.get_meandata() 

        Returns
        -------
        None
            sets self.daily_ccds
        """

        if use_dask is None:
            from dask import distributed
            try:
                _ = distributed.get_client()
                use_dask = True
            except:
                use_dask = False
                print("no dask")
        
        if not from_file :         
            # function 
            calib_from_filename = CalibrationBuilder.from_filenames
            if use_dask:
                import dask
                calib_from_filename = dask.delayed(calib_from_filename)

            prop = {**dict(corr_overscan=corr_overscan, corr_nl=corr_nl, 
                        chunkreduction=chunkreduction), 
                    **kwargs}


            data_outs = []
            for i_, s_ in self.init_datafile.iterrows():
                filesin = s_["filepath"]
                fbuilder = calib_from_filename(filesin,
                                               raw=True, as_path=True,
                                               persist=False,
                                               use_dask=use_dask)
                data = fbuilder.build(**prop)[0]
                data_outs.append(data)

            if use_dask:
                data_outs = dask.delayed(list)(data_outs).compute()

            self._daily_ccds =data_outs
            
        else : 
            datalist = self.init_datafile.copy()
        
            data_outs = []
            for i , row in datalist.iterrows(): 
                file_in = self.get_fileout(row.ccdid, 
                                           periodicity="daily",
                                           day=row.day)
                ccd = ztfimg.CCD.from_filename(file_in, use_dask=use_dask ) 
                data_outs.append(ccd.get_data(**kwargs))
            
            self._daily_ccds =data_outs
        
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

    @property
    def period_ccds(self):
        """ """
        if not hasattr(self,"_period_ccds") : 
            raise AttributeError("_period_ccds not available. run 'build_period_ccds' or 'get_period_ccd' ")
        return self._period_ccds
                                          
                                          
        