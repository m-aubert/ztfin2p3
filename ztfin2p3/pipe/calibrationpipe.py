
from .basepipe import CalibPipe
from ..builder import CalibrationBuilder
from .. import io

import pandas
import numpy as np

import ztfimg

# BasePipe has
#     - config and co.
#     - datafile and co.
#     - use_dask

__all__ = ["FlatPipe", "BiasPipe"]


class FlatPipe( CalibPipe ):

    _KIND = "flat"

    # ============== #
    #   Methods      #
    # ============== #
    def get_fileout(self, ccdid, ledid=None, filtername=None):
        """ get the filepath where the ccd data should be stored

        Parameters
        ----------
        ccdid: int
            id of the ccd (1->16)

        ledid: int or None
            = must be given if filtername is None =
            id of the LED. 

        filtername: str or None
            = must be given if ledid is None =
            name of the filter (zg, zr, zi)

        **kwargs goes to io.get_period_{kind}file

        Returns
        -------
        str
            fullpath

        See also
        --------
        get_ledid_ccd: get a ztfimg.ccd object for the given ccdid(s).

        """
        return super().get_fileout(ccdid, ledid=ledid, filtername=filtername)

    # ----------------- #
    #  High-Level build #
    # ----------------- #
    # this enables to expose ledid as option
    def get_ccd(self, ccdid=None, ledid=None, mergedhow="mean"):
        """ get a list of ztfimg.CCD object for each requested ccdid.
        These will merge all daily_ccds corresponding to this ccdid and ledid.
        
        Parameters
        ----------
        ccdid: int (or list of)
            id(s) of the ccd. (1->16). 
            If None all 16 will be assumed.

        ledid: int (or list of)
            id of the LED. 
            If None, all known ledid from init_datafile with be assumed.

        mergedhow: str
           name of the dask.array method used to merge the daily
           ccd data (e.g. 'mean', 'median', 'std' etc.) 

        Returns
        -------
        pandas.Series
            MultiIndex series (ccdid,ledid) with ztfimg.CCD objects as values.

        See also
        --------
        get_focalplane: get the full merged focalplane object
        """
        return super().get_ccd(ccdid=ccdid, ledid=ledid, mergedhow=mergedhow)

    def get_focalplane(self, ledid=None, mergedhow="mean", **kwargs):
        """ get the fully merged focalplane.
        It combines all 16 CCDs from get_ccd()

        Parameters
        ----------
        ledid: int (or list of)
            id of the LED. 
            If None, all known ledid from init_datafile with be assumed.

        mergedhow: str
           name of the dask.array method used to merge the daily
           ccd data (e.g. 'mean', 'median', 'std' etc.) 

        Returns
        -------
        ztfimg.FocalPlane
            the full merged focalplane.
        """
        ccds_df = self.get_ccd(mergedhow=mergedhow, ledid=ledid)
        return self._ccds_df_to_focalplane_df_(ccds_df, **kwargs)

    
    
    def get_period_focalplane(self, ledid=None, **kwargs):
        """ get the fully merged focalplane for the given period.
        It combines all 16 CCDs from get_ccd()

        Parameters
        ----------
        ledid: int (or list of)
            id of the LED. 
            If None, all known ledid from init_datafile with be assumed.

        mergedhow: str
           name of the dask.array method used to merge the daily
           ccd data (e.g. 'mean', 'median', 'std' etc.) 

        Returns
        -------
        ztfimg.FocalPlane
            the full merged focalplane.
        """
        ccds_df = self.get_period_ccd(ledid=ledid,**kwargs)
        return self._ccds_df_to_focalplane_df_(ccds_df, **kwargs)
    
    # ----------------- #
    #  Mid-Level build  #
    # ----------------- #
    def get_ccdarray(self, ccdid=None, ledid=None, mergedhow="mean"):
        """ get the dask.array for the given ccdids.
        The data are either 2d or 3d if merged is given.

        Parameters
        ----------
        ccdid: int (or list of)
            id(s) of the ccd. (1->16). 
            If None all 16 will be assumed.

        ledid: int (or list of)
            id of the LED. 
            If None, all known ledid from init_datafile with be assumed.

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
        datalist = self.init_datafile.reset_index().groupby(["ccdid","ledid"])["index"].apply(list)
        
        if ccdid is not None: # ledid is level 0
            datalist = datalist.loc[np.atleast_1d(ccdid)]
            
        if ledid is not None: # ledid is level 1
            datalist = datalist.loc[:,np.atleast_1d(ledid)]

        array_ = self._ccdarray_from_datalist_(datalist, mergedhow=mergedhow)
        return datalist.index, array_


    def get_daily_ccd(self, day=None, ccdid=None, ledid=None):
        """ get the ztfimg.CCD object(s) for the given day(s) and ccd(s)

        Parameters
        ----------
        day: str (or list of)
            day (format YYYYMMDD).
            If None, all known days from init_datafile will be assumed.

        ccdid: int (or list of)
            id(s) of the ccd. (1->16). 
            If None all 16 will be assumed.

        ledid: int (or list of)
            id of the LED. 
            If None, all known ledid from init_datafile with be assumed.

        Returns
        -------
        pandas.Serie
            MultiIndex (day, ccdid, ledid) of the corresponding ztfimg.CCD

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

        if ledid is not None:
            ledid = np.atleast_1d(ledid)
            datalist = datalist[datalist["ledid"].isin(ledid)]

        # to keep the same format as the other get_functions:
        datalist = datalist.reset_index().set_index(["day","ccdid", "ledid"])["index"]

        # Same as for bias from here.
        ccds = [ztfimg.CCD.from_data(self.daily_ccds[i])
                     for i in datalist.values]

        return pandas.Series(data=ccds, dtype="object",
                          index=datalist.index)

    def get_daily_focalplane(self, day=None, ledid=None, **kwargs):
        """ get the ztfimg.FocalPlane object gathering ccds
        for the given date.

        Parameters
        ----------
        day: str (or list of)
            day (format YYYYMMDD).
            If None, all known days from init_datafile will be assumed.

        **kwargs goes to ztfimg.FocalPlane()

        Returns
        -------
        pandas.Serie
            MultiIndex are day, value are the ztfimg.FocalPlane objects.

        See also
        --------
        get_daily_ccd: gets the ccd object for the given date. (used by get_daily_focalplane)
        """
        ccds_df = self.get_daily_ccd(day=day, ledid=ledid)
        return self._ccds_df_to_focalplane_df_(ccds_df, **kwargs)

    @staticmethod
    def _ccds_df_to_focalplane_df_(ccds_df, **kwargs):
        """ """
        
        ccds_df.name = "ccd" # cleaner for the df that comes next.
        ccds_df = ccds_df.reset_index(level=1)
        _grouped = ccds_df.groupby(level=[0,1])
        # convert to list of.
        # Remark that pandas does not handle groupby()[["k1","k2"]].apply(list). This
        # explain why there are two lists that I then join.
        # This is useful to make sure all 16 ccdids are there and their
        # ordering inside the dataframe. Maybe overkill but sure it is clean.
        ccds = _grouped["ccd"].apply(list).to_frame()
        ccdids = _grouped["ccdid"].apply(list).to_frame()
        ccds_df = ccds.join(ccdids)

        # the follows crashes (in purpose) if there are missing ccds
        return ccds_df.apply(lambda x: ztfimg.FocalPlane( ccds=x["ccd"],
                                                        ccdids=x["ccdid"], **kwargs), 
                                 axis=1)
    
    def get_period_ccd(self,from_file=False, rebuild=False, ccdid=None,ledid=None, filtername=None, **kwargs):
        if from_file : 
            ccds = self.get_period_ccd_fromfile(ccdid=ccdid, ledid=ledid, filtername=filtername) 
            self._period_ccds = [ccds.loc[i].data for i in ccds.index]
        else : 
            if not hasattr(self, "_period_ccds") or rebuild : 
                self.build_period_ccds(**kwargs)
            
        datalist = self.init_datafile.copy()
        datalist = bpip_baseline_period.init_datafile.reset_index().groupby(["ccdid", "ledid"])["index"].count()
        datalist = pd.Series(np.arange(len(datalist)) , index=datalist)  #Artisanal.
        
        if ccdid is not None:
            datalist = datalist.loc[np.atleast_1d(ccdid)]

        if ledid is not None:
            datalist = datalist.loc[np.atleast_1d(ledid)]
    
        ccds_im = [ztfimg.CCD.from_data(self.period_ccds[i]) for i in range(len(datalist.values))]
        ccds = pandas.Series(data=ccds_im, dtype="object", index=datalist.index)

        return ccds
    
    def get_period_ccd_fromfile(self, ccdid=None, ledid=None, use_dask=True):
        datalist = self.init_datafile.copy()
        if ccdid is not None:
            ccdid = np.atleast_1d(ccdid)
            datalist = datalist[datalist["ccdid"].isin(ccdid)]

        if ledid is not None:
            ledid = np.atleast_1d(ledid)
            datalist = datalist[datalist["ledid"].isin(ledid)]

        ids = datalist.reset_index().groupby(["ccdid" , "ledid"]).last().index.sort_values() 
                
        ccds = [ztfimg.RawCCD.from_data(self._from_fits(self.get_fileout(ccdid=ccdval, ledid=ledval), use_dask=use_dask)) for ccdval, ledval in ids]
        
        outp = pandas.Series(data=ccds, dtype="object", index=ids)
        return outp
    
    
    def _correct_master_bias(self, apply_period, from_file=False, ledid=None, **kwargs):
        """ 
        Function to apply master bias correction to designated period.
        Should not be called directly
        
        Parameters 
        ----------
        apply_period : str 
            Select the period to correct. Two options "daily" or "period". 
            "daily" applies to each flat daily_ccds
            "period" applied to each flat period_ccds. 
        
        from_file  : bool , default False
            Set to True if the period master bias / CCD are to be loaded from a file.
            Calls BiasPipe.get_fileout()
        
        ledid : int , default None
            Select the ledid to apply the bias correction. 
            If None, all LEDs will be considered.
            
        **kwargs 
            Transmitted to BiasPipe.get_period_ccd
            
        Returns
        -------
        ccd_list : list
            List of arrays. Dask array if use_dask in init , numpy otherwise.
        """
        
        if not apply_period in ["daily", "period"]:
            raise ValueError()
                
        datalist = self.init_datafile.copy()
        datalist["NFrames"] = datalist["filepath"].apply(len)
        #datalist.groupby(["ccdid", "ledid"]).Nframes.apply(sum)
                
        bias = BiasPipe.from_period(*self.period)
        bias_ccds = bias.get_period_ccd(from_file=from_file, **kwargs)
        
        call_func = getattr(self, "get_"+apply_period+"_ccd")  
         
        ccd_list = []
        for item, val in bias_ccds.iteritems():
            for itemi , vali in call_func(ccdid=item, ledid=ledid).iteritems():
                ccd_per_led = vali.data - val.data
                ccd_list.append(ccd_per_led)
                
        return ccd_list
 

    def build_period_ccds(self,corr_overscan=True, corr_nl=True, corr_bias=False, chunkreduction=2, use_dask=None, _groupbyk=["ccdid", "ledid"], normalize=False, bias_opt={}, **kwargs):
        
        """ Overloading of the build_period_ccds
        loads the period CalibrationBuilder based on the loaded daily_ccds.

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

        use_dask: bool or None
            should dask be used ? (faster if there is a client open)
            if None, this will guess if a client is available.
            
        normalize: bool, default False
            If True, normalize each flat by the nanmedian level.
        
        bias_opt : dict 
            Dictionnary to pass additionnal arguments to the master bias procedure.
            
        **kwargs
            Instruction to average the data
            The keyword arguments are passed to ztfimg.collection.ImageCollection.get_meandata() 

        Returns
        -------
        None
            sets self.period_ccds
                
        """
        
        super().build_period_ccds(corr_overscan=corr_overscan, corr_nl=corr_nl, chunkreduction=chunkreduction, use_dask=use_dask, _groupbyk=_groupbyk, **kwargs)
        
        if corr_bias : 
            ccd_list = self._correct_master_bias("period", **bias_opt)
            setattr(self, "_period_ccds", ccd_list)
            
        if normalize : 
            self._normalize(apply_period="period")
            
    def build_daily_ccds(self, corr_overscan=True, corr_nl=True, corr_bias=True, apply_period="daily", chunkreduction=None, use_dask=None, normalize=False,  bias_opt={}, **kwargs):
        """ Overloading of the build_daily_ccds
        loads the daily CalibrationBuilder based on init_datafile.

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
            
        apply_period: str, default 'daily'
            When should the bias corr happen. 
            'init' , upon loading the raw flats.
            'daily' , after creating the daily flats.
            
        chunkreduction: int or None
            rechunk and split of the image.
            If None, no rechunk

        use_dask: bool or None
            should dask be used ? (faster if there is a client open)
            if None, this will guess if a client is available.
            
        normalize: bool, default False
            If True, normalize each flat by the nanmedian level.
        
        bias_opt : dict 
            Dictionnary to pass additionnal arguments to the master bias procedure.
            
        **kwargs
            Instruction to average the data
            The keyword arguments are passed to ztfimg.collection.ImageCollection.get_meandata() 

        Returns
        -------
        None
            sets self.daily_ccds
                
        """        
        if corr_bias and apply_period == "init": 
            
            if use_dask is None:
                from dask import distributed
                try:
                    _ = distributed.get_client()
                    use_dask = True
                except:
                    use_dask = False
                    print("no dask")
                
            # function 
            calib_from_images = CalibrationBuilder.from_images
            #if use_dask:
            #    import dask
            #    calib_from_images = dask.delayed(calib_from_images)

            #Overscan corr will be done in loop
            prop = {**dict(chunkreduction=chunkreduction), **kwargs}

            bias = BiasPipe.from_period(*self.period)
            bias_ccds = bias.get_period_ccd(**bias_opt)

            data_outs = []
            for i_, s_ in self.init_datafile.iterrows():
                filesin = s_["filepath"]
                ccd_col =  ztfimg.collection.RawCCDCollection.from_filenames(filesin).get_data(corr_overscan=corr_overscan, corr_nl=corr_nl)
                ccd_col -= bias_ccds.loc[s_.ccdid].data
                new_ccds  = [ztfimg.RawCCD.from_data(ccd) for ccd in ccd_col]
                fbuilder = calib_from_images(new_ccds,
                                             raw=True, as_path=True,
                                             persist=False, use_dask=use_dask)

                data = fbuilder.build(**prop)[0]
                data_outs.append(data)

            #if use_dask:
            #    data_outs = dask.delayed(list)(data_outs).compute()

            self._daily_ccds = data_outs

        else : 
            super().build_daily_ccds(corr_overscan=corr_overscan, corr_nl=corr_nl, chunkreduction=chunkreduction,
                         use_dask=use_dask, **kwargs)
        
        if corr_bias and apply_period == "daily" : 
            ccd_list = self._correct_master_bias(apply_period, **bias_opt)
            setattr(self, "_daily_ccds", ccd_list)
        
        if normalize : 
            self._normalize(apply_period="daily")
    
    
    def _normalize(self,apply_period="daily"):
        """
        Normalize the period flats.
        Only called within build_{apply_period}_ccds.
        
        Should not be called externally.
        
        Parameters  
        ----------
        apply_period: str, default="daily")
            Period to normalize. Can be either "daily" or "period".
            
            
        
        Returns :
        None
            resets self.{apply_period}_ccds
        
        """
        data = getattr(self, '_'+apply_period+'_ccds')
        if "dask" in str(type(data[0])) :
            npda = da
        else : 
            npda = np
        
        data = npda.stack(data, axis=0)
        data /= npda.nanmedian(data,axis=0) 
        
        setattr(self, '_'+apply_period+'_ccds', [_arr for arr in data])
                    
    def store_period_ccds(self, ccdid=None, ledid=None, filtername=None, use_dask=True,  **kwargs):
        """
        Function to store created period_ccds
        
        Parameters 
        ----------
        ccdid: int or None
            id of the CCD
            
        ledid: int or None
            = must be given if filtername is None =
            id of the LED. 

        filtername: str or None
            = must be given if ledid is None =
            name of the filter (zg, zr, zi)
            
        **kwargs 
            Extra arguments to pass to the fits.writeto function.
        
        Returns
        -------
        list 
            List of filenames to which where written the data.
        
        """
        datalist = self.init_datafile.copy()
            
        datalist = self.init_datafile.copy()
        if ccdid is not None:
            ccdid = np.atleast_1d(ccdid)
            datalist = datalist[datalist["ccdid"].isin(ccdid)]

        if ledid is not None:
            ledid = np.atleast_1d(ledid)
            datalist = datalist[datalist["ledid"].isin(ledid)]

        ids = datalist.reset_index().groupby(["ccdid" , "ledid"]).last().index.sort_values() 

        outs = []
        if "dask" in str(type(self.period_ccds[0])): 
            for ccdid, ledid in ids : 
                fileout = self.get_fileout(ccdid=ccdid, ledid=ledid)
                data = self.period_ccds[ccdid-1].compute()
                out = self._to_fits(fileout, data, **kwargs)
                outs.append(out)
                
        else : 
            for ccdid, ledid in ids : 
                fileout = self.get_fileout(ccdid=ccdid, ledid=ledid)
                data = self.period_ccds[ccdid-1]
                out = self._to_fits(fileout, data, **kwargs)
                outs.append(out)
                
        return outs
    
class BiasPipe( CalibPipe ):
    _KIND = "bias"
    
    
            