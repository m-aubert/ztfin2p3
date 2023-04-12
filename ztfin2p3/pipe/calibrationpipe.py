
from .basepipe import CalibPipe
from ..builder import CalibrationBuilder
from .. import io

import pandas
import numpy as np
import ztfimg
import dask.array as da

from ztfimg import collection
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
    def get_fileout(self, ccdid, ledid=None, filtername=None, day=None, periodicity="period"):
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
        """
        return super().get_fileout(ccdid, ledid=ledid, filtername=filtername,
                                    day=day, periodicity=periodicity)

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
        ccds_df = self.get_period_ccd(ledid=ledid)
        ledids = np.unique(ccds_df.index.get_level_values(1))
        ccdids = np.arange(1,17)
        # the follows crashes (in purpose) if there are missing ccds
        fps = [ ztfimg.FocalPlane( ccds=ccds_df.loc[ccdids, ledidi].values,
                                   ccdids=ccdids)
               for ledidi in ledids]
            
        return pandas.Series(data=fps, dtype="object", index=ledids)
    
    
    def get_filter_focalplane(self, filterid=None, **kwargs):
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
        ccds_df = self.get_filter_ccd(filterid=filterid)
        filterids = np.unique(ccds_df.index.get_level_values[1])
        ccdids = np.arange(1,17)
        # the follows crashes (in purpose) if there are missing ccds
        fps = [ ztfimg.FocalPlane( ccds=ccds_df.loc[ccdids, filteri].values,
                                   ccdids=ccdids)
               for filteri in filterids]
            
        return pandas.Series(data=fps, dtype="object", index=ledids)
    
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
    
    def get_period_ccd(self, rebuild=False, ccdid=None, ledid=None,
                        filtername=None, **kwargs):
        """ get a serie of ztfimg.CCD object for each requested ccdid.
        
        Parameters
        ----------
        ccdid: int (or list of) , default None
            id(s) of the ccd. (1->16). 
            If `None` all 16 will be assumed.
        
        ledid :  int (or list of) , default None.
            id(s) of the led.
            If `None` all 11 will be assumed.
        
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
        datalist = datalist.groupby(["ccdid", "ledid"]).filepath.apply(len).reset_index()
        datalist = datalist.reset_index().groupby(["ccdid", "ledid"]).index.apply(int)
        
        if ccdid is not None:
            ccdid = np.atleast_1d(ccdid)
            datalist = datalist.loc[ccdid,:]

        if ledid is not None:
            ledid = np.atleast_1d(ledid)
            datalist = datalist.loc[:,ledid]
        
        ccds_im = [ztfimg.CCD.from_data(self.period_ccds[i]) for i in datalist.values]
        ccds = pandas.Series(data=ccds_im, dtype="object", index=datalist.index)

        return ccds
    
    def _correct_master_bias(self, apply_period, from_file=False, **kwargs):
        """ 
        Function to apply master bias correction to designated period.
        Should not be called directly
        
        Parameters 
        ----------
        apply_period : str 
            Select the period to correct. Two options "daily" or "period". 
            "daily" applies daily bias to each flat daily_ccds
            "period" applies period bias to each flat period_ccds. 
        
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

        bias = BiasPipe.from_period(*self.period)
        bias_ccds = getattr(bias, "get_"+apply_period+"_ccd")(from_file=from_file, **kwargs)
        datalist = datalist.reset_index().set_index(["day", "ccdid", "ledid"])["index"] 

        ccd_list = []
        for item, val in bias_ccds.iteritems():
            ccd_bias = val.data
            for itemi , vali in datalist.loc[item].iteritems():
                ccd_per_led = self.daily_ccds[vali] - ccd_bias
                ccd_list.append(ccd_per_led)

        return ccd_list
 

    def build_period_ccds(self, corr_overscan=True, corr_nl=True, corr_bias=False,
                              chunkreduction=2, use_dask=None,
                              _groupbyk=["ccdid", "ledid"], from_file=False,
                              normalize=False, bias_opt={}, **kwargs):
        
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
        
        if not from_file : 
            super().build_period_ccds(corr_overscan=corr_overscan, corr_nl=corr_nl,
                                        chunkreduction=chunkreduction, use_dask=use_dask,
                                        _groupbyk=_groupbyk, from_file=from_file,
                                        **kwargs)
            
        else : 
            datalist = self.init_datafile.copy()
        
            data_outs = []
            for i , row in datalist.iterrows(): 
                file_in = self.get_fileout(row.ccdid, 
                                           periodicity="period",
                                           ledid=row.ledid)
                ccd = ztfimg.CCD.from_filename(file_in, use_dask=use_dask ) 
                data_outs.append(ccd.get_data(**kwargs))
            
            self._period_ccds =data_outs
        
        if corr_bias : 
            ccd_list = self._correct_master_bias("period", **bias_opt)
            setattr(self, "_period_ccds", ccd_list)
            
        if normalize : 
            self._normalize(apply_period="period")
            
    def build_daily_ccds(self, corr_overscan=True, corr_nl=True, corr_bias=True,
                             apply_bias_period="daily", chunkreduction=None,
                             use_dask=None, from_file=None, normalize=True,
                             bias_opt={}, **kwargs):
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
        if corr_bias and apply_bias_period == "init" and not from_file : 
            
            if use_dask is None:
                from dask import distributed
                try:
                    _ = distributed.get_client()
                    use_dask = True
                except:
                    use_dask = False
                    print("no dask")
                
            # function 
            calib_from_filenames= CalibrationBuilder.from_filenames

            #Overscan corr will be done in loop            
            prop = {**dict(corr_overscan=corr_overscan, corr_nl=corr_nl, 
                    chunkreduction=chunkreduction), 
                **kwargs}
               
            bias = BiasPipe.from_period(*self.period)
            bias_ccds = bias.get_daily_ccd(**bias_opt) #Hardcode dailies

            data_outs = []
            for i_, s_ in self.init_datafile.iterrows():
                filesin = s_["filepath"]
                fbuilder = calib_from_filenames(filesin, 
                                                use_dask=use_dask, 
                                                as_path=True,
                                                raw=True, 
                                                persist=False)
                data = fbuilder.build_with_corr(corr = bias_ccds.loc[s_.day, s_.ccdid].data ,
                                               **prop)[0]
                data_outs.append(data)

            self._daily_ccds = data_outs

        else : 
            
            if not from_file : 
                super().build_daily_ccds(corr_overscan=corr_overscan, corr_nl=corr_nl, 
                                     chunkreduction=chunkreduction, use_dask=use_dask,from_file=from_file, **kwargs)
            
            else : 
                datalist = self.init_datafile.copy()
                datalist = datalist.reset_index().groupby(["ccdid", "ledid"]).index
                datalist = datalist.reset_index()
        
                data_outs = []
                for i , row in datalist.iterrows(): 
                    file_in = self.get_fileout(row.ccdid, 
                                                periodicity="period",
                                                ledid=row.ledid)
                    ccd = ztfimg.CCD.from_filename(file_in, use_dask=use_dask ) 
                    data_outs.append(ccd.get_data(**kwargs))

                self._period_ccds =data_outs
            
            
            if corr_bias and apply_bias_period == "daily" : 
                ccd_list = self._correct_master_bias(apply_bias_period, **bias_opt)
                setattr(self, "_daily_ccds", ccd_list)

        if normalize : 
            self._normalize(apply_bias_period="daily")
    
    
    def _normalize(self,apply_bias_period="daily"):
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
        if "dask" in str(type(getattr(self, '_'+apply_bias_period+'_ccds')[0])) :
            npda = da
        else : 
            npda = np
        
        data = npda.stack(getattr(self, '_'+apply_bias_period+'_ccds'), axis=0)
        datai = data/npda.nanmedian(data,axis=(1,2))[:,None,None]
        
        setattr(self, '_'+apply_bias_period+'_ccds', datai ) #Now we have an array instead
                    
    

    
    
    def build_filter_ccds(self,weights=dict(zg=None, zr=None, zi=None), axis=0, **kwargs):
        """
        Combine to period ccds per led to create filter ccds.

        Parameters
        ----------
        weights : dict, default `dict(zg=None, zr=None, zi=None)`
            Dictionnary storing for each filter the weights to apply to each led. 
            Default is a dictionnary with empty fields.
            
        axis : int, default 0
            Axis to average the LEDs.

        Returns
        -------
        None
            sets self.filter_ccds
            
        """
        if "dask" in str(type(self._period_ccds[0])) :
            npda = da
        else : 
            npda = np
        
        if not hasattr(self, "_period_ccds"):
            self.build_period_ccds(**kwargs)
        
        period_ccd = self.init_datafile.copy()
        period_ccd = period_ccd.groupby(['ccdid', "ledid"]).filepath.apply(len).reset_index()
        period_ccd["filterid"] = period_ccd["ledid"]

        for key, items in self._led_to_filter.items(): 
            period_ccd["filterid"] = period_ccd.filterid.replace(items, key)

        period_ccd = period_ccd.reset_index().groupby(['ccdid', "filterid"]).index.apply(list)

        period_filters = []
        for i, ccdi in period_ccd.iteritems():
            ccdicol = npda.average(self.period_ccds[ccdi] , weights=weights[i[1]], axis=axis)
            period_filters.append(ccdicol)
            
        self._filter_ccds = npda.stack(period_filters, axis=0)
        
        
    def get_filter_ccd(self,filterid=None, ccdid=None):
        """ get a serie of ztfimg.CCD object for each requested ccdid in each filter.
        
        Parameters
        ----------
        ccdid: int (or list of) , default None
            id(s) of the ccd. (1->16). 
            If `None` all 16 will be assumed.
        
        filterid :  str , default None.
            string describing the filter : 'zg', 'zr', 'zi'
            If `None` all three will be assumed. 
        

        Returns
        -------
        pandas.Series
            indexe as ccdid, filterid and values as ztfimg.CCD objects
            
        See Also
        ---------
        get_filter_focalplane
        """
        datalist= self.init_datafile.copy()
        datalist["filterid"] = datalist["ledid"] 
        
        for key, items in self._led_to_filter.items() : 
            datalist["filterid"] = period_ccd.replace(items, key)
        
        # to keep the same format as the other get_functions:
        datalist = datalist.groupby("ccdid", "filterid").filepath.apply(len)
        datalist = datalist.reset_index()
        
        if filterid is not None:
            filterid = np.atleast_1d(filterid)
            datalist = datalist[datalist["filterid"].isin(filterid)]

        if ccdid is not None:
            ccdid = np.atleast_1d(ccdid)
            datalist = datalist[datalist["ccdid"].isin(ccdid)]

        datalist = datalist.reset_index().set_index(["ccdid", "filterid"])

        ccds  = [ztfimg.CCD.from_data(self.filter_ccds[i]) for i in datalist["index"]]
        
        return pandas.Series(data=ccds, dtype="object",
                          index=datalist.index)
    
    def store_ccds(self, periodicity="period", fits_kwargs = {}, **kwargs):
        """
        Function to store created period_ccds
        
        Parameters 
        ----------
        
        ccdid : int (list of int) , default None
            id of the ccdid. If None, will store all ccds.
            
        periodicity : str , default "period"
            Which data to store : 
                'daily' stores the available data self.daily_ccds. 
                'period' stores the available data in self.period_ccds. 
                'filter' stores the available data in self.filter_ccds 
            
        **kwargs 
            Extra arguments to pass to the fits.writeto function.
        
        Returns
        -------
        list 
            List of filenames to which where written the data.
        
        """
            
        datalist = self.init_datafile.copy()
        if periodicity == 'period' : 
            pdata = getattr(self, periodicity+'_ccds')

            _groupbyk = ['ccdid', 'ledid']
            datalist = datalist.reset_index().groupby(_groupbyk).day.apply(lambda x : None).reset_index()
            datalist = datalist.reset_index()
            datalist["filterid"] = None
            
        elif periodicity == 'filter': 
            pdata = getattr(self, periodicity+'_ccds')

            datalist["filterid"] = datalist["ledid"]
            for key, items in self._led_to_filter.items(): 
                datalist["filterid"] = datalist.filterid.replace(items, key)

            _groupbyk = ['ccdid', 'ledid', 'filterid']
            datalist = datalist.reset_index().groupby(_groupbyk).day.apply(lambda x : None).reset_index()
            datalist = datalist.reset_index()
            
            periodicity="period" #Reset periodicity
                
        else : 
            pdata = getattr(self, periodicity+'_ccds')
            _groupbyk = ['day','ccdid', 'ledid']
            datalist = datalist.reset_index()
            datalist["filterid"] = None #To obtain all purpose function

        outs = []
        if "dask" in str(type(pdata[0])): 
            for i , row in datalist.iterrows() : 
                    fileout = self.get_fileout(ccdid=row.ccdid, 
                                               periodicity=periodicity, 
                                               day=row.day, 
                                               ledid=row.ledid,
                                                filtername=row.filterid)
                    data = pdata[row['index']].compute() 
                    out = self._to_fits(fileout, data, **fits_kwargs)
                    outs.append(out)
                
        else : 
            for i,row in datalist.iterrows() : 
                fileout = self.get_fileout(ccdid=row.ccdid, 
                                            periodicity=periodicity, 
                                            day=row.day, 
                                            ledid=row.ledid,
                                            filtername=row.filterid)
                out = self._to_fits(fileout, pdata[row['index']], **kwargs)
                outs.append(out)
                
        return outs
    
    
    def build_daily_filter_ccds(self, weights=None, axis=0):
        """
        Combine daily ccds per led to create daily filter ccds.

        Parameters
        ----------
        weights : dict, default `dict(zg=None, zr=None, zi=None)`
            Dictionnary storing for each filter the weights to apply to each led. 
            Default is a dictionnary with empty fields.
            
        axis : int, default 0
            Axis to average the LEDs.

        Returns
        -------
        list
            List of array_like objects storing each filter ccd per day.
            
        """
        
        if not weights :         
            weights = dict(zg=None,  
                           zr=None, 
                           zi=None)
        
        period_ccd = self.init_datafile.copy()
        period_ccd["filterid"] = period_ccd["ledid"]

        for key, items in self._led_to_filter.items():
            period_ccd["filterid"] = period_ccd.filterid.replace(items, key)

        period_ccd = period_ccd.reset_index()
        period_ccd = period_ccd.groupby(['day','ccdid', "filterid"]).index.apply(list)

        period_filters = []
        for i, ccdi in period_ccd.iteritems():
            ccdicol = da.average(self.daily_ccds[ccdi] , weights=weights[i[2]], axis=axis)
            period_filters.append(ccdicol)

        return period_filters
        

    @property
    def _led_to_filter(self):
        #Should be done better
        return dict(zg=[2,3,4,5] ,  zr=[7,8,9,10], zi = [11,12,13])
      
    @property
    def filter_ccds(self):
        if not hasattr(self, '_filter_ccds'):
            raise AttributeError("_filter_ccds not available. run 'build_filter_ccds'")
        return self._filter_ccds




class BiasPipe( CalibPipe ):
    _KIND = "bias"

    
