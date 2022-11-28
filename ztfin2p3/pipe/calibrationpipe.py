
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

    def get_focalplane(self, ledid=None, mergedhow="mean"):
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
        ccds = self.get_ccd(mergedhow=mergedhow)
        raise NotImplementedError("not ready.")
        focal_plane = ztfimg.FocalPlane(ccds=ccds.values, ccdids=ccdids.index)
        return focal_plane

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

    def get_daily_focalplane(self, day=None, ledid=None):
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
        ccds_df = self.get_daily_ccd(day=day, ledid=ledid)
        days = ccds_df.index.levels[0]
        ledids = ccds_df.index.levels[2]
        ccdids = np.arange(1,17)
        print("TO BE DOUBLE CHECKED")
        
        # the follows crashes (in purpose) if there are missing ccds
        indexes = [(day, ledid) for ledid in ledids for day in days]
        fps = [ ztfimg.FocalPlane( ccds=ccds_df.loc[index_[0], ccdids, index_[1]].values,
                                   ccdids=ccdids)
               for index_ in indexes]
        
        return pandas.Series(data=fps, dtype="object", index=indexes)
    
    
class BiasPipe( CalibPipe ):
    _KIND = "bias"
