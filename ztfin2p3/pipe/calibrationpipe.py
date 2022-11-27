
from .basepipe import CalibPipe
from ..builder import CalibrationBuilder
from .. import io
# BasePipe has
#     - config and co.
#     - datafile and co.
#     - use_dask

__all__ = ["FlatPipe", "BiasPipe"]


class FlatPipe( CalibPipe ):

    _KIND = "flat"
    
    def get_stacked_ccdarray(self, ccdid=None, ledid=None):
        """ """
        ccdid_list = self.init_datafile.reset_index().groupby(["ccdid","ledid"])["index"].apply(list)

        if ccdid is not None:
            ccdid = list(np.atleast_1d(ccdid))

        if ledid is not None:
            ledid = list(np.atleast_1d(ledid))

        # both given
        if ccdid is not None and ledid is not None:
            ccdid_list = ccdid_list.loc[ccdid,ledid]
        elif ledid is not None:
            ccdid_list = ccdid_list.loc[:,ledid]
        elif ccdid is not None:
            ccdid_list = ccdid_list.loc[ccdid]
        #else both are None

        arrays_ = [da.stack([self.daily_ccds[i] for i in list_id]) 
                    for list_id in ccdid_list.values]

        return ccdid_list.index.values, arrays_

    def get_daily_ledid_focalplane(self, day=None, ledid=None):
        """ """
        day_list = self.init_datafile.reset_index().groupby(["day", "ledid"])["index"].apply(list)
        
        if day is not None:
            day = list(np.atleast_1d(day))

        if ledid is not None:
            ledid = list(np.atleast_1d(ledid))


        

        

        
    def get_daily_focalplane(self, day=None):
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
        
        return focal_planes

    
class BiasPipe( CalibPipe ):
    _KIND = "bias"
