
""" library to build the ztfin2p3 pipeline screen flats """
import os
import numpy as np
import dask
import dask.array as da
import warnings
from astropy.io import fits


from ztfimg.base import Image, FocalPlane, CCD

LED_FILTER = {"zg":[2,3,4,5],
              "zr":[7,8,9,10],
              "zi":[11,12,13],
                }
    
def ledid_to_filtername(ledid):
    """ """
    for f_,v_ in LED_FILTER.items():
        if int(ledid) in v_:
            return f_
    raise ValueError(f"Unknown led with ID {ledid}")



def build_from_datapath(build_dataframe, assume_exist=False, inclheader=False, overwrite=True, **kwargs):
    """ """
    if not assume_exist:
        from ztfquery import io
        
    outs = []
    for i_, s_ in build_dataframe.iterrows():
        # 
        fileout = s_.fileout
        os.makedirs(os.path.dirname(fileout), exist_ok=True) # build if needed
        files = s_["filepath"]
        if not assume_exist:
            files = io.bulk_get_file(files)
        # 
        bflat = FlatBuilder.from_rawfiles(files, persist=False)
        data, header = bflat.build(set_it=False, inclheader=inclheader, **kwargs)
        output = dask.delayed(fits.writeto)(fileout, data, header=header, overwrite=overwrite)
        outs.append(output)
        
    return outs


