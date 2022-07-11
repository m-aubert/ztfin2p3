""" library to build the ztfin2p3 pipeline starflats """

import dask

def bulk_download_gaia_calibrators(files, client, max_ncores=25):
    """ 
    This will first 

    """
    if len(client.ncores()) > max_ncores:
        raise ValueError(f"the given client has too many cores open {len(client.ncores())}>{max_ncores}")

    #
    # Building the DataFrame
    datafile = pandas.Series([l.split("/")[-1] for l in files]).str.split("_", expand=True
                                                          ).rename({0:"source", 1:"filefracday",
                                                                   2:"field", 3:"filtername", 4:"ccdid", 
                                                                   5:"imgtypecode",6:"qid", 7:"suffix"}, axis=1)
    datafile["ccdid"] = datafile["ccdid"].str.replace("c","")
    datafile["qid"] = datafile["qid"].str.replace("q","")
    datafile["rcid"] = ccdid_qid_to_rcid(datafile["ccdid"].astype(int), datafile["qid"].astype(int))

    radecs = [dask.delayed(astrometry.read_radec)(file_) for file_ in files]
    f_radecs = client.compute(radecs)
    ra,dec = np.asarray(client.gather(f_radecs)).T
    datafile["ra"],datafile["dec"]= ra,dec

    #
    # futures GaiaCalibrators
    gaiacalib = [dask.delayed(GaiaCalibrators)(int(df_.rcid),int(df_.field),radec=[float(df_.ra),float(df_.dec)], load=False
                                         ).download_data()
                     for index_,df_ in datafile.iterrows()]
    
    return client.compute(gaiacalib)
