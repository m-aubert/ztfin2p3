import ztfimg 
import pandas as pd 
import pathlib
import rich_click as click
import numpy as np
from ztfin2p3.io import CAL_DIR, get_daily_flatfile, get_daily_biasfile
from ztfin2p3.metadata import period_from_logs
from astropy.stats import mad_std, sigma_clip
from scipy.stats import describe 
import tqdm
import glob
from copy import copy

def get_mode(arr):
   val, count =  np.unique(arr, return_counts=True)
   if count.max() > 1:
      return val[count.argmax()]
   else: 
       return None

def get_all_stats(f1, suffix=None): 
    f1 = sigma_clip(f1, sigma=20)
    nout = f1.mask.sum()
    f1 = f1.data[~f1.mask]

    output = describe(f1)
    q25, median_, q75 = np.nanpercentile(f1, [25,50,75])
    min_ , max_ = output.minmax
    data = dict(q25=q25,
                median = median_, 
                q75 = q75, #Quantiles
                madstd = mad_std(f1), 
                mode=get_mode(f1),
                std = f1.std(ddof=1),
                mean = output.mean,
                max = max_,
                min = min_,
                skew = output.skewness,
                kurt = output.kurtosis, 
                nout = nout)

    if suffix is not None : 
        data = {f'{k}{suffix}': v for k, v in data.items()}

    return data


@click.command(context_settings={"show_default": True})
@click.argument("perid", nargs=-1) 
@click.option("--ccdid", type=int, help="ccdid to process")
@click.option("--filterkey", help="filter to process")
@click.option("--outfile", default=None, help="filepath")
def QA_flat(perid, ccdid, filterkey, outfile=None):
    """Quality analysis for a list of bias for a given year, a given CCD and ZTF Filter.

    \b
    Process perid (int):
    --ccdid 
    --outfile
    """
    CAL = pathlib.Path(CAL_DIR)

    FLAT = CAL / "flat"
    if outfile is None : 
        outfile = FLAT
    else : 
        outfile = pathlib.Path(outfile)

    all_meta_path = glob.glob(CAL_DIR+"/flat/meta/"+f'uncut_masterflat_metadata_*.parquet')
    metaflats = pd.concat([pd.read_parquet(meta_path) for meta_path in all_meta_path])
    meta_list = metaflats[metaflats.FILTRKEY.eq(filterkey) & metaflats.CCDID.eq(ccdid)]
    dates_ = meta_list.PERIOD.map(lambda x : x[:4]+'-'+x[4:6]+'-'+x[6:]).values.astype('datetime64')
    meta_list['date'] = dates_
    meta_list = meta_list[((~meta_list.date.between(np.datetime64('2022-04-01'), np.datetime64('2022-04-05'), inclusive='both')) &
                            (meta_list.date.between(np.datetime64('2018-03-01'), np.datetime64('2025-12-31'), inclusive='both')))]

    meta_list['period_id'] = pd.cut(meta_list.date, period_from_logs, right=False, include_lowest=True, labels=False)

    for pid in perid :
        
        metatmp = meta_list[meta_list.period_id.eq(int(pid))]
        metatmp["filepath"] = metatmp.apply(lambda x : get_daily_flatfile(x['PERIOD'], x['CCDID'], filtername=x['FILTRKEY']), axis=1)
        metatmp = metatmp.sort_values(by='date')
        fpaths = metatmp["filepath"].values

        dfs = []
        ii = 0
        for i in range(1, metatmp.shape[0]):
        
            f_N0 = fpaths[0] #Init flat of period
            f_N1 = fpaths[i]

            #if i > 1 : 
            #    img0 = copy(img1)
            #    img1 = ztfimg.CCD.from_filename(f_N1)
            #    #img1 = img1.get_data()
            #else : 
            if i == 1:
                img0 = ztfimg.CCD.from_filename(f_N0)
                #img0 = img0.get_data()
                
            img1 = ztfimg.CCD.from_filename(f_N1)
            #img1 = img1.get_data()

            for j, (quad1, quad0) in enumerate(zip(img1.get_quadrantdata(), img0.get_quadrantdata()),
                                             start=1):
                #f1 = np.flatten(img1/img0) #Initial stuff 
                f1 = quad1/quad0 #Initial stuff 
                f1 = f1.flatten().copy()
                data = get_all_stats(f1)
                data.update(get_all_stats(f1-1, suffix='_diffexp'))
    
                data['date_n'] = metatmp['date'].iloc[0]
                data['date_n1'] = metatmp['date'].iloc[i]
    
                data['filepath_n'] = metatmp['filepath'].iloc[0]
                data['filepath_n1'] = metatmp['filepath'].iloc[i]

                data['period_id'] = int(pid)
    
                data['FILTRKEY'] = filterkey
                data['CCDID'] = ccdid                

                data['qid'] = j
                
                df = pd.DataFrame(data=data, index=[ii])
                dfs.append(df)
                ii +=1

        dfs = pd.concat(dfs)
        dfs.to_parquet(outfile / 'QA' /f'QA_masterflats_{pid}_{filterkey}_{ccdid}.parquet')


@click.command(context_settings={"show_default": True})
@click.argument("perid", nargs=-1) 
@click.option("--ccdid", type=int, help="ccdid to process")
@click.option("--outfile", default=None, help="filepath")
def QA_bias(perid, ccdid, outfile=None):
    """Quality analysis for a list of bias for a given year and a given CCD.

    \b
    Process perid (int):
    --ccdid 
    --outfile
    """
    
    CAL = pathlib.Path(CAL_DIR)
    BIAS =   CAL / "bias"
    if outfile is None : 
        outfile = BIAS
    else : 
        outfile = pathlib.Path(outfile)

    all_meta_path = glob.glob(CAL_DIR+"/bias/meta/"+f'uncut_masterbias_metadata_*.parquet')
    metaflats = pd.concat([pd.read_parquet(meta_path) for meta_path in all_meta_path])
    meta_list = metaflats[metaflats.CCDID.eq(ccdid)]
    dates_ = meta_list.PERIOD.map(lambda x : x[:4]+'-'+x[4:6]+'-'+x[6:]).values.astype('datetime64')
    meta_list['date'] = dates_
    meta_list = meta_list[((~meta_list.date.between(np.datetime64('2022-04-01'), np.datetime64('2022-04-05'), inclusive='both')) &
                            (meta_list.date.between(np.datetime64('2018-03-01'), np.datetime64('2025-12-31'), inclusive='both')))]

    meta_list['period_id'] = pd.cut(meta_list.date, period_from_logs, right=False, include_lowest=True, labels=False)
        
    for pid in perid : 

        metatmp = meta_list[meta_list.period_id.eq(int(pid))]
        metatmp["filepath"] = metatmp.apply(lambda x : get_daily_biasfile(x['PERIOD'], x['CCDID']), axis=1)
        metatmp = metatmp.sort_values(by='date')
        fpaths = metatmp["filepath"].values

        dfs = []
        for i in range(1, metatmp.shape[0]):
            f_N0 = fpaths[0] #Init bias of period  
            f_N1 = fpaths[i]
            
            if i == 1:
                img0 = ztfimg.CCD.from_filename(f_N0)
                #img0 = img0.get_data()
                
            img1 = ztfimg.CCD.from_filename(f_N1)
            #img1 = img1.get_data()

            for j, (quad1, quad0) in enumerate(zip(img1.get_quadrantdata(), img0.get_quadrantdata()),
                                             start=1):
                
                f1 = quad1 - quad0 #Initial stuff 
                f1 = f1.flatten().copy()
                data = get_all_stats(f1)
    
                data['date_n'] = meta_list['date'].iloc[0]
                data['date_n1'] = meta_list['date'].iloc[i]
    
                data['filepath_n'] = meta_list['filepath'].iloc[0]
                data['filepath_n1'] = meta_list['filepath'].iloc[i]
    
                data['CCDID'] = ccdid                
                data['period_id'] = int(pid)

                data['qid'] = j
                
                df = pd.DataFrame(data=data, index=[i+j])
                dfs.append(df)

        dfs = pd.concat(dfs)
        dfs.to_parquet(outfile / 'QA' / f'QA_masterbias_{pid}_{ccdid}.parquet')
