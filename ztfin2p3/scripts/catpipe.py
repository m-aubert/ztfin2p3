
import rich_click as click

import numpy as np
import pandas
import os

CATPIPE_DIR = "/sps/ztf/pipelines/catpipe"
METADATA_DIR = "/sps/ztf/data/storage/ubercal/metafiles/metafiles_per_field_per_year_per_filter/"

def metadata_to_catpipe_dir(meta):
    """ """
    return os.path.join(OUT_DIR, f"{meta['year']:4d}", meta["fieldid"], meta["filtername"])

def get_df_to_process(df, fields=None, years=None, filternames=None):
    """ """
    df = df.copy() # don't affect input catalog
    if fields is not None:
        df = df[ df["fieldid"].astype(int).isin(np.atleast_1d(fields).astype(int)) ]
    if years is not None:
        df = df[ df["year"].isin(np.atleast_1d(years).astype(int)) ]
    if filternames is not None:
        df = df[ df["filtername"].isin(np.atleast_1d(filternames).astype(str)) ]

    return df

def launch_catpipe_runs(dataframe):
    """ """
    import subprocess
    from tqdm import tqdm
    
    for i, col_ in tqdm(dataframe.iterrows(), total=len(dataframe)):
        target_dir = metadata_to_catpipe_dir(col_)
        #print(target_dir)
        # go to next directory
        os.chdir(target_dir)
        # launch the proc
        subprocess.run(["sbatch", "./run.sh"], capture_output=True)

def get_ztfprod_metadata():
    """ """
    from glob import glob

    allparquets = glob( os.path.join(METADATA_DIR, "*"))
    basename = [os.path.basename(l) for l in allparquets]
    
    df = pandas.DataFrame({"basename": basename})
    df_info = df["basename"].str.split("_", expand=True)[[1, 3, 5]]
    df["year"] = df_info[1].astype(int)
    df["fieldid"] = df_info[5].str.replace(".parquet","").astype(str).str.pad(6, fillchar="0")
    df["filtername"] = df_info[3].replace(["1", "2", "3"], ["ztfg", "ztfr", "ztfi"])
    return df
    
def grab_catpipe_results(dataframe):
    """ """
    from tqdm import tqdm
    results = []
    # this format for tqdm
    for i, col_ in tqdm(dataframe.iterrows(), total=len(dataframe)):
        target_dir = metadata_to_catpipe_dir(col_)
        has_result = os.path.isdir( os.path.join(target_dir, "results") )
        results.append( has_result )
        
    return results
        
    
# =================== #
#
#    PIPELINE         #
#
# =================== #


@click.command(context_settings={"show_default": True})
@click.option("--years", help='select the year(s) you want: e.g., --year 2020,2021 ; None means all', default=None)
@click.option("--fields", help='select the field(s) you want: e.g., --field 600 ; None means all', default=None)
@click.option("--filters", help='select the filter(s) you want: e.g., --filter r ; None means all', default=None)
def catpipe(years, fields, filters):
    """Parse calibration folder to produce catalogs."""
    METADATA_DF = get_ztfprod_metadata()
    df_to_process = get_df_to_process(METADATA_DF, fields=fields, years=years, filternames=filters)
    print(f"{len(df_to_process)} to process")
    return launch_catpipe_runs(df_to_process)
