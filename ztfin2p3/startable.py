""" Tools for Ubercal analyses. 

Example:
--------
usim = UbercalSimulator.from_simsample( int(1e5) )
ucal = usim.draw_ubercal(1000)

ubercal = Ubercal.from_dataframe(ucal, min_exp=3)
x_solution = ubercal.solve(ref_expid=0, method="cholmod")

"""


import numpy as np
import pandas as pd

from scipy import sparse
from scipy.sparse import linalg as splinalg

import matplotlib.pyplot as plt
import matplotlib as mpl
import healpy as hp
from scipy import stats

import copy
mycm=copy.copy(plt.cm.viridis)
mycm.set_under('k')
mycm.set_over('w')
mycm.set_bad('grey')

def _build_index_df_(dataframe, inid, outid, minsize=None):
    """ """
    tmp_df = dataframe.groupby(inid, as_index=False).size()
    if minsize:
        tmp_df = tmp_df[tmp_df["size"]>=minsize].reset_index(drop=True)

    return dataframe.merge(tmp_df[inid].reset_index().rename({"index":outid}, axis=1), on=inid)


# =================== #
#                     #
#   STARTABLE         #
#                     #
# =================== #

class Startable( object ): 
    """Class to play with startables in preparation of ubercal.
     
    Parameters
    ----------
    data : pd.DataFrame
           Data should be a dataframe with some sort of source id, a ra/dec, flux and error, the index should be the image name
        Columns of the table:
      | columns
      | ├── Source, Gaia ID
      | ├── isolated, flag for source isolation
      | └── ...
    """
    
    
    def __init__(self,data):
        print('Be careful, your input dataframe might be modified if you modify .data (but too heavy for copy)')
        self._data = data
        self._ztf_filter = None
        self.starflats = None
        
    def init_starflats(self):
        starflats=np.zeros((16,4,3072,3080))
        self.starflats=starflats  
        
    @property
    def ztf_filter(self):
        if self._ztf_filter is None:
            print('call set_filter before')
            return None
        return self._ztf_filter
    
    def set_filter(self, ztf_filter):
        self._ztf_filter = ztf_filter

    def add_column(self,col_names,metatable=None,column_from='filefracday',type_='float64'):
        """
        Adding columns to the startable or ubercal, using external columns from metatable if provided
        """
        col_names=np.atleast_1d(col_names)
        for names in col_names:
            if not hasattr(self,f"get_{names}"):
                col_to_fill = self.map_from_metatable(metatable,names,column_from)
                if col_to_fill is not None: 
                    self._data[names] = self.map_from_metatable(metatable,names,column_from)
            elif names in ['expid','pid']:
                self._data[names] = getattr(self,f"get_{names}")(metatable)
            else:
                self._data[names] = getattr(self,f"get_{names}")()
            if type_!=None:
                self._data.astype({names: type_},copy=False)

    def map_from_metatable(self,metatable,column_to_get,column_from='filefracday'):
        """
        Will map a columns from a metatable, for instance if your catalogs has a filefracday or a pid information, you can get the seeing for all these pids. 
        """
        if metatable is None: 
            print("no metatable provided, can't help you")         
        else:
            mapping_dict_col = metatable[[column_from,column_to_get]].drop_duplicates(column_from).set_index(column_from).to_dict()[column_to_get]
            return self._data[column_from].astype(int).map(mapping_dict_col) 
        

    def get_expid(self,metatable=None):
        print('deprecated since we do not use multicolumn index based on filename anymore, probably now have pid stored, so just remove the last 2 digits from it')

        if metatable is None:
            print("no metatable provided, we are here using filefracday as expid, since there is a 1 to 1 matching")
            return self._data.filefracday.astype(int)
        else:    
            mapping_dict_expid = dict(zip(metatable.filefracday,metatable.expid))
            return self._data.filefracday.astype(int).map(mapping_dict_expid)

    
    def rcid_to_ccdid_qid(self):
        self._data['qid'] = (self._data.rcid%4)+1
        self._data['ccdid']  = int((self._data.rcid-(self._data.qid - 1))/4 +1)
    
    def get_pid(self,metatable=None):
        """
        pid is is a quadrant/time unique id, like expid but at quadrant resolution (it is in the metatable, but )
        """
        print('deprecated since we do not use multicolumn index based on filename anymore')

        if "expid" not in self._data.columns:
            expid=self.get_expid(metatable)
        else: 
            expid=self._data['expid']
        if "ccdid" not in self._data.columns:
            ccdid=self.get_ccdid()
        else: 
            ccdid=self._data['ccdid']
        if "qid" not in self._data.columns:
            qid=self.get_qid()
        else: 
            qid=self._data['qid']
        return expid*100+(ccdid-1)*4+(qid-1)
        

    def add_starflat_column(self,which_photometry='AP',extra_string='',dir_path_fits_ztf="/sps/ztf/data/ztfin2p3/cal/starflat/2019_newscript/03/"):
        """
        This function is using Estelle's fits files. 
        For now we haven't designed a storage for the 'uberflats' etc.
        """
        
        print('for now no interpolation, just read in rounded xy pos, can take a couple of minutes to loop over all quadrants')
        if self._ztf_filter is None:
            print('call init_filter before')
            return None
        list_quadrant_ccd_qid=np.array(self._data[['ccdid','qid']].drop_duplicates())
        # more pandaesque but failing for dask...
        #self._data[['ccdid','qid']].drop_duplicates().to_numpy()
        for ccdid,qid in list_quadrant_ccd_qid:
            quadrant_mask=(self._data['qid']==qid)& (self._data['ccdid']==ccdid)
            import scipy.interpolate
            starflat_arr=self.get_starflat_corr(qid,ccdid,self.ztf_filter,which_photometry,dir_path_fits_ztf)
            self._data.loc[quadrant_mask,f'corr_{which_photometry}{extra_string}']=starflat_arr[self._data.x[quadrant_mask].values.round(0).astype(int),self._data.y[quadrant_mask].values.round(0).astype(int)]
     
    
    def add_starflat(self,which_photometry='AP',ccdid=None,qid=None,
                         dir_path_fits_ztf="/sps/ztf/data/ztfin2p3/cal/starflat/2019_newscript/03/"):
        """ 
        not sure if this function was used in the last run. Keeping it for now. 
        """
        if self.starflats is None: 
            print('call init_starflats before')
            return None
        print('for now no interpolation, just read in rounded xy pos, can take a couple of minutes to loop over all quadrants')
        if self._ztf_filter is None:
            print('call init_filter before')
            return None
        
        if ccdid is None: 
            ccdid=np.arange(1,17)
        if qid is None: 
            qid=np.arange(1,5)
            
        ccdid=np.atleast_1d(ccdid)
        qid=np.atleast_1d(qid)
        #list_quadrant_ccd_qid=[[ccdid_,qid_] for ccdid_ in ccdid for qid_ in qid]
        #list_quadrant_ccd_qid=self._data[['ccdid','qid']].drop_duplicates().to_numpy() 
        for ccdid_ in ccdid: 
            for qid_ in qid:
                starflat_arr=self.get_starflat_corr(qid_,ccdid_,self.ztf_filter,which_photometry,dir_path_fits_ztf)
                self.starflats[ccdid_-1,qid_-1,:,:] = starflat_arr
    
    def add_starflat_mean(self,which_photometry='AP',
                              dir_path_fits_ztf="/sps/ztf/data/ztfin2p3/cal/starflat/2019_newscript/03/"):
        """
        Never used, but should return the quadrant mean of the starflats. 
        """
        if which_photometry=='AP':
            if not hasattr(self,f"{which_photometry}_starflat_mean"): 
                setattr(self,f"{which_photometry}_starflat_mean",np.zeros((16,4)))
        print('for now no interpolation, just read in rounded xy pos, can take a couple of minutes to loop over all quadrants')
        if self._ztf_filter is None:
            print('call init_filter before')
            return None
        ccdid=np.arange(1,17)
        qid=np.arange(1,5)
        all_means=np.zeros((16,4))
        for ccdid_ in ccdid: 
            for qid_ in qid:
                starflat_arr=self.get_starflat_corr(qid_,ccdid_,self.ztf_filter,which_photometry,dir_path_fits_ztf)
                all_means[ccdid_-1,qid_-1]=starflat_arr.mean()
        setattr(self,f"{which_photometry}_starflat_mean",all_means)
    
    
    def add_uv_pix(self,bin_width_x=48*4*2,bin_width_y=40*11,verbose=True):
        """
        Function binning the uv plane. 
        Here it is based on the quadrant xy, so we call them x and y bins, not u/b, since uv has a sign change etc. 
        But here we only need a unique pixel id for a given bin on the focal plane.
        """
        n_pix_x = 3072
        n_pix_y = 3080
        if verbose:print(f'bin_width_x={bin_width_x}, n_bin_x = {n_pix_x/bin_width_x:.2f}')
        if verbose:print(f'bin_width_y={bin_width_y}, n_bin_y = {n_pix_y/bin_width_y:.2f}')
        self._data['x_bin']=(self._data.x//bin_width_x).astype('int')#*bin_width_x
        self._data['y_bin']=(self._data.y//bin_width_y).astype('int')#*bin_width_y
        pix_id=(self._data.x_bin)*n_pix_y/bin_width_y+self._data.y_bin
        # offseting the pixels so they are different for all rcid
        self._data['pix']=pix_id+self._data.rcid.astype(np.int64)*n_pix_y/bin_width_y*n_pix_x/bin_width_x
        self._data=self._data.astype({'pix': np.uint32},copy=False)
        self._data=self._data.drop(columns=['x_bin', 'y_bin'])

        self._bin_width_x = bin_width_x
        self._bin_width_y = bin_width_y
        self._n_uv_pix = np.uint32(n_pix_y/bin_width_y * n_pix_x/bin_width_x)

    @staticmethod
    def get_starflat_corr(qid,ccdid,filtercode,which_photometry='AP',
                        dir_path_fits_ztf="/sps/ztf/data/ztfin2p3/cal/starflat/2019_newscript/03/"):
        """
        Will need to define a time_validity for the starflat. For now hard coded the march SF
        
        """
        from astropy.io import fits
        print('Will need to define a time_validity for the starflat. For now hard coded the march 2019 SF')
        year=2019

        if which_photometry == "psf":
            phot="psfstarflat"
        elif which_photometry == "AP":
            phot="apstarflat"

        file_corr = f'{dir_path_fits_ztf}ztfin2p3_{year}03_000000_{filtercode}_c{ccdid:02}_q{qid}_{phot}.fits'
        with fits.open(file_corr) as fits_blocks:
            block = fits_blocks[0]
            header = block.header
            pixel = block.data
            pixel = pixel[::-1,::-1].T #correction du sens de la map
        return pixel

        
    def add_airmass(self,site='Palomar',type_='float32',groupby_='obsjd',airmass_name='airmass_calc'):
        """
        Function to add an airmass columns to the table.
        This function will require an obsjd, which you can get from the metatable.
        Had issues with this function in the past. It was reordering and messing things up. 
        This new DataFrame with a fixed index seemed to do the trick...
        In doubt, plot airmass vs dec. 
        """
        def add_airmass_calc(ser):
            """
            Function for groupby apply, maybe not the most efficient...
            """
            return pd.DataFrame({'airmass_calc':Get_airmass(ser.ra.values,ser.dec.values,ser.name,site=site)},index=ser.index,dtype=type_)# 
        airmass_df=self._data.groupby([groupby_]).apply(add_airmass_calc)
        self._data[airmass_name]=airmass_df.airmass_calc
    
        
    def clean_table(self,verbose=True,dropna=True):
        """
        function to remove nans comming for instance from psf photometry, or from mags for negative AP fluxes.
        the dropna is very inefficient memorywise, seems to make a copy even when inplace=True.
        Consider using clean_table on subset before merging. I might crash on very large table. 
        """
        if verbose:size_before=len(self._data)
        self._data.dropna(inplace=True)
        self._data.reset_index(drop=True,inplace=True)
        if verbose:print(f'After nan removal, went from {size_before} to {len(self._data)} lines')    
                        
    def add_magnitudes(self, flux_AP, starflat_corr_AP = 'corr_AP', extra_string='', do_psf=True):
        """
        For ubercal we work with magnitudes. This function goes from flux to magnitude, including starflat correction if needed. 
        """
        # adding AP mag
        self._data[f"mag_AP{extra_string}"]=-2.5*np.log10(self._data[flux_AP])
        self._data[f"e_mag_AP{extra_string}"] = (2.5/np.log(10) * self._data[f'{flux_AP}_e']/self._data[flux_AP]).values
        # adding AP mag with starflat correction, note here that we don't have yet any error on starflats, so we don't have a specific error propagation
        if starflat_corr_AP!=None:
            self._data[f"mag_AP_corr{extra_string}"]=-2.5*np.log10(self._data[flux_AP])-self._data[starflat_corr_AP]

        if do_psf:
            # adding psf mag
            self._data[f"mag_psf"]=-2.5*np.log10(self._data['flux'])
            self._data[f"e_mag_psf"] = (2.5/np.log(10) * self._data["sigflux"]/self._data["flux"]).values
            # adding AP mag with starflat correction, note here that we don't have yet any error on starflats, so we don't have a specific error propagation
            if "corr_psf" in self._data.columns:
                self._data["mag_psf_corr"]=-2.5*np.log10(self._data['flux'])-self._data['corr_psf']
                
                
    def keep_columns(self,col_list=['ra', 'dec', 'Source', 'ps1_id', 'rmag', 'x', 'y', 'flux', 'sigflux',
       'isolated', 'f_0', 'f_1', 'f_2', 'f_3', 'f_0_e', 'f_0_f', 'f_1_f',
       'f_2_f', 'f_3_f', 'filefracday', 'field', 'filterid', 'ccdid', 'qid',
       'f_1_e', 'SNR_1', 'expid', 'pid']):
        """
        oneline probably useless function to keep only a set of columns
        """
        self._data=self._data[col_list]
    
    def remove_infobits(self, metatable,verbose=False):
        """
        Function that matches startable to a log metatable to remove the quadrants with non 0 infobits 
        Should check at some point if some non 0 infobits can be kept. For now being conservative. 
        """
        if "expid" not in self._data.columns:
            self._data['expid']=self.get_expid(metatable=metatable)
        if "pid" not in self._data.columns:
            self._data['pid']=self.get_pid(metatable)
        if verbose:len_before_cut=len(self._data)
        if np.log10(metatable['pid']).astype('int').max()==11:
            self._data=self._data[self._data.pid.isin(metatable[metatable.infobits==0].pid.apply(lambda pid: pid //100))]
        else:
            self._data=self._data[self._data.pid.isin(metatable[metatable.infobits==0].pid)]
                                                      
                                                           
        if verbose: print(f'keeping {len(self._data)} with infobits==0, i.e. {len(self._data)/len_before_cut*100:.01f}% ')
        
    def filter_catalog(self,n_cpu=100, SNR_cut=False, rmag_cut=False, verbose=False, AP_radii = [4, 6, 8, 10], radius_index=1):
        """ 
        df can be a dask dataframe of a panda dataframe here. 
        SNR_cut=False for no cut, SNR_cut=some integer for a cut on the AP SNR (will bias to some extent, to be checked)
        """
        self._data
        if verbose: 
            total_n_sources=len(self._data)
            print(f'full catalog has {total_n_sources} sources')
        if verbose: self._data.info(memory_usage=True)

        self._data[f'f_{radius_index}_e']=(AP_radii[radius_index]/AP_radii[0]*self._data.f_0_e)
        self._data[f'SNR_{radius_index}']=(self._data[f'f_{radius_index}']/self._data[f'f_{radius_index}_e'])
        if rmag_cut!=False:
            self._data=self._data[self._data['rmag']<rmag_cut]
        if verbose: 
            print(f'keeping {len(self._data)} sources with rmag<{rmag_cut}, i.e. {len(self._data)/total_n_sources*100:.01f}%')
            total_n_sources=len(self._data)

        if SNR_cut!=False:
            self._data=self._data[self._data[f'SNR_{radius_index}']>SNR_cut]
        if verbose: 
            print(f'keeping {len(self._data)} sources with SNR>{SNR_cut}, i.e. {len(self._data)/total_n_sources*100:.01f}%')
            total_n_sources=len(self._data)


        self._data=self._data[self._data.isolated]

        if verbose: 
            isolated_n_sources = len(self._data)
            print(f'keeping {isolated_n_sources} isolated sources, i.e. {isolated_n_sources/total_n_sources*100:.01f}%')
        self._data=self._data[self._data.f_1_f==0]
        if verbose: 
            isolated_unflagged_n_sources = len(self._data)
            print(f'keeping {isolated_unflagged_n_sources} unflagged (sep) sources, i.e. {isolated_unflagged_n_sources/isolated_n_sources*100:.01f}% of isolated sources')
        #return df
    

    def filter_star_nobs(self, starid="Source", min_source=3):
        """ load the object given a dataframe of observation.
        
        The dataframe must be single index and must contain the column 
           - mag # observed magnitude 
           - e_mag # error on observed magnitude
           - and the starid and zpid columns (see option). 
             These represents the individual star and exposure id.
             
        Parameters
        ----------
        dataframe: [pd.DataFrame]
            dataframe containing the observations. 
            must contain mag and e_mag columns
            
        starid -optional-
            name of the star id in the input dataframe.
        
        min_exp: [int or None] -optional-
            minimum number of observations for a star to be considered.
            If None, no cut is made.
            
        Returns
        -------
        instance of Object
        """
        
        tmp_df = self._data.groupby(starid, as_index=False).size()
        if min_source:
            tmp_df = tmp_df[tmp_df["size"]>=min_source].reset_index(drop=True)
        return self._data[self._data[starid].isin(tmp_df[starid])]
    
    
    # =============== #
    #   Properties    #
    # =============== #
    @property
    def data(self):
        """ startable data """
        print('warning, data is a dataframe which is not protected by @property')
        if not hasattr(self,"_data"):
            return None
        return self._data
    
    def has_data(self):
        """ test if data has been set. """
        return self.data is not None
        
    @property
    def filenames(self):
        """ startable index = filenames """
        if not hasattr(self,"_data"):
            return None
        return self._data.index.get_level_values(0)
        
        
        
        
        
# =================== #
#                     #
#   UBERCAL           #
#                     #
# =================== #    
        

class Ubercal( Startable ):
    # these are not used in the current version of the code, but kept so that Mickael's initial code still works at least to initilize ubercal
    STARID = "u_starid"
    ZPID = "u_zpid"
    RCID = "u_rcid"
    PIXID = "u_pixid"
    MAGID = "mag"
    EMAGID = "e_mag"
    
    def __init__(self, data=None):
        """ This should not be called directly for the data format is tricky.
        See from_exposuredict() or from_dataframe()
        """
        if data is not None:
            self.set_data(data)
        
            
    # =============== #
    #   I/O           #
    # =============== #
    @classmethod
    def from_exposuredict(cls, exposure_dict, starid="starid", min_exp=3):
        """ load the object given a dictionary with the following format:
            
        exposure_dict:
            {exposure_i: {'starid':[id_1, id_2, ..], 'mag':[obsmag_1, obsmag_2], 'e_mag':[errmag_1, errmag_2],
             exposure_j: {'starid':[id_2, id_4, ..], 'mag':[obsmag_2, obsmag_4], 'e_mag':[errmag_2, errmag_4],
             exposure_k: {'starid':[id_1, id_4, ..], 'mag':[obsmag_1, obsmag_4], 'e_mag':[errmag_1, errmag_4],
             ...
            }
            
        This calls the from_dataframe() classmethod.
        
        
        Parameters
        ----------
        exposure_dict: [dict]
            dictionary containing the observations (see format above).
            
        starid: [string] -optional-
            name of the star id in the input dictionary.
            The internal index (0->nstar) set internally independently.
        
        min_exp: [int or None] -optional-
            minimum number of observations for a star to be considered.
            If None, no cut is made.
            
        Returns
        -------
        instance of Object
        """
        data = pd.DataFrame.from_dict(exposure_dict, orient="index").apply(pd.Series.explode)
        return cls.from_dataframe( data.reset_index().rename({"index":"zpid"}, axis=1),
                                   starid=starid, min_exp=min_exp )
    
    @classmethod
    def from_dataframe(cls, data, starid="starid", zpid="expid", min_exp=3):
        """ load the object given a dataframe of observation.
        
        The dataframe must be single index and must contain the column 
           - mag # observed magnitude 
           - e_mag # error on observed magnitude
           - and the starid and zpid columns (see option). 
             These represents the individual star and exposure id.
             
        Parameters
        ----------
        dataframe: [pd.DataFrame]
            dataframe containing the observations. 
            must contain mag and e_mag columns
            
        starid, zpid: [string] -optional-
            name of the star and exposure id in the input dataframe.
            The internal index (0->nstar and 0->nexposures) set internally independently.
        
        min_exp: [int or None] -optional-
            minimum number of observations for a star to be considered.
            If None, no cut is made.
            
        Returns
        -------
        instance of Object
        """
        data = cls.shape_dataframe(data, starid=starid, zpid=zpid, min_exp=min_exp)
        return cls(data)
    

    
    @classmethod
    def shape_dataframe(cls, dataframe, min_exp=3, starid="starid", zpid="expid",rcid="rcid",fit_rcid=True):
        """ reshape the input dataframe to have the internal star and zpid index set. 
        It also selects only the stars that have at least min_exp different exposure observations.
        
        Parameters
        ----------
        dataframe: [pd.DataFrame]
            dataframe containing, at least, the observations (mag, e_mag) and 
            the corresponding star and exposure ids. 
            These can be any format, they will be converted into 0->nstar and 0->nexposures
            index internally by this method.
            
        min_exp: [int or None] -optional-
            minimum number of observations for a star to be considered.
            If None, no cut is made.
            
        starid, zpid: [string] -optional-
            name of the star and exposure id in the input dataframe.
            The internal index (0->nstar and 0->nexposures) set internally independently.
            
        Returns
        -------
        DataFrame
        """            
        dataframe = _build_index_df_(dataframe, inid=starid, outid=cls.STARID, minsize=min_exp)
        dataframe = _build_index_df_(dataframe, inid=zpid, outid=cls.ZPID)
        return dataframe
    
            
    def setup_ubercal(self,fit_opt_dict={"star_mag":True,"Zp":True,"rcid_off":False,"uv_pix_off":False,"k_eff":False}, 
                            ref_dict={"ref_star_mag":None,"ref_Zp":0,"ref_rcid_off":0,"ref_uv_pix_off":0,"ref_k_eff":None}, 
                            input_column_dict={"star_mag":"Source","Zp":"expid","rcid_off":"rcid","uv_pix_off":"pix","k_eff":None},
                            unique_id_dict={"star_mag":"u_starid","Zp":"u_zpid","rcid_off":"u_rcid","uv_pix_off":"u_pixid","k_eff":"u_kid"},
                            weight_dict={"star_mag":None,"Zp":None,"rcid_off":None,"uv_pix_off":None,"k_eff":"airmass_calc"},
                            re_setup=False):
                    
        """
        Function to setup the ubercal. 
        It will add to the startable a list of unique indices that will be used to build the acoo matrix. 
        Parameters
        ----------
        For all the following, the key of the dictionary are a list of parameter names.
            . star_mag is the actual fitted star magnitude
            . Zp is the zero point, could be based on exposure expid, quadrant pid, or any other defined column.
            . rcid_off is the quadrant offset, compared to a reference quadrant
            . uv_pix_off is a super pixel offset to do "uberflats"
            . k_eff is the extinction, fitted using airmass dependence of the the magnitude
        fit_opt_dict: [dictionary]
            This is an optional dictionnary of booleans to decide which options of the ubercal
            are going to be used
        ref_dict: [dictionary]
            Some of the parameter are relative and need a reference, for instance the zero point to 
            which all other ZP are going to be anchored on
        input_column_dict: [dictionary]
            The unique indices which are used in the matrix are base on another column in the table, given here
        unique_id_dict: [dictionary]
            The name of the unique index created, for now we are trying to keep them starting with u_
        weight_dict: [dictionary]
            In the ubercal matrix, in the simplest case we give unit weights to all entries, 
            modulo the noise W matrix. But for k for instance, the matrix is filled with the airmass 
        """    
        if re_setup: 
            self.data.drop(columns=list(self._unique_id_dict.values()))
            
        # keep track of everything    
        self._fit_opt_dict=fit_opt_dict
        self._input_column_dict=input_column_dict
        self._ref_dict=ref_dict
        self._weight_dict=weight_dict
        self._unique_id_dict=unique_id_dict
        self._time_dep_dict={}
        # this is for now hard coded. Will be used if we add a time dependence to the fit.
        self._n_param_pertime_dict={"star_mag":1,"Zp":1,"rcid_off":64,"uv_pix_off":self._n_uv_pix,"k_eff":1}
        
        for key in fit_opt_dict.keys():
            self._time_dep_dict[key]=None
            if fit_opt_dict[key]:
                if input_column_dict[key]==None:
                    # for the None case (for instance for extinction/airmass), we create a columns with index 0
                    # this can then be modified to add time dependence with add_time_dependence
                    self._data[unique_id_dict[key]]=0
                    
                    #self._data=self._data.assign(unique_id_dict[key], 0)
                else:
                    self._data = _build_index_df_(self._data, inid=input_column_dict[key], outid=unique_id_dict[key])
                self._data=self._data.astype({unique_id_dict[key]:np.int32},copy=False)
        
    def set_ref_pix_list(self):
        """
        get the list of reference pixels in each quadrant, this is necessary because some of the pixels might not be observed in a given dataset, so using the 1st pixel of a quadrant is not stable.
        Here we hardcoded the names 'rcid', u_pixid etc, but could be extended
        """
        ref_pix_list=list(self._data.groupby('rcid').u_pixid.min())
        self._ref_dict["ref_uv_pix_off"]=ref_pix_list    

        
    def add_time_dependence(self,param,time_index_list,time_string='time',offset=1):
        '''
        param is the name of the parameter which we want to modify to add a time dependence, for instance uv_pix_off, k_eff
        time_index_list is a set of different time values for instance month number, of the same size as the self._data dataframe. Note that it would work the same for somehting else than time (seeing, illumination etc)
        time_string will be used to create a new unique index for this given time. Can for instance call it month
        '''
        self._time_dep_dict[param]=time_string
        name_time_index = f'u_{time_string}_index'
        self._data['time_list'] = time_index_list
        if name_time_index in self._data.columns:
            print(f'time string {time_string} already in the dataframe, so we use it without creating a new one. If you want a different one, change the name.')
        else:
            self._data=_build_index_df_(self._data, 'time_list', name_time_index, minsize=None)
        # let's not keep this
        self._data=self._data.drop(columns='time_list')

        offsets=self._n_param_pertime_dict[param]*self._data[name_time_index]

        new_index=self._data[self._unique_id_dict[param]] + offsets

        new_index_df=new_index.to_frame(name="offid")

        # here we offset the input index so that there is a specific one for a given time slice
        self._data[self._unique_id_dict[param]] = _build_index_df_(new_index_df,"offid",'new_offid').new_offid
    

        
    def get_save_string(self):
        """
        Function to create a string from ubercal options, useful for saving for instance
        """
        save_string=''
        for key in self._fit_opt_dict:
            if self._fit_opt_dict[key]:
                if self._time_dep_dict[key]!=None:
                    time_string=f'{self._time_dep_dict[key]}_'
                else:
                    time_string=''
                save_string = f'{save_string}_{time_string}{key}'
                
                if key=='uv_pix_off':
                    save_string = f'{save_string}_{self._bin_width_x}x{self._bin_width_y}'

        return save_string    
        
    def display_ubercal_stats(self):
        """
        Function that will show the number of paramers used in the fit
        quick and dirty as everything, many more things could be added here. 
        """
        total_obs=len(self._data)
        print(f"The startable has {total_obs} lines")

        for key in self._fit_opt_dict:
            if self._fit_opt_dict[key]:
                n_param = self._end_index_dict[key]-self._init_index_dict[key]+1
                print(f"{key} has {n_param} parameters")


    
    # =============== #
    #   Methods       #
    # =============== #
    # ------- #
    # SETTER  #
    # ------- #
    def set_data(self, data):
        """ Sets the ubercal dataframe. 
        
        Most lilely you should not use this method directly, 
        the input data must have a very particular structure.
            
        In case of doubt see the class method from_dataframe().
        """
        self._data = data
        self._acoo = None
        self._ref_zpid = None
        self._n_uv_pix = None

    # ------- #
    # GETTER  #
    # ------- #

    
    def get_wmatrix(self, emagid=None, rebuild=False):
        """ get (or build) the weight matrix. 
        
        The weight matrix is a sparse diagonal matrix. 
        The diagonal elements are 1/mag_err**2
        """
        if not rebuild:
            wmat = self.wmatrix
        else:
            wmat = sparse.diags(1/np.asarray(self.data[emagid], dtype="float")**2)
            
        return wmat
   
    # ------- #
    # BUILDER #
    # ------- #
    def build_acoo(self):
        """ get (or rebuild) the model sparse matrix (a in a•x=b)
        
        The sparse matrix is a M x N matrix with, 
            - M = number of observations
            - N = numer of stars + number of exposures - 1 + number of uv cells -1. 
        and is sorted such that the stars are first and then the magnitude zp.
        
        Parameters
        ----------
        rebuild: [bool] -optional-
            if the matrix has already been constructed (see self.acoo), should this use 
            it or measure it ? (True means the matrix is re-measured).
            
        Returns
        -------
        scipy Sparse Matrix (coo)
        """

        if self._fit_opt_dict["star_mag"]==False:
            print('Fit without star magnitude not implemented')
            
        if self._fit_opt_dict["Zp"]==False:
            print('Fit without Zp not implemented')

        init_index_dict={}
        end_index_dict={}    
            
        offset=0
        matrix_values=[]
        coo=pd.DataFrame([])
        
        for key in self._fit_opt_dict.keys():
            if self._fit_opt_dict[key]:
                init_index_dict[key]=offset

                if coo.empty:
                    coo=self.data[self._unique_id_dict[key]]+offset
                else:
                    coo=pd.concat([coo,self.data[self._unique_id_dict[key]]+offset])
                #coo.append(self.data[self._unique_id_dict[key]].values+offset)
                end_index_dict[key]=len(self.data[self._unique_id_dict[key]].unique())+offset-1
                offset=list(end_index_dict.values())[-1]+1
                if self._weight_dict[key]==None:
                    matrix_values.append(np.ones(len(self.data)))
                else: 
                    matrix_values.append(self.data[self._weight_dict[key]].values)
                
        #coo=np.concatenate(coo)
        
        #coo = pd.DataFrame(coo,dtype="int")
        matrix_values=np.concatenate(matrix_values)        
        acoo = sparse.coo_matrix((matrix_values, 
                     (np.asarray(coo.index, dtype="int"), 
                      np.asarray(coo.values, dtype="int"))))

        self._acoo = acoo
        self._init_index_dict = init_index_dict
        self._end_index_dict = end_index_dict
        
    # ------- #
    # SOLVER  #
    # ------- #    
    def solve(self, magid = None, emagid = None, rebuild=False, method="cholmod", ordering_method='metis', use_long=None, beta = 0, mode = "auto"):
        """ Solve for X in A•X = B.

        This method include variance, so it actually solves for
             A^t @ C @ A • X = A^T @ C • B
             
        
        Parameters
        ----------
        ref_zpid: [int]
            id of the exposure used as reference.
            Star magnitude will be in unit of this.
            Other zp will be offset with respect to it.
            
        method: [string] -optional-
            Method used to solve the linear system.
            - cholmod: uses cholmod (cholesky() then factor())
            - lsqr: uses scipy.sparse.linalg.lsqr()
            - spsolve: uses scipy.sparse.linalg.spsolve() # but super slow !            
            [No other method implemented]
            
        Returns
        -------
        whavether the model returns:
        - x for spsolve and cholmod
        - x (and more) for lsqr
        """

        self.build_acoo()

        #b = np.asarray(self.data[self.MAGID].values, dtype="float")
        b = np.asarray(self.data[magid].values, dtype="float")
        # set the reference exposure
        mask = np.ones(self.acoo.shape[1],dtype='int')

        for key in list(self._init_index_dict.keys()):
            ref_list=np.atleast_1d(self._ref_dict[f'ref_{key}'])
            for ref_i in ref_list:
                if ref_i!=None:
                    mask[self._init_index_dict[key]+ref_i] = 0

        acoo_ref = self.acoo.tocsr()[:,np.asarray(mask, dtype="bool")]

        
        # include covariance 
        wmatrix = self.get_wmatrix(emagid=emagid,rebuild=rebuild)
        atw_ref = acoo_ref.T @ wmatrix
        
        if method == "lsqr":
            return splinalg.lsqr(atw_ref @ acoo_ref, atw_ref.dot(b) )    
        
        if method == "spsolve":
            return splinalg.spsolve(atw_ref @ acoo_ref, atw_ref.dot(b) )
            
        if method == "cholmod":
            from sksparse.cholmod import cholesky
            factor = cholesky(atw_ref @ acoo_ref, beta, mode, ordering_method, use_long )            
            return factor( atw_ref.dot(b) )
            
        raise NotImplementedError(f"Only 'lsqr', 'spsolve' and 'cholmod' method implemented ; {method} given")
        
        
    def save_pre_solve(self, ref_expid, fit_rcid=True, magid=None, emagid = None, rebuild=True, string_added='',dirname = 'saved_for_solve'):
        """ 
        This function is outdated but should be rewritten for production. 
        This is based on the "solve" function, but saves the input of the large cholesky
        This is very useful for super large cases, where you better start from a clean environment
        before solving. There are probably ways to just improve memory usage, here we chose i/o solution. 
        """
        import scipy.sparse
        self.build_acoo(fit_rcid=fit_rcid)
        
        # set the reference exposure
        mask = np.ones(self.acoo.shape[1])
        mask[ref_expid + self.nstars] = 0
        acoo_ref = self.acoo.tocsr()[:,np.asarray(mask, dtype="bool")]
        self._ref_expid = ref_expid
        
        
        # include covariance 
        # include covariance 
        wmatrix = self.get_wmatrix(emagid=emagid,rebuild=rebuild)
        
        scipy.sparse.save_npz(f'{dirname}/weight_matrix_{string_added}.npz',wmatrix,compressed=True)
        scipy.sparse.save_npz(f'{dirname}/acoo_ref_{string_added}.npz',acoo_ref, compressed=True)
        if magid is None:
            magid=self.MAGID
        #b = np.asarray(self.data[self.MAGID].values, dtype="float")
        b = np.asarray(self.data[magid].values, dtype="float")
        np.save(f'{dirname}/b_{string_added}.npy',b)
        
    
    @staticmethod
    def solve_from_precomputed(b, acoo_ref,wmatrix, method="cholmod",beta=0, mode='auto', ordering_method='metis', use_long=None):
        """ 
        This function is also outdated but should be rewritten for production.
        goes with "save_pre_solve" and should be the other part of "solve"
        """
        atw_ref = acoo_ref.T @ wmatrix
        
        if method == "lsqr":
            return splinalg.lsqr(atw_ref @ acoo_ref, atw_ref.dot(b) )    
        
        if method == "spsolve":
            return splinalg.spsolve(atw_ref @ acoo_ref, atw_ref.dot(b) )
            
        if method == "cholmod":
            from sksparse.cholmod import cholesky
            factor = cholesky(atw_ref @ acoo_ref,beta, mode, ordering_method, use_long )
            return factor( atw_ref.dot(b) )
            
        raise NotImplementedError(f"Only 'lsqr', 'spsolve' and 'cholmod' method implemented ; {method} given")
            

    # =============== #
    #     Results     #
    # =============== #
    def set_solution(self, magid=None, emagid=None, rebuild=False, method="cholmod", use_long=None, string_added='',verbose=True):
        '''
        This function solves the ubercal and adds the required parameter columns to the table.
        
        '''
        if verbose:print("now creating the matrix and solving ubercalibration, this could take a while")

        solved=self.solve(magid=magid, emagid=emagid, method=method, rebuild=rebuild, use_long=use_long)
        
        if verbose:print("Done, now adding the solutions to the table")

        # initialize here, the fitted parts will be removed in the following loop.
        self._data[f'res{string_added}']= self._data[magid].astype('float32')
        # will be used to make sure we remove the indices corresponding to the reference of a given offset, Zp etc.
        # indeed solved array doesn't have any of the reference, so we will need to shift when reading it
        total_ref_removed_offset=0
        for key in list(self._init_index_dict.keys()):
            n_param = self._end_index_dict[key]-self._init_index_dict[key]+1
            
            print(key)
            print(f'n_param={n_param}')
            
            ref_list=np.atleast_1d(self._ref_dict[f'ref_{key}'])
            
            # create an empty array with the total size of params, including reference
            param_array=np.zeros(n_param)

            # fill only the ones that are fitted
            if ref_list[0]!=None:
                current_ref_removed_offset=len(ref_list)

                param_array[~np.isin(np.arange(n_param),ref_list)] = solved[self._init_index_dict[key]-total_ref_removed_offset:self._init_index_dict[key]-total_ref_removed_offset+n_param-current_ref_removed_offset]
                total_ref_removed_offset=total_ref_removed_offset+current_ref_removed_offset
            else:
                param_array[np.arange(n_param)] = solved[self._init_index_dict[key]-total_ref_removed_offset:self._init_index_dict[key]-total_ref_removed_offset+n_param]

            self._data[f"{key}{string_added}"]=param_array[self._data[self._unique_id_dict[key]]].astype('float32')
            
            # for the residuals, the extinction has non uniform weights, so we need to multiply the fitted parameters 
            # for all others we don't need these weights, but use ones to be general
            if self._weight_dict[key]==None:
                weights=np.ones(len(self.data))
            else: 
                weights=self.data[self._weight_dict[key]]
            self._data[f'res{string_added}'] = self._data[f'res{string_added}'] - self._data[f"{key}{string_added}"] * weights
                

    
    def get_solution_old(self, ref_expid, fit_rcid=True, ref_rcid = 0, magid=None, emagid=None, rebuild=False, method="cholmod", use_long=None):
        """
        deprecated 
        """
        solved = self.solve(ref_expid, fit_rcid, ref_rcid, magid, emagid, rebuild, method,use_long)
        fitted_mag = solved[self._data[self.STARID]]
        #fitted_zp = solved[self.nstars+self._data[self.EXPID]-1]
        zp_array=np.zeros(self.nexposures)
        zp_array[np.arange(self.nexposures)!=ref_expid] = solved[self.nstars:self.nstars+self.nexposures-1]
        fitted_zp = zp_array[self._data[self.EXPID]]

        if fit_rcid:
            rcidoffset_array=np.zeros(self.nrcid)
            rcidoffset_array[np.arange(self.nrcid)!=ref_rcid] = solved[self.nstars+self.nexposures-1:self.nstars+self.nexposures-1+self.nrcid-1]
            fitted_rcidoff = rcidoffset_array[self._data[self.RCID]]
            return fitted_mag,fitted_zp,fitted_rcidoff
        else: 
            return fitted_mag,fitted_zp
    
    def set_solution_old(self, ref_expid, fit_rcid=True, ref_rcid = 0, magid=None, emagid=None, string_added='', rebuild=False, method="cholmod",use_long=None):
        """
        deprecated 
        """
        if fit_rcid:
            fitted_mag,fitted_zp,fitted_rcidoff = self.get_solution(ref_expid, fit_rcid, ref_rcid, magid, emagid, rebuild, method,use_long)
        else: 
            fitted_mag,fitted_zp = self.get_solution(ref_expid, fit_rcid, ref_rcid, magid, emagid, rebuild, method,use_long)

        self._data[f"Fitted_mag{string_added}"] = fitted_mag
        self._data[f"Zp{string_added}"] = fitted_zp
        #self._data.loc[self._data.u_expid == ref_expid,f'Zp{string_added}']=0
        if fit_rcid:
            self._data[f"offset_rcid{string_added}"] = fitted_rcidoff
            #self._data.loc[self._data.rcid == ref_rcid,f'offset_rcid{string_added}']=0
            self._data[f'res{string_added}']= self._data[magid] - (self._data[f"Fitted_mag{string_added}"] + self._data[f"Zp{string_added}"] + self._data[f"offset_rcid{string_added}"])
        else: 
            self._data[f'res{string_added}']= self._data[magid] - (self._data[f"Fitted_mag{string_added}"] + self._data[f"Zp{string_added}"])

    
    def set_solution_from_solved(self,solved, fit_rcid=True, ref_rcid = 0, magid=None, ref_expid=0, string_added=''):
        """
        deprecated 
        """
        fitted_mag = solved[self._data["u_starid"]]
        fitted_zp = solved[self.nstars+self._data["u_expid"]-1]
        
        self._data[f"Fitted_mag{string_added}"] = fitted_mag
        self._data[f"Zp{string_added}"] = fitted_zp
        self._data.loc[self._data.u_expid == ref_expid,f'Zp{string_added}']=0
        self._data[f'res{string_added}']= self._data[magid] - (self._data[f"Fitted_mag{string_added}"] + self._data[f"Zp{string_added}"])
        if fit_rcid:
            fitted_off = solved[self.nstars+len(self._data["u_expid"].unique())-1+self._data["rcid"]-1]
            self._data[f'res{string_added}']= self._data[magid] - (self._data[f"Fitted_mag{string_added}"] + self._data[f"Zp{string_added}"] + self._data[f"offset_rcid{string_added}"])
        else: 
            self._data[f'res{string_added}']= self._data[magid] - (self._data[f"Fitted_mag{string_added}"] + self._data[f"Zp{string_added}"])

        
    def set_chi2(self,string_added='_AP_corr_1',string_err='_AP_1',group='expid'):
        self._data['pull_squared']=(self._data[f'res{string_added}']/self._data[f'e_mag{string_err}'])**2
        self._data[f"chi2_{group}{string_added}"]=self._data[['pull_squared',group]].groupby(group).transform("mean")
        

    # =============== #
    #   Properties    #
    # =============== #
    @property
    def data(self):
        """ ubercal data """
        #print('warning, data is a dataframe which is not protected by @property')
        if not hasattr(self,"_data"):
            return None
        return self._data
    
    def has_data(self):
        """ test if data has been set. """
        return self.data is not None
    
    @property
    def nstars(self):
        """ number of stars in the dataset """
        if not self.has_data():
            return None
        return len(self.data[self.STARID].unique())
    
    @property
    def nexposures(self):
        """ number of exposures in the dataset """
        if not self.has_data():
            return None
        return len(self.data[self.EXPID].unique())
    
    @property
    def nrcid(self):
        """ number of exposures in the dataset """
        if not self.has_data():
            return None
        return len(self.data[self.RCID].unique())
    
    @property
    def nobservations(self):
        """ data size in the dataset """
        if not self.has_data():
            return None
        return len(self.data)

    # --------- #
    #  Matrices #
    # --------- #
    @property
    def acoo(self):
        """ sparse model matrice """
        if not hasattr(self,"_acoo") or self._acoo is None:
            if not self.has_data():
                return None
            #self._acoo = self.get_acoo(rebuild=True)
            print("call build_acoo before")
        return self._acoo
    
    @property
    def ref_expid(self):
        """ This is set when solve() is called. """
        if not hasattr(self, "_ref_expid"):
            return None
        return self._ref_expid
    
    @property
    def wmatrix(self):
        """ weight sparse matrix """
        if not hasattr(self,"_wmatrix") or self._wmatrix is None:
            if not self.has_data():
                return None
            self._wmatrix = self.get_wmatrix(rebuild=True)
            
        return self._wmatrix


# =================== #
#                     #
#   SIMULATOR         #
#                     #
# =================== #

class UbercalSimulator( object ):
    """ 
    Old simulation from Mickael. 
    """
    def __init__(self, dataframe):
        """ """
        self.set_data(dataframe)
        
    @classmethod
    def from_simsample(cls, size, maglim=22, calib_percent=1):
        """ """
        mags = maglim - np.random.exponential(3, size)
        e_mag = np.random.normal(0.05,0.05/10,size=size)
        
        data = pd.DataFrame({"true_mag":mags, "true_e_mag":e_mag})
        data["mag"] = np.random.normal(mags, e_mag)
        data["e_mag"] = np.random.normal(e_mag, e_mag/10)
        return cls(data)
        
    # =============== #
    #  Methods        #
    # =============== #    
    def set_data(self, data):
        """ input a dataframe col [mag, e_mag]. """
        self._data = data

    def draw_ubercal(self, nobs, nstar_range=[40,500], offset_range=[-0.1,0.1]):
        """ """
        ntargets = np.random.randint(*nstar_range, size=nobs)
        offsets = np.random.uniform(*offset_range, size=nobs)
        datas = {}
        for i, (ntarget_, offset_) in enumerate(zip(ntargets,offsets)):
            data_obs = self.data.sample(ntarget_, replace=False) 
            data_obs["delta_mag"] = offset_
            data_obs["mag"] += offset_
            datas[i] = data_obs
            
        return pd.concat(datas).reset_index().rename({"level_0":"expid","level_1":"starid"}, axis=1)
    # =============== #
    #   Properties    #
    # =============== #    
    @property
    def data(self):
        """ """
        return self._data


    
class Plotter( Ubercal ):
    """
    Could all be in one class, but this can be useful when we want to plot only a subset of the data etc. 
    """
    def __init__(self, data=None):
        """ This should not be called directly for the data format is tricky.
        See from_exposuredict() or from_dataframe()
        """
        if data is not None:
            self.set_data(data)
            
            
    # =============== #
    # plots, analysis #
    # =============== #    
    
    def plot_ra_dec(self,nside=128,sampling=1,statistic="count",col_name='ra',err_name='e_mag_psf',mycm=mycm,min_=None,max_=None,unit=None,norm=None,title=None, return_stat=False):
        if title==None:
            title=statistic
        bins=np.arange(12*nside**2+1)
        ipix = hp.ang2pix(nside, 0.5 * np.pi - np.deg2rad(self._data.dec[::sampling].values), np.deg2rad(self._data.ra[::sampling].values))
        if statistic == "weighted_mean":
            ret=stats.binned_statistic(ipix,values=self._data[col_name][::sampling]/self._data[err_name][::sampling]**2, statistic='sum', bins=bins)
            ret_n=stats.binned_statistic(ipix,values=1/self._data[err_name][::sampling]**2, statistic='sum', bins=bins)

            ret.statistic[ret.statistic==0]=np.nan
            unit='mag'

            hp.mollview(ret.statistic/ret_n.statistic,rot=[-180,0],cmap=mycm,title=title,unit=unit,min=min_,max=max_,norm=norm)
            if return_stat:
                return ret.statistic/ret_n.statistic
        else: 

            ret=stats.binned_statistic(ipix,values=self._data[col_name][::sampling], statistic=statistic, bins=bins)
            ret.statistic[ret.statistic==0]=np.nan
            if unit == None:
                if statistic=="count":
                    unit='count'
                else:
                    unit=col_name
            
            hp.mollview(ret.statistic,rot=[-180,0],cmap=mycm,title=title,unit=unit,min=min_,max=max_,norm=norm)
            if return_stat:
                return ret

    def plot_pull_hist2D(self,sampling=1,ax=None,mag_name='mag_psf',col_name='res_psf',err_name='e_mag_psf',**kwargs):
        if ax==None:
            fig=plt.figure()
            ax = fig.add_axes([0.1,0.1,0.8,0.8])
        ax.hist2d(self._data[mag_name].values,(self._data[col_name].values/self._data[err_name]),**kwargs);
        plt.axhline(0,color="w")
        ax.set_ylabel("pull")
        ax.set_xlabel("mag")
        return ax

    def plot_hist1D_lin_log(self,col_name='res_psf',err_name='e_mag_psf',column='pull',x_name=None,bins=100,range=None,x_hor=None,y_hor=None,histtype='step',plot_gaussian=False,unique=False):
        from mpl_toolkits.axes_grid1 import make_axes_locatable
        fig=plt.figure(dpi=150,figsize=(12,4))
        gs = fig.add_gridspec(1, 2, hspace=0, wspace=0.3)
        (ax1, ax2) = gs.subplots(sharex='col')
        if column=="pull":
            if "pull" in self._data.columns:
                arr=self._data['pull']
            else:
                arr=self._data[col_name]/self._data[err_name]
        else:
             arr=self._data[column]
        
        if unique:
            arr=np.unique(arr)
            
        plot_hist(arr,x_name=x_name,y_name='',bins=bins,range=range,x_hor=x_hor,y_hor=y_hor,        yscale='log',histtype=histtype,plot_gaussian=plot_gaussian,ax=ax1)
        
        plot_hist(arr,x_name=x_name,y_name='',bins=bins,range=range,x_hor=x_hor,y_hor=y_hor,        yscale='linear',histtype=histtype,plot_gaussian=plot_gaussian,ax=ax2)
        
        
    def plot_hist2D_lin_log(self,col_name='res_psf',err_name='e_mag_psf',y_axis='pull',x_axis='index',x_name=None,y_name=None,bins=100,range=None,x_hor=None,y_hor=None,cmap=mycm):
        
        from mpl_toolkits.axes_grid1 import make_axes_locatable
        fig=plt.figure(dpi=150,figsize=(12,4))
        gs = fig.add_gridspec(1, 2, hspace=0, wspace=0.1)
        (ax1, ax2) = gs.subplots(sharex='col', sharey='row')

        if x_axis=='index':
            x=self._data.index
        elif x_axis=='obsdate':
            x=pd.to_datetime(self._data.obsdate)
        else:
            x=self._data[x_axis]

        if x_name==None: 
            x_name=x_axis
        
        if y_axis=="pull":
            if "pull" in self._data.columns:
                y=self._data['pull']
            else:
                y=self._data[col_name]/self._data[err_name]
        else:
             y=self._data[y_axis]
        plot_hist2D(x,y,x_name=x_name,y_name=y_name,bins=bins,range=range,x_hor=x_hor,y_hor=y_hor,ax=ax1,norm=mpl.colors.LogNorm(),cmap=cmap)
        plot_hist2D(x,y,x_name=x_name,y_name=y_name,bins=bins,range=range,x_hor=x_hor,y_hor=y_hor,ax=ax2,cmap=cmap)


    def plot_res_vs_mag(self,mag_name='mag_psf',col_name='res_psf',ax=None,violon=False,density=False,ZP_off=0,**kwargs):
        if ax==None:
            fig=plt.figure()
            ax = fig.add_axes([0.1,0.1,0.8,0.8])

        if density==True:
            ax.hist2d(self._data[mag_name]+ZP_off,self._data[col_name],**kwargs)
        else:
            ax.scatter(self._data[mag_name]+ZP_off,self._data[col_name],**kwargs)
        ax.set_ylim(-0.5,0.5)
        if violon==True:
            ax=violon_box(self._data[mag_name]+ZP_off,self._data[col_name],ax,alpha_v=0.5,alpha_b=0.5,xlabel=None,ylabel=None)
        ax.set_xlabel('observed magnitude')
        ax.set_ylabel('residuals')
        return ax


    def plot_pull_hist(self,col_name='res_psf',err_name='e_mag_psf',fit_err=False,k_fit_err=0,ax=None,bins=1000,range=[-10,10],histtype='step',yscale='log',**kwargs):
        if ax==None:
            fig=plt.figure()
            ax = fig.add_axes([0.1,0.1,0.8,0.8])
        sig_obs=self._data[err_name]
        if fit_err==True:
            print('not implemented yet, only in older version of the code')
            n, bins, patches=ax.hist(self._data.res.values/np.sqrt(-(self._data.Fitted_mag_err.values**2+self._data.Zp_err.values**2+k_fit_err**2*self._data.airmass_calc.values**2)+sig_obs**2),bins=bins,range=range,histtype=histtype);
        else:
            n, bins, patches=ax.hist(self._data[col_name].values/sig_obs,bins=bins,range=range,histtype=histtype);

        ax.plot(np.arange(-5,5,0.01),np.exp(-np.arange(-5,5,0.01)**2/2)*max(n))
        plt.axhline(0,color="w")
        ax.set_xlabel("pull")
        plt.yscale(yscale)
        return ax
    
    
    
    def weighted_average_std(self,string_='_AP_corr_1',estring_='_AP_1',additional_str='',keep_wmean=True,do_uncal=True):
        """
        Based on http://stackoverflow.com/a/2415343/190597 (EOL)
        """
        self._data['Weight']=1/self._data[f'e_mag{estring_}']**2
        self._data['weighted_res'] = self._data[f'res{string_}']*self._data['Weight']


        sum_wres=self._data[['weighted_res','Source']].groupby("Source").transform("sum")
        sum_w=self._data[['Weight','Source']].groupby("Source").transform("sum")
        self._data[f'res_weighted_mean{string_}{additional_str}']=(sum_wres.values/sum_w.values)
        self._data['res_weighted_std_part'] = ((self._data[f'res{string_}']-self._data[f'res_weighted_mean{string_}{additional_str}'])**2*self._data['Weight'])/sum_w.Weight
        self._data[f'res_weighted_std{string_}{additional_str}']=np.sqrt(self._data[['res_weighted_std_part','Source']].groupby("Source").transform("sum"))

        if do_uncal:
            self._data['weighted_mag'] = self._data[f'mag{string_}']*self._data['Weight']
            sum_wmag=self._data[['weighted_mag','Source']].groupby("Source").transform("sum")
            self._data[f'mag_weighted_mean{string_}{additional_str}']=(sum_wmag.values/sum_w.values)
            self._data['mag_weighted_std_part'] = ((self._data[f'mag{string_}']-self._data[f'mag_weighted_mean{string_}{additional_str}'])**2*self._data['Weight'])/sum_w.Weight
            self._data[f'mag_weighted_std{string_}{additional_str}']=np.sqrt(self._data[['mag_weighted_std_part','Source']].groupby("Source").transform("sum"))
            values = self._data.drop(["res_weighted_std_part",'mag_weighted_std_part','weighted_res','weighted_mag','Weight'],axis=1,inplace=True)
            if keep_wmean==False:
                values = self._data.drop([f'res_weighted_mean{string_}{additional_str}',f'mag_weighted_mean{string_}{additional_str}'],axis=1,inplace=True)
        else: 
            values = self._data.drop(["res_weighted_std_part",'weighted_res','Weight'],axis=1,inplace=True)
            if keep_wmean==False:
                values = self._data.drop([f'res_weighted_mean{string_}{additional_str}'],axis=1,inplace=True)


            
        #return df

    
    def plot_source_res(self,sourceid,root_name='res',string_='_AP_corr_1',estring_='_AP_1',time_index='time',offset=None,print_where=False,**kwarg):
        df_temp=self.data[self.data.Source==sourceid]
        if time_index=="time" or time_index=="obsjd":
            x=df_temp.obsjd.values
        elif time_index=="indexed":
            x=np.arange(len(df_temp))
        else:
            x=df_temp[time_index].values


        if offset==None:
            offset=0
        elif offset in self.data.columns:
            offset=df_temp[offset].values
        else:
            print(f'{offset} not a columns')

        # color based on rcid
        rcids=(df_temp.pid-df_temp.pid//100*100)
        #plt.errorbar(x=x,y=df_temp[f'{root_name}{string_}'].values+offset,yerr=df_temp[f'e_mag{estring_}'].values,fmt='.',color=plt.cm.viridis(rcids/64))

        #loop over each data point to plot
        for x_, y_, e_, color_ in zip(x, df_temp[f'{root_name}{string_}'].values+offset, df_temp[f'e_mag{estring_}'].values, plt.cm.viridis(rcids/64)):
            plt.plot(x_, y_, 'o', color=color_)
            plt.errorbar(x_, y_, e_, lw=1, capsize=3, color=color_)
        if print_where:
            print(f'rcids : {np.unique(rcids)}')
            print(f'ra = {np.unique(df_temp.ra)}, dec = {np.unique(df_temp.dec)}')
            if 'field' in df_temp.columns:
                print(f'fields : {np.unique(df_temp.field)}')
        
        if f'{root_name}_weighted_mean{string_}' in df_temp.columns:
            wmean=np.array(np.unique(df_temp[f'{root_name}_weighted_mean{string_}'].values+offset),dtype=object)
        else:
            wmean=np.unique(offset)

        wstd=np.unique(df_temp[f'{root_name}_weighted_std{string_}'].values)
        if len(wmean)==1:
            wmean=wmean[0]
        else:
            print('weird: multiple means')
        if len(wstd)==1:
            wstd=wstd[0]
        else:
            print('weird: multiple means')
        plt.axhspan(wmean-wstd,wmean+wstd,alpha=0.2,color='C2' ,zorder=2)
        plt.axhline(wmean,color='C3',ls='dashed',lw=4 ,zorder=3)

        plt.xlabel(time_index)
        plt.ylabel('residuals')
        plt.title(f'weighted mean is {wmean:02f}, and weighted std is {wstd:02f}')
        #plt.plot(df[df.Source==sourceid].obsjd,df[df.Source==sourceid][col],'.',**kwarg)


    
    def plot_dispersion_vs_mag(self,mag_name='mag_psf',col_name='res_psf',x_name='mag',ax=None,star_stat_std=None,star_stat_mean=None,starid_name='Source',threshold_count=10,range=[[-15,-4],[1e-3,1]],yscale='log',bins=[50,50],norm=mpl.colors.LogNorm(),x_hor=None,y_hor=None):

        from mpl_toolkits.axes_grid1 import make_axes_locatable
        fig=plt.figure(dpi=150,figsize=(12,4))
        gs = fig.add_gridspec(1, 2, hspace=0, wspace=0.1)
        (ax1, ax2) = gs.subplots(sharex='col', sharey='row')
        
        if type(star_stat_std)!=pd.core.frame.DataFrame:
            star_stat_std = self._data.groupby(starid_name, group_keys=False).std()
        if type(star_stat_mean)!=pd.core.frame.DataFrame:
            star_stat_mean = self._data.groupby(starid_name, group_keys=False).mean()

        if yscale=='log':
            y_space = np.logspace(np.log10(range[1][0]), np.log10(range[1][1]), bins[1])
        else:
            y_space = np.linspace(range[1][0],range[1][1], bins[1])

        x_space = np.linspace(range[0][0],range[0][1], bins[0])
        bins=(x_space, y_space)

        count_star_obs=self._data.groupby(starid_name, group_keys=False)[col_name].count().sort_values(ascending=False)
        above_=count_star_obs[count_star_obs>threshold_count].index

        plot_hist2D(star_stat_mean[star_stat_mean.index.isin(above_)][mag_name].values,star_stat_std[star_stat_std.index.isin(above_)][col_name].values,x_name=x_name,y_name='LC std',bins=bins,range=range,x_hor=x_hor,y_hor=y_hor,ax=ax1,norm=mpl.colors.LogNorm())
        ax1.set_yscale('log')

        plot_hist2D(star_stat_mean[star_stat_mean.index.isin(above_)][mag_name].values,star_stat_std[star_stat_std.index.isin(above_)][col_name].values,x_name=x_name,y_name='LC std',bins=bins,range=range,x_hor=x_hor,y_hor=y_hor,ax=ax2)
        ax2.set_yscale('log')

        fig.suptitle(f'LC dispersion for objects with more than {threshold_count} sources',y=1)
        return fig

    
    def plot_uv(self,bin_width=30,list_cols=['res_psf'],stat_list=['mean', 'std','median', 'count'],min_list = [-0.01,0,-0.01,0],max_list = [0.01,0.1,0.01,None],title=None,unit_=None,return_stat=False,incl_gap=False,mycm=mycm):
        # prepare binned data for uv plot

        self._data['x_bin']=np.round(self._data.x/bin_width).astype('int')*bin_width
        self._data['y_bin']=np.round(self._data.y/bin_width).astype('int')*bin_width
        
        if 'ccdid' not in self._data.columns:
            self.rcid_to_ccdid_qid()
        list_for_group=['x_bin', 'y_bin','ccdid','qid']
        res_stat=self._data[list_for_group+list_cols].groupby(list_for_group).agg(['mean', 'std','median', 'count'])
        
        if return_stat:
            full_stat=[]
            print(f"will return an array of {len(list_cols)} number of columns by {len(stat_list)} statistics")
        import resid_module as r_m
        FP=r_m.RES_FocalPlane()
        for i,col in enumerate(list_cols):           
            for j,stat in enumerate(stat_list):
                FP_med=FP.from_res_stat(res_stat,stat=stat,supercol=col,bin_width=bin_width)
                f, ax = plt.subplots(dpi=150)
                #cc=aa.get_data(incl_gap=True,bins=30)#,colorbar=True,cmap="viridis",vmin=-0.008,vmax=0.008,extent=[-14000,14000,-14000,14000])
                if unit_==None:
                    if stat=='count':
                        unit='count'
                    else:
                        unit='mag'
                else:
                    unit=unit_
                FP_med.show(ax=ax,incl_gap=incl_gap,bins=bin_width,colorbar=True,cmap=mycm,vmin=min_list[j],vmax=max_list[j],extent=[-14000,14000,-14000,14000],unit=unit)
                #cbar.set_label(unit)
                ax.set_xlabel('u')
                ax.set_ylabel('v')
                if title==None:
                    title=f'residuals {stat}'
                ax.set_title(title)
                if return_stat:
                    full_stat.append(FP_med.get_data(incl_gap=incl_gap,bins=bin_width))
        if return_stat:
            # so ugly, but quick and dirty, must be another clean option 
            return np.array(full_stat).reshape(len(list_cols),len(stat_list),np.shape(full_stat)[1],np.shape(full_stat)[2])
                
                
    def plot_fields(self):
        from ztfquery import fields
        all_fields=np.unique(self._data.field)
        a=fields.show_fields(all_fields,alpha=0.5,colorbar=False,inclhist=False,figsize=(15,15))
        return a
    #mag_string=['_AP_1','_AP_corr_1','_psf','_psf_corr']
    #err_string=['_AP_1','_AP_1','_psf','_psf']

    def plot_all(self,list_strings=['_AP_1'],err_strings=['_AP_1'],min_=-0.02,max_=0.02,plot_dir=None,display=False,dpi=200):#['_AP_1','_AP_corr_1','_psf','_psf_corr']
        print('many more plots to add, some existing, some to do')
        print('pull vs uv? pull vs radec? chi2 plots?')

        self.plot_fields()
        
        if plot_dir!=None:plt.savefig(f'plots/{plot_dir}/fields.png',dpi=dpi)
        if display==False:plt.close()
        
        if len(list_strings)!=len(err_strings):
            print('should have as many errors as magnitudes')
        print("plot_ra_dec")
        for i in range(len(list_strings)):
            for stat in ['mean','weighted_mean','median','count','std']:
                if stat=='count':
                    # max is 3 times the mean number of obs per pix
                    self.plot_ra_dec(col_name=f'res{list_strings[i]}',statistic=stat,mycm=mycm,min_=0,max_=3*len(self._data)/(12*64**2*3/4))
                    if plot_dir!=None:plt.savefig(f'plots/{plot_dir}/map_{stat}_res{list_strings[i]}.png',dpi=dpi)
                    if display==False:plt.close()
                elif stat=='std':
                    self.plot_ra_dec(col_name=f'res{list_strings[i]}',statistic=stat,mycm=mycm,min_=0,max_=5*max_)
                    if plot_dir!=None:plt.savefig(f'plots/{plot_dir}/map_{stat}_res{list_strings[i]}.png',dpi=dpi)
                    if display==False:plt.close()
                else:
                    self.plot_ra_dec(col_name=f'res{list_strings[i]}',statistic=stat,mycm=mycm,min_=min_,max_=max_)
                    if plot_dir!=None:plt.savefig(f'plots/{plot_dir}/map_{stat}_res{list_strings[i]}.png',dpi=dpi)
                    if display==False:plt.close()



            self.plot_hist2D_lin_log(col_name=f'res{list_strings[i]}',err_name=f'e_mag{err_strings[i]}',y_axis='pull',x_axis=f'mag{list_strings[i]}',x_name="mag",y_name='pull',bins=[300,300],range=[[-13, -5], [-5, 5]],x_hor=0,y_hor=None)
            if plot_dir!=None:plt.savefig(f'plots/{plot_dir}/pull_vs_mag{list_strings[i]}.png',dpi=dpi)
            if display==False:plt.close()
            self.plot_hist2D_lin_log(col_name=f'res{list_strings[i]}',err_name=f'e_mag{err_strings[i]}',y_axis='pull',x_axis='index',x_name="index",y_name='pull',bins=[300,300],range=[[0, self.data.index.max()], [-5, 5]],x_hor=0,y_hor=None)
            if plot_dir!=None:plt.savefig(f'plots/{plot_dir}/pull_vs_index{list_strings[i]}.png',dpi=dpi)
            if display==False:plt.close()
            self.plot_hist2D_lin_log(col_name=f'res{list_strings[i]}',err_name=f'e_mag{err_strings[i]}',y_axis='pull',x_axis='obsjd',x_name="obsjd",y_name='pull',bins=[300,300],range=[[self.data.obsjd.min(), self.data.obsjd.max()], [-5, 5]],x_hor=0,y_hor=None)
            if plot_dir!=None:plt.savefig(f'plots/{plot_dir}/pull_vs_obsjd{list_strings[i]}.png',dpi=dpi)
            if display==False:plt.close()
            self.plot_hist1D_lin_log(col_name=f'res{list_strings[i]}',err_name=f'e_mag{err_strings[i]}',column='pull',bins=1000,range=[-5,5],plot_gaussian=True)
            if plot_dir!=None:plt.savefig(f'plots/{plot_dir}/pull_hist1D{list_strings[i]}.png',dpi=dpi)
            if display==False:plt.close()
            self.plot_hist2D_lin_log(col_name=f'res{list_strings[i]}',err_name=f'e_mag{err_strings[i]}',y_axis=f'res{list_strings[i]}',x_axis=f'mag{list_strings[i]}',x_name="mag",y_name='residuals',bins=[300,300],range=None,x_hor=0,y_hor=None)
            if plot_dir!=None:plt.savefig(f'plots/{plot_dir}/res_vs_mag{list_strings[i]}.png',dpi=dpi)
            if display==False:plt.close()
            self.plot_hist2D_lin_log(col_name=f'res{list_strings[i]}',err_name=f'e_mag{err_strings[i]}',y_axis=f'res{list_strings[i]}',x_axis=f'mag{list_strings[i]}',x_name="mag",y_name='residuals',bins=[300,300],range=[[-13, -5], [-0.1, 0.1]],x_hor=0,y_hor=None)
            if plot_dir!=None:plt.savefig(f'plots/{plot_dir}/res_vs_mag_zoom{list_strings[i]}.png',dpi=dpi)
            if display==False:plt.close()
            print("plot_dispersion_vs_mag")

            ax=self.plot_dispersion_vs_mag(mag_name=f'mag{list_strings[i]}',col_name=f'res{list_strings[i]}',ax=None,star_stat_std=None,star_stat_mean=None,starid_name='Source',x_name='mag',threshold_count=10,range=[[-13,-5],[1e-3,1]],yscale='log',bins=[300,300],norm=mpl.colors.LogNorm())

            if plot_dir!=None:plt.savefig(f'plots/{plot_dir}/plot_dispersion_vs_mag{list_strings[i]}.png',dpi=dpi)
            if display==False:plt.close()
                
            self.plot_hist1D_lin_log(column=f'Zp{list_strings[i]}',plot_gaussian=False,unique=True)
            if plot_dir!=None:plt.savefig(f'plots/{plot_dir}/hist_ZP{list_strings[i]}.png',dpi=dpi)
            if display==False:plt.close()
            self.plot_hist2D_lin_log(x_axis='obsjd',y_axis=f'Zp{list_strings[i]}',range=None,bins=[500,500])
            if plot_dir!=None:plt.savefig(f'plots/{plot_dir}/ZP_vs_obsjd{list_strings[i]}.png',dpi=dpi)
            if display==False:plt.close()    
            s_i=0
            print("plot_uv")

            for stat in ['mean','median','std','count']:
                min_uv=[min_,min_,0,0][s_i]
                max_uv=[max_,max_,5*max_,None][s_i]
                s_i=s_i+1
                self.plot_uv(list_cols=[f'res{list_strings[i]}'],stat_list=[stat],min_list = [min_uv],max_list = [max_uv])
                if plot_dir!=None:plt.savefig(f'plots/{plot_dir}/plot_uv_{stat}{list_strings[i]}.png',dpi=dpi)
                if display==False:plt.close()

    # ------- #
    # SETTER  #
    # ------- #
    def set_data(self, data):
        """ Sets the ubercal dataframe. 
        
        Most lilely you should not use this method directly, 
        the input data must have a very particular structure.
            
        In case of doubt see the class method from_dataframe().
        """
        self._data = data

        

        
        


def violon_box(x,y,ax,bins='intmag',alpha_v=0.5,alpha_b=0.5,title="",xlabel='X',ylabel='Y'):
    """
    see https://matplotlib.org/stable/gallery/statistics/customized_violin.html
    Will plot a violon plot and a boxplot for a given array of x and y. 
    bins can be a number, or if let empty, will default to a bin per integer, as is used for magnitudes. 
    """

    #df = pd.DataFrame({'x': x,
    #               'y': y})
    x=np.array(x,dtype='float64')
    y=np.array(y,dtype='float64')
    x_nonan=x[(~np.isnan(x)) & (~np.isnan(y))]
    y_nonan=y[(~np.isnan(x)) & (~np.isnan(y))]
    if type(bins)==int:

        yhist, binedges=np.histogram(x_nonan,bins=bins)
        bincenters = np.mean(np.vstack([binedges[0:-1],binedges[1:]]), axis=0)
        #x=np.array(Test.metatable.Median_skynoise.values[:N_max],dtype='float64')
        bincenters_a=np.array(bincenters,dtype='float64')
        bin_in_mat = np.digitize(x_nonan, bincenters_a, right=False)
        bin_contents = [list(y_nonan[np.where(bin_in_mat == i)[0]]) for i in range(len(bincenters_a))]
        inds=np.unique(bincenters_a)
        data=bin_contents
    elif bins=='intmag':
        x_pos=np.floor(x_nonan)+0.5
        # make list of list per unit mag bin
        data = [y_nonan[x_pos==SS].tolist() for SS in np.unique(x_pos)]
        inds = np.unique(x_pos)


    ax.set_title(title)
    ax.set_xlabel(xlabel)
    ax.set_ylabel(ylabel)
    ax.grid()

    parts = ax.violinplot(
            data, showmeans=False, showmedians=False,
            showextrema=False,positions=inds,widths=5./6*(inds[1]-inds[0]))

    for pc in parts['bodies']:
        pc.set_facecolor('#D43F3A')
        pc.set_edgecolor('black')
        pc.set_alpha(alpha_v)


    quartiles1=[]
    medians=[]
    quartiles3=[]
    for i in range(len(data)):
        quartile1, median, quartile3 = np.percentile(data[i], [25, 50, 75])
        quartiles1.append(quartile1)
        medians.append(median)
        quartiles3.append(quartile3)

    whiskers = np.array([
        adjacent_values(sorted_array, q1, q3)
        for sorted_array, q1, q3 in zip(data, quartiles1, quartiles3)])
    whiskers_min, whiskers_max = whiskers[:, 0], whiskers[:, 1]

    ax.scatter(inds, medians, marker='o', color='white', s=4, zorder=3)
    ax.vlines(inds, quartiles1, quartiles3, color='k', linestyle='-', lw=5,alpha=alpha_b)
    ax.vlines(inds, whiskers_min, whiskers_max, color='k', linestyle='-', lw=1,alpha=alpha_b)

    plt.subplots_adjust(bottom=0.15, wspace=0.05)
    return ax

def adjacent_values(vals, q1, q3):
    upper_adjacent_value = q3 + (q3 - q1) * 1.5
    upper_adjacent_value = np.clip(upper_adjacent_value, q3, vals[-1])

    lower_adjacent_value = q1 - (q3 - q1) * 1.5
    lower_adjacent_value = np.clip(lower_adjacent_value, vals[0], q1)
    return lower_adjacent_value, upper_adjacent_value

def plot_hist2D(x,y,x_name='',y_name='',bins=100,range=None,x_hor=None,y_hor=None,ax=None,norm=None,cmap=mycm):
        from mpl_toolkits.axes_grid1 import make_axes_locatable
        #fig=plt.figure(dpi=150,figsize=(15,5))
        #gs = fig.add_gridspec(1, 2, hspace=0, wspace=0.1)
        #(ax1, ax2) = gs.subplots(sharex='col', sharey='row')
        h1=ax.hist2d(x,y,bins=bins,norm=norm,range=range,cmap=cmap)
        ax.grid()
        ax.set_xlabel(x_name)
        divider = make_axes_locatable(ax)
        cax = divider.append_axes("top", size="5%", pad=0.05)
        plt.colorbar(h1[3],ax=ax,cax=cax,orientation="horizontal")
        cax.xaxis.set_ticks_position("top")
        ax.set_ylabel(y_name)
        if x_hor!=None:ax.axhline(x_hor,color="r",ls='dashed')
        if y_hor!=None:ax.axhline(y_hor,color="r",ls='dashed')
        
        
def plot_hist(arr,x_name='',y_name='',bins=100,range=None,x_hor=None,y_hor=None,yscale='log',histtype='step',plot_gaussian=False,ax=None):
        from mpl_toolkits.axes_grid1 import make_axes_locatable
        #fig=plt.figure(dpi=150,figsize=(15,5))
        #gs = fig.add_gridspec(1, 2, hspace=0, wspace=0.1)
        #(ax1, ax2) = gs.subplots(sharex='col', sharey='row')
        if ax==None:
            fig=plt.figure()
            ax = fig.add_axes([0.1,0.1,0.8,0.8])
        n, bins, patches=ax.hist(arr,bins=bins,range=range,histtype=histtype);
        ax.grid()
        ax.set_xlabel(x_name)
        divider = make_axes_locatable(ax)
        ax.set_ylabel(y_name)
        if plot_gaussian:
            ax.plot(np.arange(-5,5,0.01),np.exp(-np.arange(-5,5,0.01)**2/2)*max(n),'--')
        ax.set_yscale(yscale)

        if x_hor!=None:ax.axhline(x_hor,color="r",ls='dashed')
        if y_hor!=None:ax.axhline(y_hor,color="r",ls='dashed')  
        
        
        
import astropy.units as u
from astropy.coordinates import AltAz,SkyCoord,EarthLocation
from astropy.time import Time
def Get_airmass(ra,dec,obs_date,site='Palomar'):
    ra, dec = ra*u.deg, dec*u.deg
    aa = AltAz(location=EarthLocation.of_site(site), obstime=Time(obs_date,format='jd'))
    rcid_ra_dec = SkyCoord(ra,dec,frame='icrs')
    airmass_calc = rcid_ra_dec.transform_to(aa).secz.value
    return airmass_calc
