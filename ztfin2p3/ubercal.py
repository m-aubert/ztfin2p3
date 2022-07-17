""" Tools for Ubercal analyses. 

This is the old version of the ubercal. 
startable.py shall replace it.

Example:
--------
# Generate a simulation

## Generate 3e5 stars and 400 Exposures
from ztfin2p3.simulations import linearmodel
usim = linearmodel.StarTableSimulator.from_simsample(1e5)
usim.draw_pointings(100, in_place=True)

## Add normal scatter to each expid and normalscatter for noise
from ztfin2p3.simulations import effects as ef

new_mag, effects = usim.apply_effects(on="mag", 
                       **{"expzp": ef.UniqueKeyNormalScatter("expid"),
                          "noise": ef.NormalScatter(scale=0.1),
                          }
                      )
## and update mag accordingly
usim.simdata["mag"] = new_mag

# build ubercal with the simulated data
from ztfin2p3 import ubercal
uber =ubercal.Ubercal.from_dataframe(usim.simdata)
## and solve it.
data = uber.solve_and_format(0)
"""


import numpy as np
import pandas

from scipy import sparse
from scipy.sparse import linalg as splinalg



def map_id(dataframe, in_id, out_id, inplace=False):
    """ add a new column"""
    if not inplace:
        dataframe = dataframe.copy()
        
    unique_ = np.sort(np.unique( dataframe[in_id].values ))
    new = np.arange( len(unique_) )
    dict_mapping = dict( zip(unique_, new) )
    
    dataframe[out_id] = dataframe[in_id].map( dict_mapping )
    return dataframe


# =================== #
#                     #
#   UBERCAL           #
#                     #
# =================== #

class Ubercal( object ):
    STARID = "u_starid"
    ZPID = "u_zpid"
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
        data = pandas.DataFrame.from_dict(exposure_dict, orient="index").apply(pandas.Series.explode)
        return cls.from_dataframe( data.reset_index().rename({"index":"expid"}, axis=1),
                                   starid=starid, min_exp=min_exp )
    
    @classmethod
    def from_dataframe(cls, data, starid="starid", expid="expid", min_exp=3):
        """ load the object given a dataframe of observation.
        
        The dataframe must be single index and must contain the column 
           - mag # observed magnitude 
           - e_mag # error on observed magnitude
           - and the starid and expid columns (see option). 
             These represents the individual star and exposure id.
             
        Parameters
        ----------
        dataframe: [pandas.DataFrame]
            dataframe containing the observations. 
            must contain mag and e_mag columns
            
        starid, expid: [string] -optional-
            name of the star and exposure id in the input dataframe.
            The internal index (0->nstar and 0->nexposures) set internally independently.
        
        min_exp: [int or None] -optional-
            minimum number of observations for a star to be considered.
            If None, no cut is made.
            
        Returns
        -------
        instance of Object
        """
        data = cls.shape_dataframe(data, starid=starid, expid=expid, min_exp=min_exp
                                  ).reset_index()
        return cls(data)
    
    @classmethod
    def shape_dataframe(cls, dataframe, min_exp=3, starid="starid", expid="expid"):
        """ reshape the input dataframe to have the internal star and expid index set. 
        It also selects only the stars that have at least min_exp different exposure observations.
        
        Parameters
        ----------
        dataframe: [pandas.DataFrame]
            dataframe containing, at least, the observations (mag, e_mag) and 
            the corresponding star and exposure ids. 
            These can be any format, they will be converted into 0->nstar and 0->nexposures
            index internally by this method.
            
        min_exp: [int or None] -optional-
            minimum number of observations for a star to be considered.
            If None, no cut is made.
            
        starid, expid: [string] -optional-
            name of the star and exposure id in the input dataframe.
            The internal index (0->nstar and 0->nexposures) set internally independently.
            
        Returns
        -------
        DataFrame
        """
        if min_exp is not None:
            tmp = dataframe.groupby(starid).size()
            kept = tmp[tmp>min_exp].index
            dataframe = dataframe[dataframe["starid"].isin(kept)]
        else:
            dataframe = dataframe.copy()
        
        dataframe = map_id(dataframe, in_id=starid, out_id=cls.STARID)
        dataframe = map_id(dataframe, in_id=expid, out_id=cls.ZPID)
        return dataframe

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
        self._ref_expid = None
        
    # ------- #
    # GETTER  #
    # ------- #
    def get_acoo(self, rebuild=False):
        """ get (or rebuild) the model sparse matrix (a in a•x=b)
        
        The sparse matrix is a M x N matrix with, 
            - M = number of observations
            - N = numer of stars + number of exposures
        and is sorted such that the stars are first and then the magnitude zp.
        
        Parameters
        ----------
        rebuild: [bool] -optional-
            if the matrix has already bee measured (see self.acoo), should this use 
            it or measure it ? (True means the matrix is re-measured).
            
        Returns
        -------
        scipy Sparse Matrix (coo)
        """
        if not rebuild:
            acoo = self.acoo
        else:
            coo  = pandas.concat([self.data[self.STARID],
                                  self.data[self.ZPID]+self.nstars])
            acoo = sparse.coo_matrix((np.asarray(np.ones( len(coo) ), dtype="int"), 
                                     (np.asarray(coo.index, dtype="int"), 
                                      np.asarray(coo.values, dtype="int")))
                                    )
        return acoo
    
    def get_wmatrix(self, rebuild=False):
        """ get (or build) the weight matrix. 
        
        The weight matrix is a sparse diagonal matrix. 
        The diagonal elements are 1/mag_err**2
        """
        if not rebuild:
            wmat = self.wmatrix
        else:
            wmat = sparse.diags(1/np.asarray(self.data[self.EMAGID], dtype="float")**2)
            
        return wmat

    # ------- #
    # SOLVER  #
    # ------- #
    def solve_and_format(self, ref_expid, **kwargs):
        """ calls solve and then format_solved. 

        = self.solve doc = 

        Solve for X in A•X = B.
        This method include variance, so it actually solves for
             A^t @ C @ A • X = A^T @ C • B
             
        
        Parameters
        ----------
        ref_expid: [int]
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
        x = self.solve(ref_expid, **kwargs)
        return self.format_solved(x)
        
    def solve(self, ref_expid, method="cholmod"):
        """ Solve for X in A•X = B.
        This method include variance, so it actually solves for
             A^t @ C @ A • X = A^T @ C • B
             
        
        Parameters
        ----------
        ref_expid: [int]
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
        acoo = self.get_acoo()
        b = np.asarray(self.data[self.MAGID].values, dtype="float")
        
        # set the reference exposure
        mask = np.ones(self.acoo.shape[1])
        mask[ref_expid + self.nstars] = 0
        acoo_ref = acoo.tocsr()[:,np.asarray(mask, dtype="bool")]
        self._ref_expid = ref_expid
        
        
        # include covariance 
        wmatrix = self.get_wmatrix()
        atw_ref = acoo_ref.T @ wmatrix
        
        if method == "lsqr":
            return splinalg.lsqr(atw_ref @ acoo_ref, atw_ref.dot(b) )    
        
        if method == "spsolve":
            return splinalg.spsolve(atw_ref @ acoo_ref, atw_ref.dot(b) )
            
        if method == "cholmod":
            from sksparse.cholmod import cholesky
            factor = cholesky(atw_ref @ acoo_ref)
            return factor( atw_ref.dot(b) )
            
        raise NotImplementedError(f"Only 'lsqr', 'spsolve' and 'cholmod' method implemented ; {method} given")

    def format_solved(self, solved_solution):
        """ adds the columns 'fitted_zp' and 'fitted_mag' to a copy of self.data 

        = See solve_and_format() = 
        
        Parameters
        ----------
        solved_solution: [array]
            1d array returned by self.solve()

        Returns
        -------
        DataFrame
        """
        fitted_mag = self.data[self.STARID
                              ].map( dict( zip( self.data[self.STARID].sort_values().unique(),
                                                solved_solution[:self.nstars]
                                              )))

        fitted_expid = self.data[self.ZPID
                              ].map( dict( zip( self.data[self.ZPID].sort_values().unique(),
                                                np.insert(solved_solution[self.nstars:],
                                                          self.ref_expid, 0)
                                              )))
        data = self.data.copy()
        data["fitted_zp"] = fitted_expid
        data["fitted_mag"] = fitted_mag
        return data
        
    # =============== #
    #   Properties    #
    # =============== #
    @property
    def data(self):
        """ ubercal data """
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
        return len(self.data[self.ZPID].unique())
    
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
            self._acoo = self.get_acoo(rebuild=True)
            
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


