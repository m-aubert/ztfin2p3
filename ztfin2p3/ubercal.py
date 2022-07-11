""" Tools for Ubercal analyses. 
test
Example:
--------
usim = UbercalSimulator.from_simsample( int(1e5) )
ucal = usim.draw_ubercal(1000)

ubercal = Ubercal.from_dataframe(ucal, min_exp=3)
x_solution = ubercal.solve(ref_expid=0, method="cholmod")

"""


import numpy as np
import pandas

from scipy import sparse
from scipy.sparse import linalg as splinalg



def _build_index_df_(dataframe, inid, outid, minsize=None):
    """ """
    tmp_df = dataframe.groupby(inid, as_index=False).size()
    if minsize:
        tmp_df = tmp_df[tmp_df["size"]>=minsize].reset_index(drop=True)
        
    return dataframe.merge(tmp_df[inid].reset_index().rename({"index":outid}, axis=1), on=inid)


# =================== #
#                     #
#   UBERCAL           #
#                     #
# =================== #

class Ubercal( object ):
    STARID = "u_starid"
    EXPID = "u_expid"
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
        data = cls.shape_dataframe(data, starid=starid, expid=expid, min_exp=min_exp)
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
        dataframe = _build_index_df_(dataframe, inid=starid, outid=cls.STARID, minsize=min_exp)
        dataframe = _build_index_df_(dataframe, inid=expid, outid=cls.EXPID)
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
                                  self.data[self.EXPID]+self.nstars])
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
        return len(self.data[self.EXPID].unique())
    
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


# =================== #
#                     #
#   SIMULATOR         #
#                     #
# =================== #

class UbercalSimulator( object ):
    """ """
    def __init__(self, dataframe):
        """ """
        self.set_data(dataframe)
        
    @classmethod
    def from_simsample(cls, size, maglim=22, calib_percent=1):
        """ """
        mags = maglim - np.random.exponential(3, size)
        e_mag = np.random.normal(0.05,0.05/10,size=size)
        
        data = pandas.DataFrame({"true_mag":mags, "true_e_mag":e_mag})
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
            
        return pandas.concat(datas).reset_index().rename({"level_0":"expid","level_1":"starid"}, axis=1)
    # =============== #
    #   Properties    #
    # =============== #    
    @property
    def data(self):
        """ """
        return self._data
