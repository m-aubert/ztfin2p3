import pandas
import numpy as np



def _draw_pointings(nobs, fov=0.2, pointing_scatter=[0.3,0.7]):
    """ """
    base = np.asarray([[-fov/2, fov/2],[-fov/2, fov/2]])
    pointings = np.random.uniform(*pointing_scatter, size=(nobs,2,1))
    return base+pointings


class StarTableSimulator( object ):
    """ """
    def __init__(self, dataframe):
        """ """
        self.set_truedata(dataframe)
        
    @classmethod
    def from_simsample(cls, size, maglim=22, calib_percent=1):
        """ """
        mags = maglim - np.random.exponential(3, size)
        e_mag = np.random.normal(0.05,0.05/10,size=size)
        ra, dec = np.random.rand(2, size)

        data = pandas.DataFrame({"true_mag":mags, "true_e_mag":e_mag})
        data["mag"] = np.random.normal(mags, e_mag)
        data["e_mag"] = np.random.normal(e_mag, e_mag/10)
        data["ra"] = ra
        data["dec"] = dec
        
        return cls(data)
        
    # =============== #
    #  Methods        #
    # =============== #    
    def set_truedata(self, data):
        """ input a dataframe col [mag, e_mag]. """
        self._truedata = data
        
    def set_simdata(self, data):
        """ """
        self._simdata = data
        
    def draw_pointings(self, nobs, 
                               fov=0.2,
                               pointing_scatter=[0.3,0.7], 
                               offset_range=[-0.1,0.1],
                               in_place=False):
        """ """
        from ..utils.tools import get_in_squares
        
        pointings = _draw_pointings(nobs, fov=fov, 
                                  pointing_scatter=pointing_scatter).reshape(nobs ,4)
        # Must be in
        self._pointings = pointings
        self._nobs = nobs
        
        # Generate the simulation
        datas = []
        for i,[xmin,xmax,ymin, ymax] in enumerate(pointings):
            index_in = get_in_squares(np.asarray([xmin,xmax,ymin, ymax]),
                                      self.truedata[["ra","dec"]].values)
            data_obs = self.truedata[index_in][["ra","dec","mag","e_mag"]].copy()
            data_obs[["x","y"]] = data_obs[["ra","dec"]]-[xmin, ymin]
            data_obs["expid"] = i
            datas.append(data_obs)
            
        data = pandas.concat(datas).reset_index().rename({"index":"starid"}, axis=1)
        
        if in_place:
            return self.set_simdata(data) 

        return data
    
    def apply_effects(self, on="mag", data=None, **kwargs):
        """ """
        if data is None:
            data = self.simdata
            
        effects = {}
        for name, effect in kwargs.items():
            effects[name] = effect( data )
            
        df_effects = pandas.DataFrame(effects, index=data.index)
        return data[on]+df_effects.sum(axis=1), df_effects
    
    # =============== #
    #   Properties    #
    # =============== #    
    @property
    def truedata(self):
        """ """
        return self._truedata

    @property
    def simdata(self):
        """ """
        return self._simdata
    
    @property
    def pointings(self):
        """ """
        if not hasattr(self, "_pointings"):
            raise AttributeError("you did not call for draw_pointings.")
        return self._pointings
    
    @property
    def nobs(self):
        """ """
        if not hasattr(self, "_nobs"):
            raise AttributeError("you did not draw a simulation.")
        return self._nobs
    
