
import numpy as np


class _Effect_( object ):
    """ """
    def __init__(self, **kwargs):
        """ """
        self._prop = kwargs

    # This is mandatory
    def __call__(self, dataframe, **kwargs):
        """ """
        raise NotImplementedError("You must define the __call__ function for your effect.")

    # This is optional    
    def get(self, *args, **kwargs):
        """ """
        raise NotImplementedError("get() method not implemented")

    # ============== #
    #   Property     #
    # ============== #    
    @property
    def prop(self):
        """ """
        return self._prop

class _KeyEffect_( _Effect_ ):
    
    def __init__(self, key, **kwargs):
        """ """
        self._key = key
        self._prop = kwargs

    # ============== #
    #   Property     #
    # ============== #    
    @property
    def key(self):
        """ """
        return self._key

        

class UniqueKeyNormalScatter( _KeyEffect_ ):
    
    def __init__(self, key, loc=0, scale=1):
        """ """
        super().__init__(key, loc=0, scale=1)
        
    def __call__(self, dataframe,  **kwargs):
        """ """
        keys = np.unique(dataframe[self.key].values)
        effect = self.get( len(keys) )
        return dataframe[self.key].replace(keys, effect).values
    
    def get(self, size, **kwargs):
        """ """
        return np.random.normal(size=size, **{**self.prop, **kwargs})
        

    
class PositionCurve( _KeyEffect_ ):
    
    def __init__(self, coef, key=["x","y"], **kwargs):
        """ """
        self._coef = coef
        super().__init__(key, **kwargs)
        
    def __call__(self, dataframe, coef=None):
        """ """
        if coef is None:
            coef = self.coef
        x,y = dataframe[self.key].values.T
        return self.get(*dataframe[self.key].values.T)
    
    def get(self, x, y):
        """ """
        return np.dot( np.asarray([np.ones(x.shape[0]), x, y, x*y, x*x, y*y]).T, 
                        self.coef)
        
    # ============== #
    #   Property     #
    # ============== #
    
    @property
    def coef(self):
        """ """
        return self._coef
        
class NormalScatter( _Effect_ ):
    
    def __init__(self, loc=0, scale=1):
        """ """
        super().__init__(loc=loc, scale=scale)
        
    def __call__(self, dataframe,  **kwargs):
        """ """
        return self.get(len(dataframe), **kwargs)
    
    def get(self, size, **kwargs):
        """ """
        return np.random.normal(size=size, **{**self.prop, **kwargs})
            
        
class KeyNormalScatter( NormalScatter ):
    
    def __init__(self, key, loc=0, scale=1):
        """ """
        super().__init__(loc=loc, scale=scale)
        self._key = key
        
    def __call__(self, dataframe,  **kwargs):
        """ """
        noise = self.get(len(dataframe), **kwargs)
        return dataframe[self.key]+noise
        
    # ============== #
    #   Property     #
    # ============== #
    @property
    def key(self):
        """ """
        return self._key
