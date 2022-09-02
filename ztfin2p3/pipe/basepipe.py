


class BasePipe( object ):

    def __init__(self, use_dask=True):
        """ """
        self._use_dask = use_dask

    # ============== #
    #   Methods      #
    # ============== #
    def set_config(self, config):
        """ """
        return self._config

    def set_datafile(self, datafile):
        """ """
        self._datafile = datafile
    
    # ============== #
    #   Parameters   #
    # ============== #
    @property
    def config(self):
        """ configuration to use for the pipeline """
        return self._config

    @property
    def datafile(self):
        """ dataframe of files use to build the flats """
        return self._datafile
    
    @property
    def use_dask(self):
        """ shall this pipeline use dask (you have delayed object) """
        return self._use_dask
    
