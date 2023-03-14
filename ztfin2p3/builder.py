""" Top level calibration builder class """

import os        
import warnings
import numpy as np
from ztfimg import collection
from ztfimg import RawCCD
import dask.array as da

__all__ = ["CalibrationBuilder"]


class CalibrationBuilder( object ): # /day /week /month

    def __init__(self, imgcollection):
        """ CalibrationBuilder enables you to build 
        the mean image from list flats or biases.

        Method
        ------
        build: build the data from the image collection.
        to_fits: dump the data and header to a fits file.
        build_and_store: build the data and store them into a fits file.
        

        Parameters
        ----------
        imgcollection: `ztfimg.ImageCollection`
            ztfimg image collection object (or child of)
            up on which the whole class is built


        See also
        --------
        from_filenames: loads the instance given a list of files.
        """
        self.set_imgcollection( imgcollection )

    # ============== #
    #  I/O           # 
    # ============== #
    @classmethod
    def from_filenames(cls, filenames, as_path=False,
                           use_dask=True, persist=False, 
                           raw=None, correction=None, **kwargs):
        """ loads the instance from a list of filenames

        Parameters
        ----------
        filenames: list
            list of filenames. Could be pathes or ztf filenames 
            (see as_path)

        as_path: bool
            Set to True if the filename [filename_mask] are path and not just ztf filename, 
            hence bypassing ``files = ztfquery.io.get_file(files)``

        use_dask: bool
            Should dask be used ?
            The data will not be loaded but delayed  (dask.array).

        persist: bool
            = only applied if use_dask=True =
            should we use dask's persist() on data ?

        raw: bool
            are you inputting raw files ? 
            If None, this will guess it given the first filename.

        **kwargs goes to Collection.from_filenames()

        Returns
        -------
        instance 
            
        """
        from ztfimg import collection

        if raw is None:
            # Here this is deprecated -- need to change it.
            from ztfimg.buildurl import filename_to_kind
            raw = filename_to_kind(filenames[0]) == "raw"
        
        #if raw:
        CcdCollection = collection.ImageCollection
        #else:
        #    CcdCollection = collection.CCDCollection
            
        flatcollection = CcdCollection.from_filenames(filenames, use_dask=use_dask,
                                                      persist=persist, as_path=as_path, **kwargs)
        return cls(flatcollection)

    
    #@classmethod
    #def from_images(cls, images, raw=True, **kwargs):
    #    """
    #    """
    #    from ztfimg import collection
    #    if raw:
    #        CcdCollection = collection.RawCCDCollection
    #    else:
    #        CcdCollection = collection.CCDCollection
    #    
    #    flatcollection = CcdCollection.from_images(images)
    #    
    #    return cls(flatcollection)     
    
    def to_fits(self, fileout, header=None, overwrite=True, **kwargs):
        """ Store the data in fits format 

        Parameters
        ----------
        fileout: str
            filepath where the data should be stored

        header: `fits.Header`
            header. If None self.header will be used

        overwrite: bool
            if fileout already exist, should this overwrite it ?

        **kwargs goes to fits.writeto()
        

        Returns
        -------
        str
            The input fileout (to check all works fine)
        """
        if header is None:
            header = self.header

        if not os.path.isdir(dirout):
            os.makedirs(dirout, exist_ok=True)

        return self._to_fits(fileout, self.data, header=header,
                                 overwrite=overwrite,
                                 **kwargs)

    @staticmethod
    def _to_fits(fileout, data, header=None,  overwrite=True,
                     **kwargs):
        """ static method to dump the data using fits.writeto 

        Parameters
        ----------
        data: nd-array
            image to store.

        header: `fits.Header`
            header. If None an empty header will be used.

        overwrite: bool
            if fileout already exist, should this overwrite it ?

        **kwargs goes to fits.writeto()

        Returns
        -------
        str
            the input fileout
        """
        from astropy.io import fits
        fits.writeto(fileout, data, header=header,
                         overwrite=overwrite, **kwargs)
        return fileout
        
    # ============== #
    #  Methods       # 
    # ============== #
    # -------- # 
    #  SETTER  #
    # -------- #
    def set_imgcollection(self, imgcollection):
        """ set the image collection 

        = it is unlikely you want to used this method =

        Parameters
        ----------
        imgcollection: `ztfimg.ImageCollection`
            ztfimg image collection object (or child of)
            up on which the whole class is built

        Returns
        -------
        None
        
        See also
        --------
        from_filenames: load the instance from a list of filenames
        """
        self._imgcollection = imgcollection

    def set_data(self, data):
        """ Set the data 

        = it is unlikely you want to use this method =
        
        Parameters
        ----------
        data: 2d-array
            dask or numpy array corresponding to the built image

        Returns
        -------
        None

        See also
        --------
        build: build the data from the image collection.
        to_fits: dump the data and header to a fits file.
        """
        self._data = data

    def set_header(self, header):
        """ set the image data header
        
        = it is unlikely you want to use this method =

        Parameters
        ----------
        header: fits.Header
            header to eventually be stored.

        See also
        --------
        build: build the data from the image collection.
        to_fits: dump the data and header to a fits file.
        """
        self._header = header
        
    # -------- # 
    # BUILDER  #
    # -------- #
    def build_and_store(self, fileout, overwrite=True, 
                        corr_nl=True, corr_overscan=True,
                        chunkreduction=2,
                        set_it=False, incl_header=False,
                        header_keys=None, **kwargs):
        """ build the data and store them into a fits file.
        
        This is a high-level method applying build() and then to_fits().

        Parameters
        ----------
        fileout: str
            filepath where the data should be stored

        overwrite: bool
            if fileout already exist, should this overwrite it ?

        corr_nl: bool
            Should data be corrected for non-linearity

        corr_overscan: bool
            Should the data be corrected for overscan
            (if both corr_overscan and corr_nl are true,
            corr_nl is applied first)

        chunkreduction: int
            rechunk and split of the image.
            If None, no rechunk

        set_it: bool
            should data created by this method be set as self.data
            (using self.set_data())

        incl_header: bool
            should this also build the header. If False, header will 
            be set to None

        header_keys: list
            = ignored if incl_header=False =
            limit the keys to be kept in the header to these ones.
            None means all are kept.

        **kwargs goes to self.build() -> self.imgcollection.get_meandata()
            
        Returns
        -------
        str
            input fileout

        See also
        --------
        build: build the data from the image collection.
        to_fits: dump the data and header to a fits file.
        """
        # Build
        data, header = self.build(corr_nl=corr_nl, corr_overscan=corr_overscan,
                                  chunkreduction=chunkreduction,
                                  header_keys=header_keys,
                                  set_it=False, incl_header=incl_header,
                                  **kwargs)

        #data = data.persist() # needed to force the good graph
        
        if "dask" in str(type(data)): # is a dask object
            data = data.compute()
            to_fits = self.to_fits
        else:
            to_fits = self._to_fits
            
        # and store
        return to_fits(fileout, data, header=header, overwrite=overwrite)

    @staticmethod
    def build_from_data(datas, corr_nl=True, corr_overscan=True,
                  set_it=False, incl_header=False,
                  header_keys=None, chunkreduction=2,
                  dask_on_header=False, **kwargs):
        """ build the mean data.

        Parameters
        ----------
        corr_nl: bool
            Should data be corrected for non-linearity

        corr_overscan: bool
            Should the data be corrected for overscan
            (if both corr_overscan and corr_nl are true,
            corr_nl is applied first)

        chunkreduction: int
            rechunk and split of the image.
            If None, no rechunk

        set_it: bool
            should data created by this method be set as self.data
            (using self.set_data())

        incl_header: bool
            should this also build the header. If False, header will 
            be set to None

        header_keys: list
            = ignored if incl_header=False =
            limit the keys to be kept in the header to these ones.
            None means all are kept.

        dask_on_header: bool
            should dask be used on header merging ?

        **kwargs goes to self.imgcollection.get_meandata 

        Returns
        -------
        2d-array, fits.Header
            mean data and header

        See also
        --------
        build_and_store: build the data and store them into a fits file.
        """
        
        # This could be updated in the calibration function #
        
        prop = {**dict(corr_overscan=corr_overscan, corr_nl=corr_nl,
                       chunkreduction=chunkreduction),
                **kwargs}
                    
        return get_meandata(datas, **prop)
    

    def build(self, corr_nl=True, corr_overscan=True,
                  set_it=False, incl_header=False,
                  header_keys=None, chunkreduction=2,
                  dask_on_header=False, **kwargs):
        """ build the mean data.

        Parameters
        ----------
        corr_nl: bool
            Should data be corrected for non-linearity

        corr_overscan: bool
            Should the data be corrected for overscan
            (if both corr_overscan and corr_nl are true,
            corr_nl is applied first)

        chunkreduction: int
            rechunk and split of the image.
            If None, no rechunk

        set_it: bool
            should data created by this method be set as self.data
            (using self.set_data())

        incl_header: bool
            should this also build the header. If False, header will 
            be set to None

        header_keys: list
            = ignored if incl_header=False =
            limit the keys to be kept in the header to these ones.
            None means all are kept.

        dask_on_header: bool
            should dask be used on header merging ?

        **kwargs goes to self.imgcollection.get_meandata 

        Returns
        -------
        2d-array, fits.Header
            mean data and header

        See also
        --------
        build_and_store: build the data and store them into a fits file.
        """
        
        # This could be updated in the calibration function #
        
        #prop = {**dict(corr_overscan=corr_overscan, corr_nl=corr_nl,
        #               chunkreduction=chunkreduction),
        #        **kwargs}
            
        data =self.imgcollection.get_data(**dict(corr_overscan=corr_overscan, corr_nl=corr_nl))
        data = get_meandata(data,chunkreduction=chunkreduction, **kwargs) #self.imgcollection.get_meandata(**prop)
        if incl_header:
            header = self.build_header(keys=header_keys,
                                       use_dask=dask_on_header)
        else:
            header = None
            
        if set_it:
            self.set_data(data)
            self.set_header(header)
            
        return data, header
    
    
    def build_with_corr(self, corr_nl=True, corr_overscan=True,
                  corr = None, set_it=False, incl_header=False,
                  header_keys=None, chunkreduction=2,
                  dask_on_header=False, **kwargs):
        """ build the mean data.

        Parameters
        ----------
        corr_nl: bool
            Should data be corrected for non-linearity

        corr_overscan: bool
            Should the data be corrected for overscan
            (if both corr_overscan and corr_nl are true,
            corr_nl is applied first)

        chunkreduction: int
            rechunk and split of the image.
            If None, no rechunk

        set_it: bool
            should data created by this method be set as self.data
            (using self.set_data())

        incl_header: bool
            should this also build the header. If False, header will 
            be set to None

        header_keys: list
            = ignored if incl_header=False =
            limit the keys to be kept in the header to these ones.
            None means all are kept.

        dask_on_header: bool
            should dask be used on header merging ?

        **kwargs goes to self.imgcollection.get_meandata 

        Returns
        -------
        2d-array, fits.Header
            mean data and header

        See also
        --------
        build_and_store: build the data and store them into a fits file.
        """
        
        # This could be updated in the calibration function #
        
        if corr is None : 
            return self.build(corr_nl=corr_nl, corr_overscan=corr_overscan,
                  set_it=set_it, incl_header=incl_header,
                  header_keys=header_keys, chunkreduction=2,
                  dask_on_header=dask_on_header, **kwargs)
                    
        prop_data = dict(corr_overscan=corr_overscan, corr_nl=corr_nl)
        prop = {**dict(chunkreduction=chunkreduction),  **kwargs}
        
        if incl_header:
            header = self.build_header(keys=header_keys,
                                       use_dask=dask_on_header)
        else:
            header = None
            
        data = self.imgcollection.get_data(**prop_data) - corr        
        data = get_meandata(data,**prop)
                   
        if set_it:
            self.set_data(data)
            self.set_header(header)
            
        return data, header
    
    def build_header(self, keys=None, refid=0, incl_input=False,
                         use_dask=None):
        """ build the merged header 

        Parameters
        ----------
        keys: list
            list of keys to be kept.
            None means all.

        refid: int
            image to be used as reference for the header.

        incl_input: bool
            should a INPUT{i} be added to the header for each input file.
            (filenames will be added)

        use_dask: bool
            should dask be used to build the header.

        Returns
        -------
        `fits.Header`
            the merged header.
        """
        import copy
        from astropy.io import fits
        header = self.imgcollection.get_singleheader(refid, as_serie=False, 
                                                    use_dask=use_dask)
        if "dask" in str(type(header)):
            header = header.compute()
            
        if keys is not None:
            newheader = header.__class__([ copy.copy(header.cards[k]) for k in np.atleast_1d(keys)])
        else:
            newheader = copy.copy(header)
            
        newheader.set(f"NINPUTS",self.imgcollection.nimages, "num. input images")
        
        if incl_input:
            basenames = self.imgcollection.filenames
            for i, basename_ in enumerate(basenames):
                newheader.set(f"INPUT{i:02d}",basename_, "input image")
              
        return newheader
    
    
    # ============== #
    #  Properties    # 
    # ============== #
    @property
    def imgcollection(self):
        """  ztfimg.ImageCollection object up on which the class is built. """
        if not hasattr(self, "_imgcollection"):
            return None
        
        return self._imgcollection
    
    @property
    def data(self):
        """ merged data.
        
        See also
        --------
        build: build the data from the image collection.        
        """
        if not hasattr(self, "_data"):
            return None
        
        return self._data

    def has_data(self):
        """ test if data has been set. False means no """
        return self.data is not None
    
    @property
    def header(self):
        """ merged header

        See also
        --------
        build_header: build the header
        build: build the data from the image collection. (build_header called inside.)
        
        """
        if not hasattr(self, "_header"):
            return None
        
        return self._header
    
    def has_header(self):
        """ test if the header has been set. False means no """
        return self.header is not None


    
def get_meandata(datas, axis=0,
                         chunkreduction=2,
                         weights=1,  sigma_clip=None, mergedhow="mean", clipping_prop={},
                         **kwargs):
        """ get a the mean 2d-array of the images [nimages, N, M]->[N, M]

        Parameters
        ----------
        chunkreduction: int
            rechunk and split of the image.
            If None, no rechunk

        weights: str, float or array
            multiplicative weighting coef for individual images.
            If string, this will be understood as a functuon to apply on data.
            (ie. mean, median, std etc.) | any np.{weights}(data, axis=(1,2) will work.
            otherwise this happens:
            ```
            datas = self.get_data(**kwargs)
            weights = np.asarray(np.atleast_1d(weights))[:, None, None] # broadcast
            datas *=weights
            ```
        
        sigma_clip: float or None
            sigma clipping to be applied to the data (along the stacking axis by default)
            None means, no clipping.

        clipping_prop: dict
            kwargs entering scipy.stats.sigma_clip()

        **kwargs goes to self.get_data()

        Returns
        -------
        2d-array
            mean image (dask or numpy)

        See also
        --------
        get_data: get the stack images [nimages, N, M]
        """
        
        use_dask = "dask" in str(type(datas)) 
        
        npda = da if use_dask else np 
        
        # Should you apply weight on the data ?
        if weights is not None and weights != 1:
            if type(weights) == str:
                weights = getattr(npda,weights)(data, axis=(1,2))[:, None, None]
            else:
                weights = np.asarray(np.atleast_1d(weights))[:, None, None] # broadcast
                
            datas *=weights

        # If dasked, should you change the chunkredshuct n?
        if use_dask and chunkreduction is not None:
            if axis==0:
                chunk_merging_axis0 = np.asarray(np.asarray(datas.shape)/(1, 
                                                                  chunkreduction, 
                                                                  chunkreduction), dtype="int")
                datas = datas.rechunk( list(chunk_merging_axis0) )
            
            else:
                warnings.warn(f"chunkreduction only implemented for axis=0 (axis={axis} given)")

        # Is sigma clipping applied ?
        if sigma_clip is not None and sigma_clip>0:
            from astropy.stats import sigma_clip as scipy_clipping # sigma_clip is the sigma option
            clipping_prop = {**dict(sigma=sigma_clip, # how many sigma clipping
                                    axis=axis,
                                    sigma_lower=None, sigma_upper=None, maxiters=5,
                                    cenfunc='median', stdfunc='std', 
                                    masked=False),
                             **clipping_prop}
                
            if use_dask:
                datas = datas.map_blocks(scipy_clipping, **clipping_prop)
            else:
                datas = scipy_clipping(datas, **clipping_prop)

        # Let's go.
        return getattr(npda, mergedhow)(datas, axis=axis) #npda.mean(datas, axis=axis)
