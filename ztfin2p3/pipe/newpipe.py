import datetime
import logging
import os
import warnings

from astropy.io import fits
import numpy as np
import ztfimg

from .. import io, metadata, __version__
from ..builder import calib_from_filenames
from ..builder import calib_from_filenames_withcorr

LED2FILTER = {"zg": [2, 3, 4, 5], "zr": [7, 8, 9, 10], "zi": [11, 12, 13]}
FILTER2LED = {led: filt for filt, leds in LED2FILTER.items() for led in leds}


def ensure_path_exists(filename):
    path = os.path.dirname(filename)
    if not os.path.isdir(path):
        os.makedirs(path, exist_ok=True)


class CalibPipe:

    kind: str = ""
    group_keys: list[str, ...] = ["day", "ccdid"]

    def __init__(
        self,
        period: str | tuple[str, str],
        nskip: int | None = None,
        use_dask: bool = False,
        **kwargs,
    ):
        assert self.kind in ("bias", "flat")

        self.logger = logging.getLogger(__name__)
        self.period = period
        self.use_dask = use_dask
        self.ccds: list[ztfimg.CCD] = []

        self.df = metadata.get_rawmeta(
            self.kind, self.period, add_filepath=True, use_dask=use_dask, **kwargs
        )
        self.init_df = (
            self.df.groupby(self.group_keys)["filepath"].apply(list).reset_index()
        )
        self.init_df["fileout"] = self.init_df.apply(
            lambda row: io.get_daily_biasfile(row.day, row.ccdid), axis=1
        )

        if nskip is not None:
            self.init_df["filepath"] = self.init_df["filepath"].map(lambda x: x[nskip:])
            # Fix patch in case whacky data acquisition (e.g only 1 bias file)
            # requires at least two bias data for the pipeline to run "smoothly"
            n_files = self.init_df.filepath.map(len)
            good = n_files > 2
            if not good.all():
                warnings.warn("Days with less than two raw images were removed.")
                self.init_df = self.init_df[good].reset_index(drop=True)

    def build_ccds(
        self,
        corr_overscan: bool = True,
        corr_nl: bool = True,
        reprocess: bool = False,
        save: bool = True,
        **kwargs,
    ):
        """Compute/save/load the daily calibration file.

        Parameters
        ----------
        corr_overscan : bool
            Correct for overscan?  (if both corr_overscan and corr_nl are
            true, nl is applied first)
        corr_nl : bool
            Correct for non-linearity?
        reprocess : bool
            Reprocess existing files?
        save : bool
            Save the processed files?
        **kwargs
            Instruction to average the data, passed to
            ztfimg.collection.ImageCollection.get_meandata()

        """
        self.ccds = []
        for _, row in self.init_df.iterrows():
            filename = row.fileout
            if reprocess or not os.path.exists(filename):
                self.logger.info("processing %s %s", self.kind, row.day)
                data = calib_from_filenames(
                    row["filepath"],
                    corr_overscan=corr_overscan,
                    corr_nl=corr_nl,
                    **kwargs,
                )
                if save:
                    self.logger.info("writing file %s", filename)
                    hdr = self.build_header(row)
                    ensure_path_exists(filename)
                    fits.writeto(filename, data, header=hdr, overwrite=True)
            else:
                self.logger.info("loading file %s", filename)
                data = ztfimg.CCD.from_filename(filename).get_data()
            self.ccds.append(data)

    def store_ccds(self, overwrite: bool = True, **kwargs) -> list[str]:
        """Store created ccds.
        Extra arguments are passed to `fits.writeto`.

        """
        outs = []
        for i, row in self.init_df.iterrows():
            data = self.ccds[i]
            hdr = self.build_header(row)
            ensure_path_exists(row.fileout)
            fits.writeto(row.fileout, data, header=hdr, overwrite=overwrite, **kwargs)
            outs.append(row.fileout)

        return outs

    def get_ccd(self, day: str, ccdid: int = None, **kwargs):
        sel = self.init_df.day == day
        if ccdid is not None:
            sel &= self.init_df.ccdid == ccdid

        index = self.init_df[sel].index
        if index.size > 1:
            raise ValueError("selection is not unique")

        if self.ccds:
            return self.ccds[index[0]]
        else:
            row = self.init_df.loc[index[0]]
            self.logger.info("loading file %s", row.fileout)
            return ztfimg.CCD.from_filename(row.fileout).get_data(**kwargs)

    def build_header(self, row, **kwargs):
        now = datetime.datetime.now().isoformat()
        meta = {
            "IMGTYPE": self.kind,
            "NFRAMES": len(row.filepath),
            "NDAYS": 1,
            "PTYPE": "daily",
            "PERIOD": row.day,
            "CCDID": row.ccdid,
            "PIPELINE": ("ZTFIN2P3", "image processing pipeline"),
            "PIPEV": (__version__, "ztfin2p3 pipeline version"),
            "ZTFIMGV": (ztfimg.__version__, "ztfimg pipeline version"),
            "PIPETIME": (now, "ztfin2p3 file creation"),
            **kwargs,
        }
        hdr = fits.Header()
        hdr.update(meta)
        return hdr


class BiasPipe(CalibPipe):
    kind = "bias"


class FlatPipe(CalibPipe):
    kind = "flat"
    group_keys = ["day", "ccdid", "ledid"]

    def __init__(
        self,
        period: str | tuple[str, str],
        nskip: int | None = None,
        use_dask: bool = False,
        **kwargs,
    ):
        super().__init__(period=period, nskip=nskip, use_dask=use_dask, **kwargs)

        # Add filterid (grouping by LED)
        self.init_df["filterid"] = self.init_df.ledid.map(lambda x: FILTER2LED[x])
        _groupbyk = ["day", "ccdid", "filterid"]
        self.init_df = self.init_df.groupby(_groupbyk).aggregate(list).reset_index()
        # self.init_df.filepath = self.init_df.filepath.map(
        #     lambda x: list(itertools.chain(*x))
        # )

        self.init_df.fileout = self.init_df.apply(
            lambda row: io.get_daily_flatfile(
                row.day, row.ccdid, filtername=row.filterid
            ),
            axis=1,
        )

    def build_ccds(
        self,
        bias: BiasPipe | None = None,
        corr_overscan: bool = True,
        corr_nl: bool = True,
        normalize: bool = True,
        reprocess: bool = False,
        save: bool = True,
        weights: dict[str, list[float]] | None = None,
        **kwargs,
    ):
        """Compute/save/load the daily calibration file.

        Parameters
        ----------
        bias : BiasPipe
            If given, remove bias on raw flats
        corr_overscan : bool
            Correct for overscan?  (if both corr_overscan and corr_nl are
            true, nl is applied first)
        corr_nl : bool
            Correct for non-linearity?
        normalize: bool
            Normalize each flat by the nanmedian level?
        reprocess : bool
            Reprocess existing files?
        save : bool
            Save the processed files?
        weights : dict
            Dictionnary storing for each filter the weights to apply to each led.
            default ``dict(zg=None, zr=None, zi=None)``.
        **kwargs
            Instruction to average the data, passed to
            ztfimg.collection.ImageCollection.get_meandata()

        """
        self.ccds = []
        if weights is None:
            weights = dict(zg=None, zr=None, zi=None)

        for _, row in self.init_df.iterrows():
            filename = row.fileout
            if reprocess or not os.path.exists(filename):
                self.logger.info(
                    "processing %s %s filter=%s", self.kind, row.day, row.filterid
                )
                bias_data = (
                    bias.get_ccd(row.day, ccdid=row.ccdid) if bias is not None else None
                )
                arrays, norms = [], []
                for led_filelist in row["filepath"]:
                    data = calib_from_filenames_withcorr(
                        led_filelist,
                        corr=bias_data,
                        corr_overscan=corr_overscan,
                        corr_nl=corr_nl,
                        **kwargs,
                    )
                    if normalize:
                        norm = np.nanmedian(data)
                        data /= norm
                        norms.append(norm)
                    arrays.append(data)

                data = np.average(arrays, weights=weights[row.filterid], axis=0)
                if normalize:
                    norm = np.average(norms, weights=weights[row.filterid])
                else:
                    norm = None

                if save:
                    self.logger.info("writing file %s", filename)
                    hdr = self.build_header(row, FLTNORM=norm)
                    ensure_path_exists(filename)
                    fits.writeto(filename, data, header=hdr, overwrite=True)
            else:
                self.logger.info("loading file %s", filename)
                data = ztfimg.CCD.from_filename(filename).get_data()
            self.ccds.append(data)

    def build_header(self, row, **kwargs):
        ledid = row.ledid if isinstance(row.ledid, int) else None
        return super().build_header(row, FILTRKEY=row.filterid, LEDID=ledid, **kwargs)
