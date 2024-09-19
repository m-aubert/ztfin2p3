import datetime
import itertools
import logging
import os

import numpy as np
from astropy.io import fits
from ztfimg import CCD, __version__ as ztfimg_version

from .. import io, metadata, __version__
from ..builder import calib_from_filenames

FILTER2LED = {"zg": [2, 3, 4, 5], "zr": [7, 8, 9, 10], "zi": [11, 12, 13]}
LED2FILTER = {led: filt for filt, leds in FILTER2LED.items() for led in leds}


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
        keep_rawmeta: bool = False,
        nskip: int | None = None,
        use_dask: bool = False,
        **kwargs,
    ):
        assert self.kind in ("bias", "flat")

        self.logger = logging.getLogger(__name__)
        self.period = period
        self.use_dask = use_dask

        rawmeta = metadata.get_rawmeta(
            self.kind, self.period, add_filepath=True, use_dask=use_dask, **kwargs
        )
        if keep_rawmeta:
            self.rawmeta = rawmeta

        self.df = rawmeta.groupby(self.group_keys)["filepath"].apply(list).reset_index()

        if len(self.df) == 0:
            self.logger.warning("no metadata for %s", period)
            self.df["fileout"] = None
        else:
            self.df["fileout"] = self.df.apply(
                lambda row: io.get_daily_biasfile(row.day, row.ccdid), axis=1
            )
        self.df["ccd"] = None

        if nskip is not None:
            self.df["filepath"] = self.df["filepath"].map(lambda x: x[nskip:])
            # Fix patch in case whacky data acquisition (e.g only 1 bias file)
            # requires at least two bias data for the pipeline to run "smoothly"
            n_files = self.df.filepath.map(len)
            good = n_files > 2
            if not good.all():
                self.logger.warning("Days with less than two raw images were removed.")
                self.df = self.df[good].reset_index(drop=True)

    def build_ccds(
        self,
        corr_overscan: bool = True,
        corr_nl: bool = True,
        load_if_exists: bool = False,
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
        load_if_exists : bool
            Load existing files in memory?
        reprocess : bool
            Reprocess existing files?
        save : bool
            Save the processed files?
        **kwargs
            Instruction to average the data, passed to
            ztfimg.collection.ImageCollection.get_meandata()

        """
        for i, row in self.df.iterrows():
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
            elif load_if_exists:
                self.logger.info("loading file %s", filename)
                data = CCD.from_filename(filename)
            else:
                data = None

            if data is not None:
                self.df.at[i, "ccd"] = data

    def store_ccds(self, overwrite: bool = True, **kwargs):
        """Store created ccds.
        Extra arguments are passed to `fits.writeto`.
        """
        for i, row in self.df.iterrows():
            hdr = self.build_header(row)
            ensure_path_exists(row.fileout)
            fits.writeto(
                row.fileout, row.ccd, header=hdr, overwrite=overwrite, **kwargs
            )

    def get_ccd(self, day: str, ccdid: int = None, **kwargs):
        sel = self.df.day == day
        if ccdid is not None:
            sel &= self.df.ccdid == ccdid
        for key, val in kwargs.items():
            if key in self.df.columns:
                sel &= self.df[key] == val

        idx = self.df.index[sel]
        if len(idx) == 0:
            raise ValueError("not found")
        elif len(idx) > 1:
            raise ValueError("selection is not unique")

        row = self.df.loc[idx[0]]
        if row.ccd is None or (
            isinstance(row.ccd, list) and all(x is None for x in row.ccd)
        ):
            self.logger.info("loading file %s", row.fileout)
            self.df.at[idx[0], "ccd"] = CCD.from_filename(row.fileout)
        return self.df.loc[idx[0]].ccd

    def build_header(self, row, **kwargs):
        now = datetime.datetime.now().isoformat()
        # flatten file list if needed
        flist = row.filepath
        nframes = len(
            list(itertools.chain(*flist)) if isinstance(flist[0], list) else flist
        )
        meta = {
            "IMGTYPE": self.kind,
            "NFRAMES": nframes,
            "NDAYS": 1,
            "PTYPE": "daily",
            "PERIOD": row.day,
            "CCDID": row.ccdid,
            "PIPELINE": ("ZTFIN2P3", "image processing pipeline"),
            "PIPEV": (__version__, "ztfin2p3 pipeline version"),
            "ZTFIMGV": (ztfimg_version, "ztfimg pipeline version"),
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
        keep_rawmeta: bool = False,
        nskip: int | None = None,
        suffix: str | None = None,
        use_dask: bool = False,
        **kwargs,
    ):
        super().__init__(
            period, keep_rawmeta=keep_rawmeta, nskip=nskip, use_dask=use_dask, **kwargs
        )
        # Add filterid (grouping by LED)
        self.df["filterid"] = self.df.ledid.map(lambda x: LED2FILTER.get(x, ""))

        if self.df.filterid.eq("").any():
            self.logger.warning("removing some flats without ledid")
            self.df = self.df[self.df.filterid.ne("")]

        if len(self.df) == 0:
            return

        _groupbyk = ["day", "ccdid", "filterid"]
        self.df = self.df.groupby(_groupbyk).aggregate(list).reset_index()
        self.df["nled"] = self.df.ledid.map(len)
        self.df.fileout = self.df.apply(
            lambda row: io.get_daily_flatfile(
                row.day, row.ccdid, filtername=row.filterid
            ),
            axis=1,
        )
        if suffix is not None:
            self.df.fileout = self.df.fileout.str.replace(".fits", f"_{suffix}.fits")

        if {tuple(df.nled) for _, df in self.df.groupby("ccdid")} != {(4, 3, 4)}:
            self.logger.warning("non-standard number of flats per led:")
            self.logger.warning(str(dict(zip(self.df.filterid, self.df.nled))))

    def build_ccds(
        self,
        bias: BiasPipe | None = None,
        corr_nl: bool = True,
        corr_overscan: bool = True,
        corr_pocket: bool = True,
        load_if_exists: bool = False,
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
        load_if_exists : bool
            Load existing files in memory?
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
        if weights is None:
            weights = dict(zg=None, zr=None, zi=None)

        for i, row in self.df.iterrows():
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
                    data = calib_from_filenames(
                        led_filelist,
                        corr=bias_data,
                        corr_nl=corr_nl,
                        corr_overscan=corr_overscan,
                        corr_pocket=corr_pocket,
                        **kwargs,
                    )
                    if normalize:
                        norms.append(norm := np.nanmedian(data))
                        data /= norm
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
            elif load_if_exists:
                self.logger.info("loading file %s", filename)
                data = CCD.from_filename(filename)
            else:
                data = None

            if data is not None:
                self.df.at[i, "ccd"] = data

    def build_header(self, row, **kwargs):
        ledid = row.ledid if isinstance(row.ledid, int) else None
        return super().build_header(row, FILTRKEY=row.filterid, LEDID=ledid, **kwargs)
