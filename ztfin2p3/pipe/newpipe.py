import warnings

from .. import metadata


class CalibPipe:

    kind: str = ""

    def __init__(
        self,
        period: str | tuple[str, str],
        nskip: int | None = None,
        use_dask: bool = False,
        **kwargs,
    ):

        assert self.kind in ("bias", "flat")

        self.period = period
        self.use_dask = use_dask
        self.datafile = metadata.get_rawmeta(
            self.kind, self.period, add_filepath=True, use_dask=use_dask, **kwargs
        )

        groupby_ = ["day", "ccdid"]
        if self.kind == "flat":
            groupby_.append("ledid")
        self.init_datafile = (
            self.datafile.groupby(groupby_)["filepath"].apply(list).reset_index()
        )

        if nskip is not None:
            self.init_datafile["filepath"] = self.init_datafile["filepath"].map(
                lambda x: x[nskip:]
            )
            # Fix patch in case whacky data acquisition (e.g only 1 bias file)
            # requires at least two bias data for the pipeline to run "smoothly"
            n_files = self.init_datafile.filepath.map(len)
            good = n_files > 2
            if not good.all():
                warnings.warn("Days with less than two raw images were removed.")
                self.init_datafile = self.init_datafile[good].reset_index(drop=True)


class BiasPipe(CalibPipe):
    kind = "bias"


class FlatPipe(CalibPipe):
    kind = "flat"
