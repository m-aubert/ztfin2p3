import datetime
import json
import logging
import os
import pathlib
from typing import Any

from rich.logging import RichHandler
from ztfimg import __version__ as ztfimg_version

from ztfin2p3 import __version__ as ztfin2p3_version


def _run_pdb(type, value, tb) -> None:  # pragma: no cover
    import pdb
    import traceback

    traceback.print_exception(type, value, tb)
    pdb.pm()


def setup_logger(debug=False):
    handler_opts: dict[str, Any] = dict(markup=False, rich_tracebacks=True)
    if debug:
        level = "DEBUG"
        handler_opts.update(dict(show_time=True, show_path=True))
    else:
        level = "INFO"
        handler_opts.update(dict(show_time=True, show_path=False))

    logging.basicConfig(
        level=level,
        format="%(message)s",
        datefmt="[%X]",
        handlers=[RichHandler(**handler_opts)],
    )


def init_stats(**kwargs):
    now = datetime.datetime.now(datetime.UTC)
    return {
        "date": now.isoformat(),
        "ztfimg_version": ztfimg_version,
        "ztfin2p3_version": ztfin2p3_version,
        "slurm_jobid": os.getenv("SLURM_JOB_ID"),
        **kwargs,
    }


def save_stats(stats, statsdir, day, ccdid=None):
    logger = logging.getLogger(__name__)
    statsdir = pathlib.Path(statsdir)
    now = datetime.datetime.now(datetime.UTC)

    if ccdid is not None:
        stats_file = statsdir / f"stats_{day}_{ccdid}_{now:%Y%M%dT%H%M%S}.json"
    else:
        stats_file = statsdir / f"stats_{day}_{now:%Y%M%dT%H%M%S}.json"

    logger.info("writing stats to %s", stats_file)
    stats_file.write_text(json.dumps(stats))
