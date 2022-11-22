
from .basepipe import CalibPipe
from ..builder import CalibrationBuilder
from .. import io
# BasePipe has
#     - config and co.
#     - datafile and co.
#     - use_dask

__all__ = ["BiasPipe"]


class BiasPipe( CalibPipe ):
    _KIND = "bias"

    
