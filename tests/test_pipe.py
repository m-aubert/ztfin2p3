import pytest

from ztfin2p3.pipe.newpipe import BiasPipe, FlatPipe


def test_bias_init():
    bi = BiasPipe("20190404")
    assert len(bi.init_df) == 16
    assert bi.init_df.day.unique().tolist() == ["20190404"]
    assert bi.init_df.ccdid.tolist() == list(range(1, 17))
    assert bi.init_df.filepath.map(len).unique().tolist() == [20]

    bi = BiasPipe("20190404", ccdid=5, nskip=10)
    assert len(bi.init_df) == 1
    assert bi.init_df.ccdid.tolist() == [5]
    assert bi.init_df.filepath.map(len).unique().tolist() == [10]

    bi = BiasPipe(("2019-04-04", "2019-04-08"), ccdid=6, nskip=10)
    days = bi.init_df.day.unique().tolist()
    assert days == ["20190404", "20190405", "20190406", "20190407"]
    assert bi.init_df.ccdid.unique().tolist() == [6]
    assert bi.init_df.filepath.map(len).unique().tolist() == [10]


def test_flat_init():
    fi = FlatPipe("20190404", ccdid=1)
    assert len(fi.init_df) == 3
    assert fi.init_df.day.unique().tolist() == ["20190404"]
    assert fi.init_df.ccdid.unique().tolist() == [1]
    assert fi.init_df.ledid.tolist() == [[2, 3, 4, 5], [11, 12, 13], [7, 8, 9, 10]]


def test_bias_fileout():
    bi = BiasPipe("20190404", ccdid=5)
    out = bi.init_df[bi.init_df.ccdid == 5].fileout.iloc[0]
    assert out.endswith("cal/bias/2019/0404/ztfin2p3_20190404_000000_bi_c05_bias.fits")


def test_flat_fileout():
    fi = FlatPipe("20190404", ccdid=1)
    out = fi.init_df[(fi.init_df.ccdid == 1) & (fi.init_df.filterid == "zr")].fileout.iloc[0]
    assert out.endswith(
        "cal/flat/2019/0404/ztfin2p3_20190404_000000_zr_c01_l00_flat.fits"
    )
