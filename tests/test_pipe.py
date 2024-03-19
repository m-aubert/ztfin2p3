from ztfin2p3.pipe.newpipe import BiasPipe, FlatPipe


def test_bias_init():
    bi = BiasPipe("20190404")
    assert len(bi.init_datafile) == 16
    assert bi.init_datafile.day.unique().tolist() == ["20190404"]
    assert bi.init_datafile.ccdid.tolist() == list(range(1, 17))
    assert bi.init_datafile.filepath.map(len).unique().tolist() == [20]

    bi = BiasPipe("20190404", ccdid=5, nskip=10)
    assert len(bi.init_datafile) == 1
    assert bi.init_datafile.ccdid.tolist() == [5]
    assert bi.init_datafile.filepath.map(len).unique().tolist() == [10]

    bi = BiasPipe(("2019-04-04", "2019-04-08"), ccdid=6, nskip=10)
    days = bi.init_datafile.day.unique().tolist()
    assert days == ["20190404", "20190405", "20190406", "20190407"]
    assert bi.init_datafile.ccdid.unique().tolist() == [6]
    assert bi.init_datafile.filepath.map(len).unique().tolist() == [10]


def test_flat_init():
    fi = FlatPipe("20190404", ccdid=1)
    assert len(fi.init_datafile) == 11
    assert fi.init_datafile.day.unique().tolist() == ["20190404"]
    assert fi.init_datafile.ccdid.unique().tolist() == [1]
    assert fi.init_datafile.ledid.tolist() == [2, 3, 4, 5, 7, 8, 9, 10, 11, 12, 13]
