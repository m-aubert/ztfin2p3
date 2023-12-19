#! /usr/bin/env python

from setuptools import setup, find_packages

DESCRIPTION = " ZTFIN2P3 "
LONG_DESCRIPTION = """ ztfin2p3 """

DISTNAME = "ztfin2p3"
AUTHOR = "Mickael Rigault"
MAINTAINER = "Mickael Rigault"
MAINTAINER_EMAIL = "m.rigault@ipnl.in2p3.fr"
URL = "https://github.com/MickaelRigault/ztfin2p3"
LICENSE = "Apache 2.0"
DOWNLOAD_URL = "https://github.com/MickaelRigault/ztfin2p3"
VERSION = "0.3.5"


def check_dependencies():
    install_requires = []
    try:
        import ztfimg
    except ImportError:
        install_requires.append("ztfimg")

    return install_requires


if __name__ == "__main__":
    install_requires = check_dependencies()
    packages = find_packages()

    setup(
        name=DISTNAME,
        author=AUTHOR,
        author_email=MAINTAINER_EMAIL,
        maintainer=MAINTAINER,
        maintainer_email=MAINTAINER_EMAIL,
        description=DESCRIPTION,
        long_description=LONG_DESCRIPTION,
        license=LICENSE,
        url=URL,
        version=VERSION,
        download_url=DOWNLOAD_URL,
        install_requires=install_requires,
        packages=packages,
        package_data={"ztfin2p3": ["config/*"]},  # , 'data/*'
        classifiers=[
            "Intended Audience :: Science/Research",
            "Programming Language :: Python :: 3.6",
            "License :: OSI Approved :: BSD License",
            "Topic :: Scientific/Engineering :: Astronomy",
            "Operating System :: POSIX",
            "Operating System :: Unix",
            "Operating System :: MacOS",
        ],
        entry_points={
            "console_scripts": [
                "ztfin2p3-d2a = ztfin2p3.scripts.detrend2aper:main",
            ]
        },
    )
