from setuptools import setup, find_packages
import pathlib

here = pathlib.Path(__file__).parent.resolve()

setup(
    name='quectel-runner',
    version='v1.0.0',
    packages=find_packages(where='.'),
    install_requires=[
        "argparse-formatter>=1.4",
        "construct~=2.10.67",
        "gpstime>=0.6.2",
        "psutil>=5.9.4",
        "pynmea2~=1.18.0",
        "pyserial~=3.5",
        "urllib3>=1.21.1",
        "websockets>=10.1",
        "fusion-engine-client @ https://github.com/PointOneNav/fusion-engine-client/archive/c8c97662b6142560e5d5f610481ff367fda62299.zip#egg=fusion-engine-client&subdirectory=python",
        # Note: Using the P1 fork of ntripstreams until fixes are mainlined.
        "ntripstreams @ https://github.com/PointOneNav/ntripstreams/archive/c26605710a53a1ebe1a16310565b5605e77228c1.zip#egg=ntripstreams",
    ],
)
