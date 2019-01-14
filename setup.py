import os
from setuptools import setup

setup(
    name="pampro",
    packages=["pampro"],
    version="0.5.1",
    author="Tom White\Ella Hutchinson",
    maintainer="Ella Hutchinson",
    maintainer_email="ella.hutchinson@mrc-epid.cam.ac.uk",
    description=("physical activity monitor processing"),
    url="https://github.com/MRC-Epid/pampro",
    install_requires=['numpy>=1.14.0', 'scipy>=1.1.0', 'matplotlib>=2.2.2', 'h5py>=2.9.0', 'pandas>==0.23.0', 'statsmodels>=0.9.0'],
    Classifiers=[
        "Intended Audience :: Science/Research",
        "Operating System :: Microsoft :: Windows :: Windows 7",
        "Operating System :: POSIX :: Linux",
        "Programming Language :: Python :: 3.6"
    ],
)
