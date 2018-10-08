import os
from setuptools import setup

setup(
    name = "pampro",
    packages = ["pampro"],
    version = "0.5.0",
    author = "Tom White",
    maintainer = "Ella Hutchinson",
    maintainer_email = "ella.hutchinson@mrc-epid.cam.ac.uk",
    description = ("physical activity monitor processing"),
    url = "https://gitlab.mrc-epid.cam.ac.uk/PATT/pampro",
    install_requires=['numpy', 'scipy', 'matplotlib', 'h5py', 'pandas'],
    Classifiers = [
        "Intended Audience :: Science/Research",
        "License :: OSI Approved :: GNU General Public License (GPL)",
        "Operating System :: Microsoft :: Windows :: Windows 7",
        "Operating System :: POSIX :: Linux",
        "Programming Language :: Python :: 3.6"
    ],
)
