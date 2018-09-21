import os
from setuptools import setup

setup(
    name = "pampro",
    packages = ["pampro"],
    version = "0.5",
    author = "Tom White/Ella Hutchinson",
    description = ("physical activity monitor processing"),
    url = "https://gitlab.mrc-epid.cam.ac.uk/PATT/pampro",
    install_requires = ['numpy', 'scipy', 'matplotlib', 'h5py', 'pandas']
)
