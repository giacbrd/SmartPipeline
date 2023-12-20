import os

from setuptools import find_packages, setup

__author__ = "Giacomo Berardi <giacbrd.com>"


def readfile(fname):
    path = os.path.join(os.path.dirname(__file__), fname)
    return open(path).read()


setup(
    name="SmartPipeline",
    version="0.7.3",
    description="A framework for fast developing scalable data pipelines following a simple design pattern",
    long_description=readfile("README.rst"),
    long_description_content_type="text/x-rst",
    classifiers=[
        "Topic :: Software Development",
        "Topic :: Scientific/Engineering",
        "Intended Audience :: Developers",
        "Intended Audience :: Science/Research",
        "License :: OSI Approved :: GNU Lesser General Public License v3 or later (LGPLv3+)",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Development Status :: 4 - Beta",
    ],
    url="https://github.com/giacbrd/SmartPipeline",
    project_urls={
        "Documentation": "https://smartpipeline.readthedocs.io",
        "Source": "https://github.com/giacbrd/SmartPipeline",
    },
    keywords=[
        "data pipeline",
        "task queue",
        "data science",
        "machine learning",
        "design pattern",
    ],
    author="Giacomo Berardi",
    author_email="barnets@gmail.com",
    packages=find_packages(exclude=["tests"]),
    package_data={
        "smartpipeline": ["py.typed"],
    },
    tests_require=["pytest", "coverage"],
    python_requires=">=3.9",
)
