import os

from setuptools import setup

__author__ = 'Giacomo Berardi <giacbrd.com>'


def readfile(fname):
    path = os.path.join(os.path.dirname(__file__), fname)
    return open(path).read()


setup(
    name='SmartPipeline',
    version='0.1.0',
    description='A framework for developing simple data pipelines',
    long_description=readfile('README.md'),
    classifiers=[
        'Intended Audience :: Science/Research',
        'License :: OSI Approved :: GNU Lesser General Public License v3 or later (LGPLv3+)',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 3',
    ],
    url='https://github.com/giacbrd/SmartPipeline',
    author='Giacomo Berardi',
    author_email='barnets@gmail.com',
    packages=['smartpipeline'],
    tests_require=['pytest'],
    python_requires='>=3'
)
