from setuptools import setup, find_packages

setup(
    name="data_importer",
    version="1",
    packages=find_packages(),
    install_requires=['pyspark']
)
