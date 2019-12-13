from setuptools import setup, find_packages


def find(directory):
    return [directory] + [directory + "." + s for s in find_packages(directory)]


setup(
    name="data_importer",
    version="1",
    packages=find("data_importer"),
    install_requires=['pyspark']
)
