from setuptools import find_packages, setup

setup(
    name="pymongex",
    version="0.0.2",
    author="Nils Naumann",
    author_email="nils.naumann02@gmail.com",
    packages=find_packages(),
    install_requires=[
        "motor==3.5.1",
        "pymongo[srv]==4.8.0",
        "pydantic==2.8.2",
    ],
)
