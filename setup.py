import os
import setuptools


def openf(fname):
    return open(os.path.join(os.path.dirname(__file__), fname), encoding='utf-8')


setuptools.setup(
    name="mabangapi",
    version="0.0.1",
    author="toby",
    author_email="toby.lee@foxmail.com",
    description="mabang api 工具包",
    long_description=openf("README.md").read(),
    packages=[line.strip() for line in openf("requirements.txt") if line.strip()],
    python_requires=">=3.6",
)
