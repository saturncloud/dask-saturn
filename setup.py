import os
from setuptools import setup, find_packages

import versioneer

install_requires = ["distributed", "requests"]


setup(
    name="dask-saturn",
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
    maintainer="Saturn Cloud Developers",
    maintainer_email="dev@saturncloud.io",
    license="BSD-3-Clause",
    classifiers=[
        "Development Status :: 4 - Beta",
        "License :: OSI Approved :: BSD License",
        "Intended Audience :: Developers",
        "Intended Audience :: Science/Research",
        "Topic :: Scientific/Engineering",
        "Topic :: System :: Distributed Computing",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
    ],
    keywords="dask saturn cloud distributed cluster",
    description="Dask Cluster objects in Saturn Cloud",
    long_description=(open("README.md").read() if os.path.exists("README.md") else ""),
    long_description_content_type="text/markdown",
    url="https://saturncloud.io/",
    project_urls={
        "Documentation": "http://docs.saturncloud.io",
        "Source": "https://github.com/saturncloud/dask-saturn",
        "Issue Tracker": "https://github.com/saturncloud/dask-saturn/issues",
    },
    packages=find_packages(),
    package_data={"dask_saturn": ["*.yaml"]},
    install_requires=install_requires,
    tests_require=["pytest"],
    zip_safe=False,
)
