"""Python Methods for Google Cloud Platform.

Copyright (c) 2025 Colin Dietrich
MIT License, see LICENSE file for complete text.
"""

import io
import os

import setuptools


name = "pygcp"
description = "Python Methods for Google Cloud Platform"

# status should be one of:
release_status = "Development Status :: 3 - Alpha"
# release_status = "Development Status :: 4 - Beta"
# release_status = 'Development Status :: 5 - Production/Stable'

dependencies = [
    'google-api-python-client',
    'google-auth',
    'google-cloud-aiplatform',
    'google-cloud-bigquery',
    'google-cloud-dlp',
    'google-cloud-language',
    'google-cloud-logging',
    'google-cloud-secret-manager',
    'google-cloud-storage',
    'google-crc32c',
    'google-oauth',
    'googlemaps',
    'gspread-formatting',
    'gspread-pandas',
    'pandas',
    'pandas-gbq',
    'tqdm',
    'vertexai'
    ]
extras = {
    }

# Setup boilerplate below this line.

package_root = os.path.abspath(os.path.dirname(__file__))

readme_filename = os.path.join(package_root, "README.md")
with io.open(readme_filename, encoding="utf-8") as readme_file:
    readme = readme_file.read()

version = 0.1

packages = [
    ]


setuptools.setup(
    name=name,
    version=version,
    description=description,
    long_description=readme,
    author="Colin Dietrich",
    author_email="repos@wildjuniper.com",
    license="MIT",
    url="None",
    classifiers=[
        release_status,
        "Intended Audience :: Developers",
        "Intended Audience :: Science/Research",
        "Operating System :: OS Independent",
        "License :: OSI Approved :: BSD License",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Operating System :: OS Independent",
        "Topic :: Internet",
        "Topic :: Scientific/Engineering",
    ],
    platforms="Posix; MacOS X; Windows",
    packages=packages,
    install_requires=dependencies,
    extras_require=extras,
    python_requires=">=3.9",
    include_package_data=True,
    zip_safe=False,
)
