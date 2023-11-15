# Copyright 2021 Agnostiq Inc.
#
# This file is part of Covalent.
#
# Licensed under the Apache License 2.0 (the "License"). A copy of the
# License may be obtained with this software package or at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Use of this file is prohibited except in compliance with the License.
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


import site
import sys

from setuptools import find_packages, setup

site.ENABLE_USER_SITE = "--user" in sys.argv[1:]

plugins_list = ["hpc = covalent_hpc_plugin.hpc"]

setup_info = {
    "name": "covalent-hpc-plugin",
    "packages": find_packages("."),
    "version": "0.0.9",
    "maintainer": "Andrew S. Rosen",
    "url": "https://github.com/quantum-accelerators/covalent-hpc-plugin",
    "license": "GNU Affero GPL v3.0",
    "author": "Andrew S. Rosen",
    "description": "Covalent HPC Plugin",
    "long_description": open("README.md").read(),
    "long_description_content_type": "text/markdown",
    "include_package_data": True,
    "install_requires": [
        "asyncssh>=2.10.1",
        "aiofiles>=0.8.0",
        "cloudpickle>=2.0.0",
        "covalent>=0.226.0rc0",
        "psij-python>=0.9.3",
    ],
    "classifiers": [
        "Development Status :: 5 - Production/Stable",
        "Environment :: Console",
        "Environment :: Plugins",
        "Intended Audience :: Developers",
        "Intended Audience :: Education",
        "Intended Audience :: Science/Research",
        "License :: Other/Proprietary License",
        "Natural Language :: English",
        "Operating System :: MacOS",
        "Operating System :: POSIX :: Linux",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3 :: Only",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Topic :: Adaptive Technologies",
        "Topic :: Scientific/Engineering",
        "Topic :: Scientific/Engineering :: Interface Engine/Protocol Translator",
        "Topic :: Software Development",
        "Topic :: System :: Distributed Computing",
    ],
    "entry_points": {
        "covalent.executor.executor_plugins": plugins_list,
    },
}

if __name__ == "__main__":
    setup(**setup_info)
