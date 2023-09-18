# Copyright 2021 Agnostiq Inc.
#
# This file is part of Covalent.
#
# Licensed under the GNU Affero General Public License 3.0 (the "License").
# A copy of the License may be obtained with this software package or at
#
#      https://www.gnu.org/licenses/agpl-3.0.en.html
#
# Use of this file is prohibited except in compliance with the License. Any
# modifications or derivative works of this file must retain this copyright
# notice, and modified files must contain a notice indicating that they have
# been altered from the originals.
#
# Covalent is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
# FITNESS FOR A PARTICULAR PURPOSE. See the License for more details.
#
# Relief from the License may be granted by purchasing a commercial license.

"""HPC executor plugin for the Covalent dispatcher."""

from __future__ import annotations

import asyncio
import datetime
import os
import sys
from pathlib import Path
from typing import Literal, TypedDict

import aiofiles
import asyncssh
import cloudpickle as pickle
import covalent as ct
from aiofiles import os as async_os
from covalent._results_manager.result import Result
from covalent._shared_files import logger
from covalent._shared_files.config import get_config
from covalent.executor.base import AsyncBaseExecutor

app_log = logger.app_log
log_stack_info = logger.log_stack_info

_EXECUTOR_PLUGIN_DEFAULTS = {
    # SSH credentials
    "address": "",
    "username": "",
    "ssh_key_file": "~/.ssh/id_rsa",
    "cert_file": None,
    # PSI/J parameters
    "instance": "slurm",
    "launcher": "single",
    "inherit_environment": True,
    "environment": {},
    "resource_spec_kwargs": {
        "node_count": 1,
        "processes_per_node": 1,
        "gpu_cores_per_process": 0,
    },
    "job_attributes_kwargs": {
        "duration": 10,  # minutes
        "queue_name": None,
        "project_name": None,
        "custom_attributes": None,
    },
    # Pre/Post-launch commands
    "pre_launch_cmds": [],
    "post_launch_cmds": [],
    "shebang": "#!/bin/bash",
    # Remote Python env parameters
    "remote_python_exe": "python",
    "remote_conda_env": None,
    # Covalent parameters
    "remote_workdir": "~/covalent-workdir",
    "create_unique_workdir": False,
    "cache_dir": str(Path(get_config("dispatcher.cache_dir")).expanduser().resolve()),
    "poll_freq": 60,
}
_DEFAULT = object()

executor_plugin_name = "HPCExecutor"


class ResourceSpecV1Hint(TypedDict):
    """
    Type hint for PSI/J `ResourceSpecV1` object.

    Reference: https://exaworks.org/psij-python/docs/v/0.9.0/.generated/psij.html#psij.resource_spec.ResourceSpecV1
    """

    node_count: int
    process_count: int
    processes_per_node: int
    cpu_cores_per_process: int
    gpu_cores_per_process: int
    exclusive_node_use: bool


class JobAttributesHint(TypedDict):
    """
    Type hint for PSI/J `JobAttributes` object.

    Reference: https://exaworks.org/psij-python/docs/v/0.9.0/.generated/psij.html#psij.JobAttributes
    """

    duration: datetime.timedelta | int | float
    queue_name: str | None
    project_name: str | None
    reservation_id: str | None
    custom_attributes: dict[str, object] | None


class HPCExecutor(AsyncBaseExecutor):
    """
    HPC executor plugin class, built around PSI/J.

    This plugin requires that Covalent and PSI/J exist in the remote machine's Python environment.

    Args:
        address: Remote address or hostname of the login node (e.g. "coolmachine.university.edu").
        username: Username used to authenticate over SSH (i.e. what you use to login to `address`).
            The default is None (i.e. no username is required).
        ssh_key_file: Private RSA key used to authenticate over SSH.
            The default is "~/.ssh/id_rsa". If no key is required, set this as None.
        cert_file: Certificate file used to authenticate over SSH, if required (usually has extension .pub).
            The default is None. If no certificate is required, leave this as None.
        instance: The PSI/J `JobExecutor` instance (i.e. job scheduler) to use for job submission.
            Must be one of: "cobalt", "flux", "lsf", "pbspro", "rp", "slurm". Defaults to "slurm".
        launcher: The PSI/J `JobSpec` launcher to use for the job.
            Must be one of: "aprun", "jsrun", "mpirun", "multiple", "single", "srun". Defaults to "single".
        resource_spec_kwargs: The PSI/J keyword arguments for `ResourceSpecV1`, which describes the resources to
            reserve on the scheduling system. Defaults to None, which is equivalent to the PSI/J defaults.
        job_attributes_kwargs: The PSI/J keyword arguments for `JobAttributes`, which describes information about how the
            job is queued and run. Defaults to None, which is equivalent to the PSI/J defaults.
        inherit_environment: Whether the job should inherit the parent environment. Defaults to True.
        environment: Environment variables to set for the job. Defaults to None, which is equivalent to {}.
        pre_launch_cmds: List of shell-compatible commands to run before launching the job. Defaults to None.
        post_launch_cmds: List of shell-compatible commands to run after launching the job. Defaults to None.
        shebang: Shebang to use for pre-launch and post-launch commands. Defaults to "#!/bin/bash".
        remote_python_exe: Python executable to use for job submission. Defaults to "python".
        remote_conda_env: Conda environment to activate on the remote machine. Defaults to None.
        remote_workdir: Working directory on the remote cluster. Defaults to "~/covalent-workdir".
        create_unique_workdir: Whether to create a unique working (sub)directory for each task.
            Defaults to False.
        cache_dir: Local cache directory used by this executor for temporary files.
            Defaults to the dispatcher's cache directory.
        poll_freq: Frequency with which to poll a submitted job. Defaults to 60. Note that settings this value
            to be significantly smaller is not advised, as it will cause too frequent SSHs into the remote machine.
        log_stdout: Path to file to log stdout to. Defaults to "" (i.e. no logging).
        log_stderr: Path to file to log stderr to. Defaults to "" (i.e. no logging).
        time_limit: time limit for the task (in seconds). Defaults to -1 (i.e. no time limit). Note that this is
            not the same as the job scheduler's time limit, which is set in `job_attributes_kwargs`.
        retries: Number of times to retry execution upon failure. Defaults to 0 (i.e. no retries).
    """

    def __init__(
        self,
        # SSH credentials
        address: str = _DEFAULT,
        username: str | None = _DEFAULT,
        ssh_key_file: str | Path | None = _DEFAULT,
        cert_file: str | Path | None = _DEFAULT,
        # PSI/J parameters
        instance: Literal["cobalt", "flux", "lsf", "pbspro", "rp", "slurm"] = _DEFAULT,
        launcher: Literal["aprun", "jsrun", "mpirun", "multiple", "single", "srun"] = _DEFAULT,
        resource_spec_kwargs: ResourceSpecV1Hint = _DEFAULT,
        job_attributes_kwargs: JobAttributesHint = _DEFAULT,
        inherit_environment: bool = _DEFAULT,
        environment: dict[str, str] | None = _DEFAULT,
        # Pre/Post-launch commands
        pre_launch_cmds: list[str] | None = _DEFAULT,
        post_launch_cmds: list[str] | None = _DEFAULT,
        shebang: str | None = _DEFAULT,
        # Remote Python env parameters
        remote_python_exe: str = _DEFAULT,
        remote_conda_env: str | None = _DEFAULT,
        # Covalent parameters
        remote_workdir: str | Path = _DEFAULT,
        create_unique_workdir: bool = _DEFAULT,
        cache_dir: str | Path = _DEFAULT,
        poll_freq: int = _DEFAULT,
        # AsyncBaseExecutor parameters
        log_stdout: str = "",
        log_stderr: str = "",
        time_limit: int = -1,
        retries: int = 0,
    ):
        super().__init__(
            log_stdout=log_stdout, log_stderr=log_stderr, time_limit=time_limit, retries=retries
        )

        config = ct.get_config()
        hpc_config = config["executors"].get("hpc", {})

        # SSH credentials
        self.address = (
            address
            if address != _DEFAULT
            else hpc_config["address"]
            if "address" in hpc_config
            else _EXECUTOR_PLUGIN_DEFAULTS["address"]
        )

        self.username = (
            username
            if username != _DEFAULT
            else hpc_config["username"]
            if "username" in hpc_config
            else _EXECUTOR_PLUGIN_DEFAULTS["username"]
        )

        self.ssh_key_file = (
            ssh_key_file
            if ssh_key_file != _DEFAULT
            else hpc_config["ssh_key_file"]
            if "ssh_key_file" in hpc_config
            else _EXECUTOR_PLUGIN_DEFAULTS["ssh_key_file"]
        )

        self.cert_file = (
            cert_file
            if cert_file != _DEFAULT
            else hpc_config["cert_file"]
            if "cert_file" in hpc_config
            else _EXECUTOR_PLUGIN_DEFAULTS["cert_file"]
        )

        # PSI/J parameters
        self.instance = (
            instance
            if instance != _DEFAULT
            else hpc_config["instance"]
            if "instance" in hpc_config
            else _EXECUTOR_PLUGIN_DEFAULTS["instance"]
        )

        self.launcher = (
            launcher
            if launcher != _DEFAULT
            else hpc_config["launcher"]
            if "launcher" in hpc_config
            else _EXECUTOR_PLUGIN_DEFAULTS["launcher"]
        )

        self.resource_spec_kwargs = (
            resource_spec_kwargs
            if resource_spec_kwargs != _DEFAULT
            else hpc_config["resource_spec_kwargs"]
            if "resource_spec_kwargs" in hpc_config
            else _EXECUTOR_PLUGIN_DEFAULTS["resource_spec_kwargs"]
        )

        self.job_attributes_kwargs = (
            job_attributes_kwargs
            if job_attributes_kwargs != _DEFAULT
            else hpc_config["job_attributes_kwargs"]
            if "job_attributes_kwargs" in hpc_config
            else _EXECUTOR_PLUGIN_DEFAULTS["job_attributes_kwargs"]
        )

        self.inherit_environment = (
            inherit_environment
            if inherit_environment != _DEFAULT
            else hpc_config["inherit_environment"]
            if "inherit_environment" in hpc_config
            else _EXECUTOR_PLUGIN_DEFAULTS["inherit_environment"]
        )

        self.environment = (
            environment
            if environment != _DEFAULT
            else hpc_config["environment"]
            if "environment" in hpc_config
            else _EXECUTOR_PLUGIN_DEFAULTS["environment"]
        )

        # Pre/Post-launch commands
        self.pre_launch_cmds = (
            pre_launch_cmds
            if pre_launch_cmds != _DEFAULT
            else hpc_config["pre_launch_cmds"]
            if "pre_launch_cmds" in hpc_config
            else _EXECUTOR_PLUGIN_DEFAULTS["pre_launch_cmds"]
        )

        self.post_launch_cmds = (
            post_launch_cmds
            if post_launch_cmds != _DEFAULT
            else hpc_config["post_launch_cmds"]
            if "post_launch_cmds" in hpc_config
            else _EXECUTOR_PLUGIN_DEFAULTS["post_launch_cmds"]
        )

        self.shebang = (
            shebang
            if shebang != _DEFAULT
            else hpc_config["shebang"]
            if "shebang" in hpc_config
            else _EXECUTOR_PLUGIN_DEFAULTS["shebang"]
        )

        # Remote Python environment parameters
        self.remote_python_exe = (
            remote_python_exe
            if remote_python_exe != _DEFAULT
            else hpc_config["remote_python_exe"]
            if "remote_python_exe" in hpc_config
            else _EXECUTOR_PLUGIN_DEFAULTS["remote_python_exe"]
        )

        self.remote_conda_env = (
            remote_conda_env
            if remote_conda_env != _DEFAULT
            else hpc_config["remote_conda_env"]
            if "remote_conda_env" in hpc_config
            else _EXECUTOR_PLUGIN_DEFAULTS["remote_conda_env"]
        )

        # Covalent parameters
        self.remote_workdir = (
            remote_workdir
            if remote_workdir != _DEFAULT
            else hpc_config["remote_workdir"]
            if "remote_workdir" in hpc_config
            else _EXECUTOR_PLUGIN_DEFAULTS["remote_workdir"]
        )

        self.create_unique_workdir = (
            create_unique_workdir
            if create_unique_workdir != _DEFAULT
            else hpc_config["create_unique_workdir"]
            if "create_unique_workdir" in hpc_config
            else _EXECUTOR_PLUGIN_DEFAULTS["create_unique_workdir"]
        )

        self.cache_dir = (
            cache_dir
            if cache_dir != _DEFAULT
            else hpc_config["cache_dir"]
            if "cache_dir" in hpc_config
            else _EXECUTOR_PLUGIN_DEFAULTS["cache_dir"]
        )

        self.poll_freq = (
            poll_freq
            if poll_freq != _DEFAULT
            else hpc_config["poll_freq"]
            if "poll_freq" in hpc_config
            else _EXECUTOR_PLUGIN_DEFAULTS["poll_freq"]
        )

        if self.poll_freq < 30:
            print("Polling frequency will be increased to 30 seconds.")
            self.poll_freq = 30

        # Make sure local cache dir exists
        os.makedirs(self.cache_dir, exist_ok=True)

    def _format_pickle_script(self) -> str:
        """
        Create the Python script that executes the pickled python function.

        Returns:
            script: String object containing a Python script.
        """

        return f"""
from pathlib import Path

import cloudpickle as pickle

with open(Path("{self._remote_func_filepath}").expanduser().resolve(), "rb") as f:
    function, args, kwargs = pickle.load(f)

result = None
exception = None

try:
    result = function(*args, **kwargs)
except Exception as e:
    exception = e

with open(Path("{self._remote_result_filepath}").expanduser().resolve(), "wb") as f:
    pickle.dump((result, exception), f)
"""

    def _format_job_script(self) -> str:
        """
        Create the PSI/J Python script that will submit the compute job to the scheduler.

        NOTE: When PSI/J Remote is released, we can do this directly on the Covalent server
        instead of having to copy the script to the remote machine and running it there.

        Returns:
            String representation of the Python script to make/execute the PSI/J Job object.
        """
        
        # Validate/clean up parameters
        if not isinstance(self.job_attributes_kwargs.get("duration"), datetime.timedelta):
            self.job_attributes_kwargs["duration"] = datetime.timedelta(
                minutes=self.job_attributes_kwargs["duration"]
            )

        resources_string = (
            f"resources=ResourceSpecV1(**{self.resource_spec_kwargs}),"
            if self.resource_spec_kwargs
            else ""
        )
        attributes_string = (
            f"attributes=JobAttributes(**{self.job_attributes_kwargs}),"
            if self.job_attributes_kwargs
            else ""
        )
        post_launch_string = (
            f"post_launch=Path('{self._remote_post_launch_filepath}').expanduser().resolve(),"
            if self.post_launch_cmds
            else ""
        )

        return f"""
import datetime
from pathlib import Path
from psij import Job, JobAttributes, JobExecutor, JobSpec, ResourceSpecV1

job_executor = JobExecutor.get_instance("{self.instance}")

job = Job(
    JobSpec(
        name="{self._name}",
        executable="{self.remote_python_exe}",
        environment={self.environment},
        launcher="{self.launcher}",
        arguments=[str(Path("{self._remote_pickle_script_filepath}").expanduser().resolve())],
        directory=Path("{self._job_remote_workdir}").expanduser().resolve(),
        stdout_path=Path("{self._remote_stdout_filepath}").expanduser().resolve(),
        stderr_path=Path("{self._remote_stderr_filepath}").expanduser().resolve(),
        pre_launch=Path('{self._remote_pre_launch_filepath}').expanduser().resolve(),
        {post_launch_string}
        {resources_string}
        {attributes_string}
    )
)

job_executor.submit(job)
native_id = job.native_id
print(native_id)
"""

    def _format_query_status_script(self) -> str:
        """
        Create the PSI/J Python script to query the submitted job status.

        NOTE: When PSI/J Remote is released, we can do this directly on the Covalent server
        instead of having to copy the script to the remote machine and running it there.

        Returns:
            String representation of the Python script to query the job status with PSI/J.
        """

        # NOTE: Once https://github.com/ExaWorks/psij-python/issues/400 is resolved,
        # we can change it to just `target_states=[JobState.QUEUED]`.

        # NOTE: Once https://github.com/ExaWorks/psij-python/issues/401 is resolved,
        # we can remove the `timeout` argument and set `state = job_status.state`.

        return f"""
import datetime
from psij import Job, JobExecutor, JobState

job_executor = JobExecutor.get_instance("{self.instance}")

job = Job()
job_executor.attach(job, f"{self._jobid}")
job_status = job.wait(
    target_states=[
        JobState.QUEUED,
        JobState.CANCELED,
        JobState.ACTIVE,
        JobState.FAILED,
        JobState.COMPLETED,
    ],
    timeout = datetime.timedelta(seconds=10),
)
state = job_status.state or JobState.COMPLETED
print(state.name)
"""

    def _format_pre_launch_script(self) -> str:
        """
        Create the pre-launch script to activate the conda environment,
        check the Python version, and run any user-requested commands.

        Returns:
            String representation of the pre-launch script.
        """
        pre_launch_script = f"{self.shebang}\n" if self.shebang else ""

        if self.remote_conda_env:
            pre_launch_script += f"""
CONDA_BASE=$(conda info --base)
source $CONDA_BASE/etc/profile.d/conda.sh
conda activate {self.remote_conda_env}
retval=$?
if [ $retval -ne 0 ] ; then
    >&2 echo "Conda environment {self.remote_conda_env} is not present on the compute node. "\
    "Please create the environment and try again."
    exit 99
fi
"""
        pre_launch_script += f"""
remote_py_version=$(python -c "print('.'.join(map(str, __import__('sys').version_info[:2])))")
if [[ "{self._remote_python_version}" != $remote_py_version ]] ; then
    >&2 echo "Python version mismatch. Please install Python {self._remote_python_version} in the compute environment."
    exit 199
fi
"""
        if self.pre_launch_cmds:
            pre_launch_script += "".join(f"{cmd}\n" for cmd in self.pre_launch_cmds)

        return pre_launch_script

    def _format_post_launch_script(self) -> str:
        """
        Create the post-launch script to run any user-requested commands.

        Returns:
            String representation of the post-launch script.
        """
        post_launch_script = f"{self.shebang}\n" if self.shebang else ""
        if self.remote_conda_env:
            post_launch_script += f"""
CONDA_BASE=$(conda info --base)
source $CONDA_BASE/etc/profile.d/conda.sh
conda activate {self.remote_conda_env}
"""
        post_launch_script += "".join(f"{cmd}\n" for cmd in self.post_launch_cmds)

        return post_launch_script

    async def _client_connect(self) -> asyncssh.SSHClientConnection:
        """
        Helper function for connecting to the remote host through the asyncssh module.

        Returns:
            The connection object
        """

        if not self.address:
            raise ValueError("address is a required parameter.")

        # Define paths to private key and certificate files
        self.cert_file = Path(self.cert_file).expanduser().resolve() if self.cert_file else None
        self.ssh_key_file = (
            Path(self.ssh_key_file).expanduser().resolve() if self.ssh_key_file else None
        )

        # Read the private key and certificate files
        if self.ssh_key_file:
            if self.cert_file:
                client_keys = [
                    (
                        asyncssh.read_private_key(self.ssh_key_file),
                        asyncssh.read_certificate(self.cert_file),
                    )
                ]
            else:
                client_keys = [asyncssh.read_private_key(self.ssh_key_file)]
        elif self.cert_file:
            raise ValueError("ssh_key_file is required if cert_file is provided.")
        else:
            client_keys = []

        # Connect to the remote host
        try:
            conn = await asyncssh.connect(
                self.address,
                username=self.username,
                client_keys=client_keys,
                known_hosts=None,
            )
        except Exception as e:
            raise RuntimeError(
                f"Could not use asyncssh to connect to host: '{self.address}' as user: '{self.username}'",
                e,
            )

        return conn

    async def run(
        self, function: callable, args: list, kwargs: dict, task_metadata: dict
    ) -> Result:
        """
        Run a function on the remote machine.

        Args:
            function: Function to be executed.
            args: List of positional arguments to be passed to the function.
            kwargs: Dictionary of keyword arguments to be passed to the function.
            task_metadata: Dictionary of metadata associated with the task.

        Returns:
            result: Result object containing the result of the function execution.
        """

        # Set up variables
        dispatch_id = task_metadata["dispatch_id"]
        node_id = task_metadata["node_id"]
        self._name = f"{dispatch_id}-{node_id}"

        results_dir = task_metadata["results_dir"]
        self._task_results_dir = Path(results_dir) / dispatch_id
        self._job_remote_workdir = (
            Path(self.remote_workdir) / dispatch_id / f"node_{node_id}"
            if self.create_unique_workdir
            else Path(self.remote_workdir)
        )

        result_filename = f"result-{dispatch_id}-{node_id}.pkl"
        job_script_filename = f"psij-{dispatch_id}-{node_id}.py"
        pickle_script_filename = f"script-{dispatch_id}-{node_id}.py"
        query_status_script_filename = f"query-status-{dispatch_id}-{node_id}.py"
        func_filename = f"func-{dispatch_id}-{node_id}.pkl"
        stdout_filename = f"stdout-{dispatch_id}-{node_id}.log"
        stderr_filename = f"stderr-{dispatch_id}-{node_id}.log"
        pre_launch_file = f"pre-launch-{dispatch_id}-{node_id}.sh"
        post_launch_file = f"post-launch-{dispatch_id}-{node_id}.sh"

        self._remote_result_filepath = self._job_remote_workdir / result_filename
        self._remote_jobscript_filepath = self._job_remote_workdir / job_script_filename
        self._remote_pickle_script_filepath = self._job_remote_workdir / pickle_script_filename
        self._remote_query_script_filepath = (
            self._job_remote_workdir / query_status_script_filename
        )
        self._remote_func_filepath = self._job_remote_workdir / func_filename
        self._remote_stdout_filepath = self._job_remote_workdir / stdout_filename
        self._remote_stderr_filepath = self._job_remote_workdir / stderr_filename
        self._remote_pre_launch_filepath = self._job_remote_workdir / pre_launch_file
        self._remote_post_launch_filepath = self._job_remote_workdir / post_launch_file

        # Establish connection
        conn = await self._client_connect()

        # Check if the remote Python version is compatible with the local Python version
        self._remote_python_version = ".".join(function.args[0].python_version.split(".")[:2])
        app_log.debug(f"Remote Python version: {self._remote_python_version}")

        # Create the remote directory
        app_log.debug(f"Creating remote work directory: {self._job_remote_workdir}")
        cmd_mkdir_remote = f"mkdir -p {self._job_remote_workdir}"
        proc_mkdir_remote = await conn.run(cmd_mkdir_remote)

        if client_err := proc_mkdir_remote.stderr.strip():
            raise RuntimeError(f"Making remote directory failed: {client_err}")

        # Pickle the function, write to file, and copy to remote filesystem
        async with aiofiles.tempfile.NamedTemporaryFile(dir=self.cache_dir) as temp:
            app_log.debug("Writing pickled function, args, kwargs to file")
            await temp.write(pickle.dumps((function, args, kwargs)))
            await temp.flush()

            app_log.debug(
                f"Copying pickled function to remote filesystem: {self._remote_func_filepath}"
            )
            await asyncssh.scp(temp.name, (conn, self._remote_func_filepath))

        # Format the function execution script, write to file, and copy to remote filesystem
        async with aiofiles.tempfile.NamedTemporaryFile(dir=self.cache_dir, mode="w") as temp:
            python_exec_script = self._format_pickle_script()
            app_log.debug("Writing python run-function script to tempfile")
            await temp.write(python_exec_script)
            await temp.flush()

            app_log.debug(
                f"Copying python run-function to remote filesystem: {self._remote_pickle_script_filepath}"
            )
            await asyncssh.scp(temp.name, (conn, self._remote_pickle_script_filepath))

        # Make the pre launch file
        async with aiofiles.tempfile.NamedTemporaryFile(dir=self.cache_dir, mode="w") as temp:
            pre_launch_script = self._format_pre_launch_script()
            app_log.debug("Writing pre launch file")
            await temp.write(pre_launch_script)
            await temp.flush()

            app_log.debug(
                f"Copying pre launch script to remote filesystem: {self._remote_pre_launch_filepath}"
            )
            await asyncssh.scp(temp.name, (conn, self._remote_pre_launch_filepath))

            cmd_chmod = f"chmod +x {self._remote_pre_launch_filepath}"
            proc_chmod = await conn.run(cmd_chmod)

            if client_err := proc_chmod.stderr.strip():
                raise RuntimeError(f"Changing permissions failed with file: {proc_chmod}")

        if self.post_launch_cmds:
            async with aiofiles.tempfile.NamedTemporaryFile(dir=self.cache_dir, mode="w") as temp:
                post_launch_script = self._format_post_launch_script()
                app_log.debug("Writing post launch file")
                await temp.write(post_launch_script)
                await temp.flush()

                app_log.debug(
                    f"Copying post launch script to remote filesystem: {self._remote_post_launch_filepath}"
                )
                await asyncssh.scp(temp.name, (conn, self._remote_post_launch_filepath))

                cmd_chmod = f"chmod +x {self._remote_post_launch_filepath}"
                proc_chmod = await conn.run(cmd_chmod)

                if client_err := proc_chmod.stderr.strip():
                    raise RuntimeError(f"Changing permissions failed with file: {proc_chmod}")

        # ---------------------------------------------------------------------------------------------
        # NOTE: When PSI/J Remote is released, we can do the following server-side instead.
        # ---------------------------------------------------------------------------------------------

        # Format the PSI/J Python submit script, write to file, and copy to remote filesystem
        async with aiofiles.tempfile.NamedTemporaryFile(dir=self.cache_dir, mode="w") as temp:
            submit_script = self._format_job_script()
            app_log.debug("Writing PSI/J submit script to tempfile")
            await temp.write(submit_script)
            await temp.flush()

            app_log.debug(
                f"Copying PSI/J submit script to remote filesystem: {self._remote_jobscript_filepath} ..."
            )
            await asyncssh.scp(temp.name, (conn, self._remote_jobscript_filepath))

        # Execute the job submission Python script
        app_log.debug("Submitting the job")
        cmd = f"{self.remote_python_exe} {self._remote_jobscript_filepath}"
        if self.remote_conda_env:
            cmd = f"conda activate {self.remote_conda_env} &&" + cmd
        proc = await conn.run(cmd)

        if proc.returncode != 0:
            raise RuntimeError(f"Job submission failed: {proc.stderr.strip()}")

        app_log.debug(f"Job submitted with stdout: {self._remote_stdout_filepath}")
        self._jobid = proc.stdout.strip()

        # Format the Python script to query the job status, write to file, and copy to remote filesystem
        async with aiofiles.tempfile.NamedTemporaryFile(dir=self.cache_dir, mode="w") as temp:
            python_query_status_script = self._format_query_status_script()
            app_log.debug("Writing PSI/J query status script to tempfile")
            await temp.write(python_query_status_script)
            await temp.flush()

            app_log.debug(
                f"Copying PSI/J query status script to remote filesystem: {self._remote_query_script_filepath}"
            )
            await asyncssh.scp(temp.name, (conn, self._remote_query_script_filepath))

        app_log.debug(f"Polling job scheduler with job_id: {self._jobid}")
        await self._poll_scheduler(conn)

        # ---------------------------------------------------------------------------------------------

        app_log.debug(f"Fetching result with job_id: {self._jobid}")
        result, stdout, stderr, exception = await self._fetch_result(conn)

        print(stdout)
        print(stderr, file=sys.stderr)

        if exception:
            raise RuntimeError(f"Fetching job result failed: {stderr}")

        app_log.debug("Preparing for teardown")

        app_log.debug("Closing SSH connection")
        conn.close()
        await conn.wait_closed()
        app_log.debug("SSH connection closed, returning result")

        return result

    async def get_status(self, conn: asyncssh.SSHClientConnection) -> Result | str:
        """
        Query the status of a job previously submitted to the job scheduler.

        Args:
            conn: SSH connection object.

        Returns:
            status: String describing the job status.
        """

        if not hasattr(self, "_jobid"):
            return Result.NEW_OBJ

        cmd = f"{self.remote_python_exe} {self._remote_query_script_filepath}"
        if self.remote_conda_env:
            cmd = f"conda activate {self.remote_conda_env} &&" + cmd
        proc = await conn.run(cmd)

        if proc.returncode != 0:
            raise RuntimeError(f"Getting job status failed: {proc.stderr.strip()}")

        return proc.stdout.strip()

    async def _poll_scheduler(self, conn: asyncssh.SSHClientConnection) -> None:
        """
        Poll for the job status until completion.

        Args:
            conn: SSH connection object.

        Returns:
            None
        """

        # Poll status every `poll_freq` seconds
        status = await self.get_status(conn)

        # Continue to poll until the job is done
        while status in {"QUEUED", "ACTIVE"}:
            await asyncio.sleep(self.poll_freq)
            status = await self.get_status(conn)

        if status != "COMPLETED":
            raise RuntimeError(f"Status for job with native ID {self._jobid}: {status}.")

    async def _fetch_result(
        self,
        conn: asyncssh.SSHClientConnection,
    ) -> tuple[Result, str, str, Exception]:
        """
        Query and retrieve the task result including stdout and stderr logs.

        Args:
            conn: SSH connection object.

        Returns:
            result: Task result.
            stdout: stdout log.
            stderr: stderr log.
            exception: Exception raised during task execution.
        """

        # Check the result file exists on the remote backend
        proc = await conn.run(f"test -e {self._remote_result_filepath}")

        if proc.returncode != 0:
            raise FileNotFoundError(
                proc.returncode, proc.stderr.strip(), self._remote_result_filepath
            )

        # Copy result file from remote machine to Covalent server
        local_result_filename = self._task_results_dir / self._remote_result_filepath.name
        await asyncssh.scp((conn, self._remote_result_filepath), local_result_filename)

        # Copy stdout, stderr from remote machine to Covalent server
        local_stdout_file = self._task_results_dir / self._remote_stdout_filepath.name
        local_stderr_file = self._task_results_dir / self._remote_stderr_filepath.name

        await asyncssh.scp((conn, self._remote_stdout_filepath), local_stdout_file)
        await asyncssh.scp((conn, self._remote_stderr_filepath), local_stderr_file)

        async with aiofiles.open(local_result_filename, "rb") as f:
            contents = await f.read()
            result, exception = pickle.loads(contents)
        await async_os.remove(local_result_filename)

        async with aiofiles.open(local_stdout_file, "r") as f:
            stdout = await f.read()
        await async_os.remove(local_stdout_file)

        async with aiofiles.open(local_stderr_file, "r") as f:
            stderr = await f.read()
        await async_os.remove(local_stderr_file)

        return result, stdout, stderr, exception

    async def teardown(self, task_metadata: dict) -> None:
        """
        Perform cleanup on remote machine.

        Args:
            task_metadata: Dictionary of metadata associated with the task.
                Even though it's not used here, it is always passed by Covalent
                and can't be removed.

        Returns:
            None
        """
        app_log.debug("Performing cleanup on remote...")
        conn = await self._client_connect()
        await self._perform_cleanup(conn)

        app_log.debug("Closing SSH connection...")
        conn.close()
        await conn.wait_closed()
        app_log.debug("SSH connection closed, teardown complete")

    async def _perform_cleanup(self, conn: asyncssh.SSHClientConnection) -> None:
        """
        Function to perform cleanup on remote machine.

        NOTE: When PSI/J Remote is released, the following files will no longer need to be
        deleted: `self._remote_jobscript_filepath`, `self._remote_query_script_filepath`.

        Args:
            conn: SSH connection object

        Returns:
            None
        """
        files_to_remove = [
            self._remote_func_filepath,
            self._remote_pickle_script_filepath,
            self._remote_pre_launch_filepath,
            self._remote_jobscript_filepath,
            self._remote_query_script_filepath,
            self._remote_result_filepath,
            self._remote_stdout_filepath,
            self._remote_stderr_filepath,
        ]
        if self.post_launch_cmds:
            files_to_remove.append(self._remote_post_launch_filepath)
        for f in files_to_remove:
            await conn.run(f"rm {f}")
