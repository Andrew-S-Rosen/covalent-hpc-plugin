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
import os
import sys
from pathlib import Path
from typing import Literal

import aiofiles
import asyncssh
import cloudpickle as pickle
import covalent as ct
from aiofiles import os as async_os
from covalent._results_manager.result import Result
from covalent._shared_files import logger
from covalent._shared_files.config import get_config
from covalent.executor.base import AsyncBaseExecutor

# TODO:
# - Raise an error if PSI/J not found on remote machine
# - Activate conda env on remote machine
# - Reintroduce checks from slurm-covalent-plugin

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
    "python_executable": "python",
    "inherit_environment": True,
    "environment": None,
    "resource_spec": None,
    "job_attributes": None,
    "pre_launch": None,
    "post_launch": None,
    "launcher": "single",
    # Covalent parameters
    "remote_workdir": "~/covalent-workdir",
    "create_unique_workdir": False,
    "cache_dir": str(Path(get_config("dispatcher.cache_dir")).expanduser().resolve()),
    "poll_freq": 60,
}
_DEFAULT = object()

executor_plugin_name = "HPCExecutor"


class HPCExecutor(AsyncBaseExecutor):
    """
    HPC executor plugin class, built around PSI/J. PSI/J must be present in the remote machine's Python environment.

    Args:
        address: Remote address or hostname of the login node.
        username: Username used to authenticate over SSH.
        ssh_key_file: Private RSA key used to authenticate over SSH (usually at ~/.ssh/id_rsa).
        cert_file: Certificate file used to authenticate over SSH, if required (usually has extension .pub).
        instance: PSI/J instance to use for job submission.
        python_executable: Python executable to use for job submission.
        environment: Environment variables to set for the job.
        pre_launch: Scripts to run before launching the job.
        post_launch: Scripts to run after launching the job.
        resource_spec: ResourceSpec for the job.
        job_attributes: JobAttributes for the job.
        launcher: Launcher to use for the job.
        remote_workdir: Working directory on the remote cluster.
        create_unique_workdir: Whether to create a unique working (sub)directory for each task.
        cache_dir: Local cache directory used by this executor for temporary files.
        poll_freq: Frequency with which to poll a submitted job. Always is >= 60.
        cleanup: Whether to perform cleanup or not on remote machine.
        time_limit: time limit for the task
        retries: Number of times to retry execution upon failure
    """

    def __init__(
        self,
        # SSH credentials
        address: str = _DEFAULT,
        username: str | None = _DEFAULT,
        ssh_key_file: str | None = _DEFAULT,
        cert_file: str | None = _DEFAULT,
        # PSI/J parameters
        instance: Literal["cobalt", "flux", "local", "lsf", "pbspro", "rp", "slurm"] = _DEFAULT,
        python_executable: str = _DEFAULT,
        inherit_environment: bool = _DEFAULT,
        environment: dict | None = _DEFAULT,
        pre_launch: str | None = _DEFAULT,
        post_launch: str | None = _DEFAULT,
        resource_spec: dict | None = _DEFAULT,
        job_attributes: dict | None = _DEFAULT,
        launcher: Literal["aprun", "jsrun", "mpirun", "multiple", "single", "srun"] = _DEFAULT,
        # Covalent parameters
        remote_workdir: str = _DEFAULT,
        create_unique_workdir: bool = _DEFAULT,
        cache_dir: str = _DEFAULT,
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

        if self.cert_file and not self.ssh_key_file:
            raise ValueError("ssh_key_file must be set if cert_file is set.")

        # PSI/J parameters
        self.instance = (
            instance
            if instance != _DEFAULT
            else hpc_config["instance"]
            if "instance" in hpc_config
            else _EXECUTOR_PLUGIN_DEFAULTS["instance"]
        )

        self.python_executable = (
            python_executable
            if python_executable != _DEFAULT
            else hpc_config["python_executable"]
            if "python_executable" in hpc_config
            else _EXECUTOR_PLUGIN_DEFAULTS["python_executable"]
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

        self.pre_launch = (
            pre_launch
            if pre_launch != _DEFAULT
            else hpc_config["pre_launch"]
            if "pre_launch" in hpc_config
            else _EXECUTOR_PLUGIN_DEFAULTS["pre_launch"]
        )

        self.post_launch = (
            post_launch
            if post_launch != _DEFAULT
            else hpc_config["post_launch"]
            if "post_launch" in hpc_config
            else _EXECUTOR_PLUGIN_DEFAULTS["post_launch"]
        )

        self.resource_spec = (
            resource_spec
            if resource_spec != _DEFAULT
            else hpc_config["resource_spec"]
            if "resource_spec" in hpc_config
            else _EXECUTOR_PLUGIN_DEFAULTS["resource_spec"]
        )

        self.job_attributes = (
            job_attributes
            if job_attributes != _DEFAULT
            else hpc_config["job_attributes"]
            if "job_attributes" in hpc_config
            else _EXECUTOR_PLUGIN_DEFAULTS["job_attributes"]
        )

        self.launcher = (
            launcher
            if launcher != _DEFAULT
            else hpc_config["launcher"]
            if "launcher" in hpc_config
            else _EXECUTOR_PLUGIN_DEFAULTS["launcher"]
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

        # Make sure local cache dir exists
        os.makedirs(self.cache_dir, exist_ok=True)

        # Initialize jobid as None
        self._jobid = None

    def _format_py_script(self) -> str:
        """Create the Python script that executes the pickled python function.

        Returns:
            script: String object containing a script.
        """

        return f"""
import cloudpickle as pickle

with open("{self._remote_func_filename}", "rb") as f:
    function, args, kwargs = pickle.load(f)

result = None
exception = None

try:
    result = function(*args, **kwargs)
except Exception as e:
    exception = e

with open("{self._remote_result_filename}", "wb") as f:
    pickle.dump((result, exception), f)
"""

    def _format_job_script(self) -> str:
        """Create the PSI/J Job that defines the job specs.

        Returns:
            string representation of the Python script to make/execute the PSI/J Job object.
        """
        resources_string = (
            f"resources=ResourceSpecV1(**{self.resource_spec})," if self.resource_spec else ""
        )
        attributes_string = (
            f"attributes=JobAttributes(**{self.job_attributes})," if self.job_attributes else ""
        )
        pre_launch_string = (
            f"pre_launch=Path('{self.pre_launch}').expanduser().resolve(),"
            if self.pre_launch
            else ""
        )
        post_launch_string = (
            f"post_launch=Path('{self.post_launch}').expanduser().resolve(),"
            if self.post_launch
            else ""
        )

        return f"""
from pathlib import Path
from psij import Job, JobAttributes, JobExecutor, JobSpec, ResourceSpecV1

job_executor = JobExecutor.get_instance("{self.instance}")

job = Job(
    JobSpec(
        name="{self._name}",
        executable="{self.python_executable}",
        arguments=[str(Path("{self._remote_py_script_filename}"))],
        directory=Path("{self._job_remote_workdir}"),
        environment={self.environment},
        stdout_path=Path("{self._remote_stdout_path}"),
        stderr_path=Path("{self._remote_stderr_path}"),
        {resources_string}
        {attributes_string}
        {pre_launch_string}
        {post_launch_string}
        launcher="{self.launcher}",
    )
)

job_executor.submit(job)
print(job.native_id)
"""

    def _format_query_status_script(self) -> str:
        """
        Create the PSI/J script to query job status.

        Returns:
            string representation of the Python script to make/execute the PSI/J Job object.
        """

        return f"""
from datetime import timedelta
from psij import Job, JobExecutor, JobState

job_executor = JobExecutor.get_instance("{self.instance}")

job = Job()
job_executor.attach(job, "{self._jobid}")
state = job.wait(
    target_states=[
        JobState.QUEUED,
        JobState.CANCELED,
        JobState.ACTIVE,
        JobState.FAILED,
        JobState.COMPLETED,
    ],
    timeout = timedelta(seconds=10),
)
state = state or JobState.NEW
print(state.name)
"""

    async def _client_connect(self) -> asyncssh.SSHClientConnection:
        """
        Helper function for connecting to the remote host through asyncssh module.

        Args:
            None

        Returns:
            The connection object
        """

        if not self.address:
            raise ValueError("address is a required parameter.")

        if self.cert_file:
            self.cert_file = Path(self.cert_file).expanduser().resolve()
            client_keys = (
                asyncssh.read_private_key(self.ssh_key_file),
                asyncssh.read_certificate(self.cert_file),
            )
        elif self.ssh_key_file:
            self.ssh_key_file = Path(self.ssh_key_file).expanduser().resolve()
            client_keys = asyncssh.read_private_key(self.ssh_key_file)

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
        """Run a function on the remote machine.

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
            (
                Path(self.remote_workdir) / dispatch_id / f"node_{node_id}"
                if self.create_unique_workdir
                else Path(self.remote_workdir)
            )
            .expanduser()
            .resolve()
        )

        result_filename = f"result-{dispatch_id}-{node_id}.pkl"
        job_script_filename = f"psij-{dispatch_id}-{node_id}.py"
        py_script_filename = f"script-{dispatch_id}-{node_id}.py"
        query_status_script_filename = f"query-status-{dispatch_id}-{node_id}.py"
        func_filename = f"func-{dispatch_id}-{node_id}.pkl"
        stdout_filename = f"stdout-{dispatch_id}-{node_id}.log"
        stderr_filename = f"stderr-{dispatch_id}-{node_id}.log"

        self._remote_result_filename = self._job_remote_workdir / result_filename
        self._remote_job_script_filename = self._job_remote_workdir / job_script_filename
        self._remote_py_script_filename = self._job_remote_workdir / py_script_filename
        self._remote_query_status_filename = (
            self._job_remote_workdir / query_status_script_filename
        )
        self._remote_func_filename = self._job_remote_workdir / func_filename
        self._remote_stdout_path = self._job_remote_workdir / stdout_filename
        self._remote_stderr_path = self._job_remote_workdir / stderr_filename

        # Establish connection
        conn = await self._client_connect()

        py_version_func = ".".join(function.args[0].python_version.split(".")[:2])
        app_log.debug(f"Python version: {py_version_func}")

        # Create the remote directory
        app_log.debug(f"Creating remote work directory {self._job_remote_workdir} ...")
        cmd_mkdir_remote = f"mkdir -p {self._job_remote_workdir}"
        proc_mkdir_remote = await conn.run(cmd_mkdir_remote)

        if client_err := proc_mkdir_remote.stderr.strip():
            raise RuntimeError(client_err)

        async with aiofiles.tempfile.NamedTemporaryFile(dir=self.cache_dir) as temp:
            # Pickle the function, write to file, and copy to remote filesystem
            app_log.debug("Writing pickled function, args, kwargs to file...")
            await temp.write(pickle.dumps((function, args, kwargs)))
            await temp.flush()

            app_log.debug(
                f"Copying pickled function to remote fs: {self._remote_func_filename} ..."
            )
            await asyncssh.scp(temp.name, (conn, self._remote_func_filename))

        async with aiofiles.tempfile.NamedTemporaryFile(dir=self.cache_dir, mode="w") as temp:
            # Format the function execution script, write to file, and copy to remote filesystem
            python_exec_script = self._format_py_script()
            app_log.debug("Writing python run-function script to tempfile...")
            await temp.write(python_exec_script)
            await temp.flush()

            app_log.debug(
                f"Copying python run-function to remote fs: {self._remote_py_script_filename}"
            )
            await asyncssh.scp(temp.name, (conn, self._remote_py_script_filename))

        async with aiofiles.tempfile.NamedTemporaryFile(dir=self.cache_dir, mode="w") as temp:
            # Format the submit script, write to file, and copy to remote filesystem
            submit_script = self._format_job_script()
            app_log.debug("Writing submit script to tempfile...")
            await temp.write(submit_script)
            await temp.flush()

            app_log.debug(
                f"Copying submit script to remote fs: {self._remote_job_script_filename} ..."
            )
            await asyncssh.scp(temp.name, (conn, self._remote_job_script_filename))

        # Execute the job submission script
        app_log.debug(f"Submitting the job...")
        proc = await conn.run(f"{self.python_executable} {self._remote_job_script_filename}")

        if proc.returncode != 0:
            raise RuntimeError(proc.stderr.strip())

        app_log.debug(f"Job submitted with stdout: {self._remote_stdout_path}")

        self._jobid = proc.stdout.strip()

        async with aiofiles.tempfile.NamedTemporaryFile(dir=self.cache_dir, mode="w") as temp:
            # Format the function execution script, write to file, and copy to remote filesystem
            python_query_status_script = self._format_query_status_script()
            app_log.debug("Writing python query status script to tempfile...")
            await temp.write(python_query_status_script)
            await temp.flush()

            app_log.debug(
                f"Copying python query-function to remote fs: {self._remote_query_status_filename}"
            )
            await asyncssh.scp(temp.name, (conn, self._remote_query_status_filename))

        app_log.debug(f"Polling job scheduler with job_id: {self._jobid} ...")
        await self._poll_scheduler(conn)

        app_log.debug(f"Querying result with job_id: {self._jobid} ...")
        result, stdout, stderr, exception = await self._query_result(conn)

        print(stdout)
        print(stderr, file=sys.stderr)

        if exception:
            raise RuntimeError(exception)

        app_log.debug("Preparing for teardown...")

        app_log.debug("Closing SSH connection...")
        conn.close()
        await conn.wait_closed()
        app_log.debug("SSH connection closed, returning result")

        return result

    async def get_status(self, conn: asyncssh.SSHClientConnection) -> Result | str:
        """Query the status of a job previously submitted to the job scheduler.

        Args:
            conn: SSH connection object.

        Returns:
            status: String describing the job status.
        """

        if self._jobid is None:
            return Result.NEW_OBJ

        proc = await conn.run(f"{self.python_executable} {self._remote_query_status_filename}")

        if proc.returncode != 0:
            raise RuntimeError(proc.stderr.strip())

        return proc.stdout.strip()

    async def _poll_scheduler(self, conn: asyncssh.SSHClientConnection) -> None:
        """Poll a for the job until completion.

        Args:
            conn: SSH connection object.

        Returns:
            None
        """

        # Poll status every `poll_freq` seconds
        status = await self.get_status(conn)

        while status in {"QUEUED", "ACTIVE"}:
            await asyncio.sleep(self.poll_freq)
            status = await self.get_status(conn)

        if status == "FAILED":
            raise RuntimeError(f"Job failed.")
        elif status == "CANCELED":
            raise RuntimeError(f"Job cancelled.")

    async def _query_result(
        self,
        conn: asyncssh.SSHClientConnection,
    ):
        """Query and retrieve the task result including stdout and stderr logs.

        Args:
            conn: SSH connection object.

        Returns:
            result: Task result.
            stdout: stdout log.
            stderr: stderr log.
            exception: Exception raised during task execution.
        """

        # Check the result file exists on the remote backend
        proc = await conn.run(f"test -e {self._remote_result_filename}")

        if proc.returncode != 0:
            raise FileNotFoundError(
                proc.returncode, proc.stderr.strip(), self._remote_result_filename
            )

        # Copy result file from remote machine to Covalent server
        local_result_filename = self._task_results_dir / self._remote_result_filename.name
        await asyncssh.scp((conn, self._remote_result_filename), local_result_filename)

        # Copy stdout, stderr from remote machine to Covalent server
        local_stdout_file = self._task_results_dir / self._remote_stdout_path.name
        local_stderr_file = self._task_results_dir / self._remote_stderr_path.name

        await asyncssh.scp((conn, self._remote_stdout_path), local_stdout_file)
        await asyncssh.scp((conn, self._remote_stderr_path), local_stderr_file)

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
        """Perform cleanup on remote machine.

        Args:
            task_metadata: Dictionary of metadata associated with the task.
            Even though it's not used here, it is always passed by Covalent
            and can't be removed.

        Returns:
            None
        """
        try:
            app_log.debug("Performing cleanup on remote...")
            conn = await self._client_connect()
            await self._perform_cleanup(conn)

            app_log.debug("Closing SSH connection...")
            conn.close()
            await conn.wait_closed()
            app_log.debug("SSH connection closed, teardown complete")
        except Exception:
            app_log.warning("Cleanup could not successfully complete. Nonfatal error.")

    async def _perform_cleanup(self, conn: asyncssh.SSHClientConnection) -> None:
        """
        Function to perform cleanup on remote machine

        Args:
            conn: SSH connection object

        Returns:
            None
        """
        files_to_remove = (
            [
                self._remote_func_filename,
                self._remote_job_script_filename,
                self._remote_py_script_filename,
                self._remote_query_status_filename,
                self._remote_result_filename,
                self._remote_stdout_path,
                self._remote_stderr_path,
            ],
        )
        for f in files_to_remove:
            await conn.run(f"rm {f}")
