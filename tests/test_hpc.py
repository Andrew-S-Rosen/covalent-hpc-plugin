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

"""Tests for the HPC executor plugin."""

import asyncio
import os
import subprocess
from copy import deepcopy
from datetime import timedelta
from functools import partial
from pathlib import Path
from unittest import mock

import asyncssh
import cloudpickle as pickle
import pytest
from covalent._results_manager.result import Result
from covalent._shared_files.config import get_config, set_config
from covalent._workflow.transport import TransportableObject
from covalent.executor.base import wrapper_fn

from covalent_hpc_plugin import HPCExecutor


@pytest.fixture
def proc_mock():
    return mock.Mock()


@pytest.fixture
def conn_mock():
    return mock.Mock()


def mock_key_read(*args, **kwargs):
    """Mock for asyncssh.read_private_key() and asyncssh.read_certificate()"""
    return True


def mock_basic_async(*args, **kwargs):
    """A basic async mock"""
    future = asyncio.Future()
    future.set_result(True)
    return future


def mock_sleep(*args, **kwargs):
    future = asyncio.Future()
    future.set_exception(ValueError("No sleep!"))
    return future


def mock_fetch_result(
    result: any = "result", stdout: str = "", stderr: str = "", exception: Exception = None
):
    """Mock for HPCExecutor._fetch_result()"""

    def mock_func(*args, **kwargs):
        future = asyncio.Future()
        future.set_result((result, stdout, stderr, exception))
        return future

    return mock_func


def mock_asyncssh_run(returncode: int = 0, stdout: str = "", stderr: str = ""):
    """Mock for asyncssh.SSHClientConnection.run()"""

    def mock_func(*args, **kwargs):
        class MockStdout:
            def __init__(self):
                self.returncode = returncode
                self.stdout = stdout
                self.stderr = stderr

        future = asyncio.Future()
        future.set_result(MockStdout())
        return future

    return mock_func


def test_init_defaults(tmpdir):
    """Test that initialization properly sets member variables."""
    tmpdir.chdir()

    # Test with defaults
    address = "host"
    executor = HPCExecutor(address=address)
    assert executor.address == address
    assert executor.username == ""
    assert executor.ssh_key_file == "~/.ssh/id_rsa"
    assert executor.cert_file is None
    assert executor.instance == "slurm"
    assert executor.inherit_environment == True
    assert executor.environment == {}
    assert executor.resource_spec_kwargs == {
        "node_count": 1,
        "exclusive_node_use": False,
        "process_count": 1,
        "processes_per_node": 1,
        "cpu_cores_per_process": 1,
        "gpu_cores_per_process": 0,
    }
    assert executor.job_attributes_kwargs == {"duration": timedelta(minutes=10)}
    assert executor.launcher == "single"
    assert executor.remote_workdir == "~/covalent-workdir"
    assert executor.create_unique_workdir == False
    assert executor.cache_dir == str(
        Path(get_config("dispatcher.cache_dir")).expanduser().resolve()
    )
    assert executor.poll_freq == 60


def test_init_nondefaults(tmpdir):
    """Test that initialization properly sets member variables."""

    tmpdir.chdir()

    # Test with non-defaults
    address = "host"
    username = "username"
    executor = HPCExecutor(
        address=address,
        username=username,
        ssh_key_file="ssh_key_file",
        cert_file="cert_file",
        instance="flux",
        inherit_environment=False,
        environment={"hello": "world"},
        resource_spec_kwargs={"node_count": 2},
        job_attributes_kwargs={"duration": 20},
        launcher="multiple",
        remote_python_exe="python2",
        remote_conda_env="myenv",
        remote_workdir="~/my-remote-dir",
        create_unique_workdir=True,
        cache_dir=tmpdir / "my-cache-dir",
        poll_freq=90,
    )
    assert executor.username == username
    assert executor.address == address
    assert executor.ssh_key_file == "ssh_key_file"
    assert executor.cert_file == "cert_file"
    assert executor.instance == "flux"
    assert executor.inherit_environment == False
    assert executor.environment == {"hello": "world"}
    assert executor.resource_spec_kwargs == {"node_count": 2}
    assert executor.job_attributes_kwargs == {"duration": timedelta(minutes=20)}
    assert executor.launcher == "multiple"
    assert executor.remote_python_exe == "python2"
    assert executor.remote_conda_env == "myenv"
    assert executor.remote_workdir == "~/my-remote-dir"
    assert executor.create_unique_workdir == True
    assert executor.cache_dir == tmpdir / "my-cache-dir"
    assert executor.poll_freq == 90
    assert os.path.exists(tmpdir / "my-cache-dir")


def test_init_defaults(tmpdir):
    """Test that initialization properly sets member variables."""
    tmpdir.chdir()

    # Test poll freq is auto-raised
    executor = HPCExecutor(address="host", poll_freq=10)
    assert executor.poll_freq == 30


def test_removed_inits(tmpdir):
    """Test for removed inits"""

    tmpdir.chdir()

    start_config = deepcopy(get_config())
    for key in ["cert_file", "remote_conda_env"]:
        config = get_config()
        config["executors"]["hpc"].pop(key, None)
        set_config(config)
        executor = HPCExecutor(address="host")
        assert not executor.__dict__[key]
        set_config(start_config)


def test_format_pickle_script(tmpdir):
    """Test that the python script (in string form) for the pickle function is as-expected"""

    tmpdir.chdir()

    executor = HPCExecutor(
        username="test_user",
        address="test_address",
        ssh_key_file="ssh_key_file",
        cert_file="cert_file",
        remote_workdir="/federation/test_user/.cache/covalent",
        cache_dir="~/.cache/covalent",
    )

    dispatch_id = "148dedae-1b58-3870-z08d-db89bceec915"
    task_id = 2
    func_filename = f"func-{dispatch_id}-{task_id}.pkl"
    result_filename = f"result-{dispatch_id}-{task_id}.pkl"
    executor._remote_func_filepath = func_filename
    executor._remote_result_filepath = result_filename

    py_script_str = executor._format_pickle_script()
    assert func_filename in py_script_str
    assert result_filename in py_script_str


def test_pickle_script(tmpdir):
    """Test Python pickle script works appropriately"""

    tmpdir.chdir()

    def test_func(a, b="default"):
        return f"{a} {b}"

    executor = HPCExecutor(username="test_user", address="test_address")
    dispatch_id = "148dedae-1b58-3870-z08d-db89bceec915"
    task_id = 2
    func_filename = tmpdir / f"func-{dispatch_id}-{task_id}.pkl"
    result_filename = tmpdir / f"result-{dispatch_id}-{task_id}.pkl"
    executor._remote_func_filepath = func_filename
    pickle.dump([test_func, {"hello"}, {"b": "world"}], open(func_filename, "wb"))
    executor._remote_result_filepath = result_filename

    py_script_str = executor._format_pickle_script()
    with open("test.py", "w") as w:
        w.write(py_script_str)
    p = subprocess.run(
        "python test.py", shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )
    assert p.returncode == 0
    assert p.stderr == b""
    assert os.path.exists(result_filename)
    pickle_load = pickle.load(open(result_filename, "rb"))
    assert pickle_load[0] == "hello world"
    assert pickle_load[1] is None


def test_format_submit_script(tmpdir):
    """Test that the shell script (in string form) which is to be submitted on
    the remote server is created with no errors."""

    tmpdir.chdir()

    remote_workdir = "/federation/test_user/.cache/covalent"
    executor = HPCExecutor(
        username="test_user",
        address="test_address",
        ssh_key_file="~/.ssh/id_rsa",
        remote_workdir=remote_workdir,
        remote_python_exe="python3",
        environment={"hello": "world"},
        launcher="srun",
        resource_spec_kwargs={"node_count": 10},
        job_attributes_kwargs={"duration": timedelta(minutes=20)},
        remote_conda_env="myenv",
    )

    dispatch_id = "259efebf-2c69-4981-a19e-ec90cdffd026"
    task_id = 3
    executor._name = f"{dispatch_id}-{task_id}"
    executor._remote_pickle_script_filepath = f"script-{dispatch_id}-{task_id}.py"
    executor._job_remote_workdir = tmpdir
    executor._remote_stdout_filepath = f"stdout-{dispatch_id}-{task_id}.log"
    executor._remote_stderr_filepath = f"stderr-{dispatch_id}-{task_id}.log"
    executor._remote_pre_launch_filepath = f"pre-launch-{dispatch_id}-{task_id}.sh"

    submit_script_str = executor._format_job_script()

    assert "JobSpec" in submit_script_str
    assert f'name="{executor._name}"' in submit_script_str
    assert f'executable="python3"' in submit_script_str
    assert (
        "directory=" in submit_script_str
        and str(executor._job_remote_workdir) in submit_script_str
    )
    assert "environment={'hello': 'world'}" in submit_script_str
    assert (
        "stdout_path=" in submit_script_str
        and executor._remote_stdout_filepath in submit_script_str
    )
    assert (
        "stderr_path=" in submit_script_str
        and executor._remote_stderr_filepath in submit_script_str
    )
    assert 'launcher="srun"' in submit_script_str
    assert "resources=ResourceSpecV1(**{'node_count': 10})" in submit_script_str
    assert (
        "attributes=JobAttributes(**{'duration': datetime.timedelta(seconds=1200)})"
        in submit_script_str
    )
    assert (
        "pre_launch=" in submit_script_str
        and executor._remote_pre_launch_filepath in submit_script_str
    )


def test_format_submit_script_minimal(tmpdir):
    """Test that the shell script (in string form) which is to be submitted on
    the remote server is created with no errors."""

    tmpdir.chdir()
    executor = HPCExecutor(
        username="test_user",
        address="test_address",
        resource_spec_kwargs={},
        job_attributes_kwargs={},
    )

    dispatch_id = "259efebf-2c69-4981-a19e-ec90cdffd026"
    task_id = 3
    executor._name = f"{dispatch_id}-{task_id}"
    executor._remote_pickle_script_filepath = f"script-{dispatch_id}-{task_id}.py"
    executor._job_remote_workdir = tmpdir
    executor._remote_stdout_filepath = f"stdout-{dispatch_id}-{task_id}.log"
    executor._remote_stderr_filepath = f"stderr-{dispatch_id}-{task_id}.log"
    executor._remote_pre_launch_filepath = f"pre-launch-{dispatch_id}-{task_id}.sh"

    submit_script_str = executor._format_job_script()
    assert "resources" not in submit_script_str
    assert "attributes" not in submit_script_str
    assert "pre_launch" in submit_script_str


def test_submit_script(tmpdir):
    """Test that the submit script actually works as intended"""
    tmpdir.chdir()

    executor = HPCExecutor(
        username="test_user",
        address="test_address",
        instance="local",
    )
    dispatch_id = "259efebf-2c69-4981-a19e-ec90cdffd026"
    task_id = 3
    executor._name = f"{dispatch_id}-{task_id}"
    executor._remote_pickle_script_filepath = f""
    executor._job_remote_workdir = tmpdir
    executor._remote_stdout_filepath = f"stdout-{dispatch_id}-{task_id}.log"
    executor._remote_stderr_filepath = f"stderr-{dispatch_id}-{task_id}.log"
    executor._remote_pre_launch_filepath = f"pre-launch-{dispatch_id}-{task_id}.sh"

    submit_str = executor._format_job_script()
    assert f'JobExecutor.get_instance("local")' in submit_str

    with open("test_submit.py", "w") as w:
        w.write(submit_str)
    p = subprocess.run(
        "python test_submit.py", shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )
    assert p.returncode == 0
    assert p.stderr == b""
    assert p.stdout != b""
    assert int(p.stdout) > 0


def test_format_query_script():
    """Test that the Python script to perform job queries is formatted correctly"""
    executor = HPCExecutor(
        username="test_user",
        address="test_address",
        instance="flux",
    )

    executor._jobid = "123456"
    query_str = executor._format_query_status_script()
    assert f'JobExecutor.get_instance("flux")' in query_str
    assert f'job_executor.attach(job, f"{executor._jobid}")' in query_str


def test_query_script(tmpdir):
    """Test that the Python script to perform job queries works as expected"""
    tmpdir.chdir()

    executor = HPCExecutor(
        username="test_user",
        address="test_address",
        instance="local",
    )
    dispatch_id = "259efebf-2c69-4981-a19e-ec90cdffd026"
    task_id = 3
    executor._name = f"{dispatch_id}-{task_id}"
    executor._remote_pickle_script_filepath = f""
    executor._job_remote_workdir = tmpdir
    executor._remote_stdout_filepath = f"stdout-{dispatch_id}-{task_id}.log"
    executor._remote_stderr_filepath = f"stderr-{dispatch_id}-{task_id}.log"
    executor._remote_pre_launch_filepath = f"pre-launch-{dispatch_id}-{task_id}.sh"

    submit_str = executor._format_job_script()
    assert f'JobExecutor.get_instance("local")' in submit_str

    executor._jobid = "{native_id}"
    query_str = executor._format_query_status_script()
    with open("test_query.py", "w") as w:
        w.write(submit_str + query_str)
    p = subprocess.run(
        "python test_query.py", shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )
    assert p.returncode == 0
    assert p.stderr == b""
    assert "ACTIVE" in p.stdout.decode()


def test_format_pre_launch_script(tmpdir):
    """Test that the prelaunch script is formatted appropriately"""
    tmpdir.chdir()

    executor = HPCExecutor(
        username="test_user",
        address="test_address",
        instance="flux",
    )

    executor._remote_python_version = "3.8.5"

    pre_launch_str = executor._format_pre_launch_script()
    assert "3.8.5" in pre_launch_str
    assert "conda activate" not in pre_launch_str

    executor = HPCExecutor(
        username="test_user", address="test_address", instance="flux", remote_conda_env="myenv"
    )
    executor._remote_python_version = "3.8.5"

    pre_launch_str = executor._format_pre_launch_script()
    assert "3.8.5" in pre_launch_str
    assert "conda activate myenv" in pre_launch_str

    executor = HPCExecutor(
        username="test_user",
        address="test_address",
        instance="flux",
        remote_conda_env="myenv",
        pre_launch_cmds=["echo hello", "echo world"],
    )
    executor._remote_python_version = "3.8.5"

    pre_launch_str = executor._format_pre_launch_script()
    assert "3.8.5" in pre_launch_str
    assert "conda activate myenv" in pre_launch_str
    assert "echo hello\necho world\n" in pre_launch_str


def test_format_post_launch_script(tmpdir):
    """Test that the postlaunch script is formatted appropriately"""
    tmpdir.chdir()
    executor = HPCExecutor(
        username="test_user",
        address="test_address",
        instance="flux",
        post_launch_cmds=["echo hello", "echo world"],
    )
    post_launch_str = executor._format_post_launch_script()
    assert post_launch_str == "echo hello\necho world\n"


@pytest.mark.asyncio
async def test_client_connect_failure(tmpdir, monkeypatch):
    """Test for _client_connect without mocking .connect()"""
    tmpdir.chdir()
    monkeypatch.setattr("asyncssh.read_private_key", mock_key_read)
    monkeypatch.setattr("asyncssh.read_certificate", mock_key_read)

    with pytest.raises(RuntimeError):
        await HPCExecutor(
            address="test_address", username="test_use", ssh_key_file="ssh_key_file"
        )._client_connect()


@pytest.mark.asyncio
async def test_client_connect(tmpdir, monkeypatch):
    """Test for _client_connect with mocking .connect()"""
    tmpdir.chdir()
    monkeypatch.setattr("asyncssh.read_private_key", mock_key_read)
    monkeypatch.setattr("asyncssh.read_certificate", mock_key_read)
    monkeypatch.setattr("asyncssh.connect", mock_basic_async)

    assert (
        await HPCExecutor(
            address="test_address", username="test_use", ssh_key_file="ssh_key_file"
        )._client_connect()
        == True
    )

    assert (
        await HPCExecutor(
            address="test_address",
            username="test_use",
            ssh_key_file="ssh_key_file",
            cert_file="cert_file",
        )._client_connect()
        == True
    )

    assert (
        await HPCExecutor(
            address="test_address", username="test_use", ssh_key_file=None, cert_file=None
        )._client_connect()
        == True
    )

    with pytest.raises(ValueError, match="address is a required parameter"):
        await HPCExecutor(ssh_key_file="ssh_key_file")._client_connect()

    with pytest.raises(ValueError, match="ssh_key_file is required if cert_file is provided."):
        await HPCExecutor(
            address="test_address", ssh_key_file=None, cert_file="cert_file"
        )._client_connect()


@pytest.mark.asyncio
async def test_get_status_success(tmpdir, monkeypatch):
    """Test the get_status method."""
    tmpdir.chdir()
    monkeypatch.setattr(
        "asyncssh.SSHClientConnection.run", mock_asyncssh_run(0, "Fake Status", "")
    )

    executor = HPCExecutor(
        username="test_user",
        address="test_address",
        ssh_key_file="ssh_key_file",
        remote_workdir="/federation/test_user/.cache/covalent",
    )

    # Test when jobid is not set
    status = await executor.get_status(asyncssh.SSHClientConnection)
    assert status == Result.NEW_OBJ

    # Test when jobid is set
    executor._jobid = "123456"
    executor._remote_query_script_filepath = "mock.py"
    status = await executor.get_status(asyncssh.SSHClientConnection)
    assert status == "Fake Status"


@pytest.mark.asyncio
async def test_get_status_failures(tmpdir, monkeypatch):
    """Test the get_status method for failures."""
    tmpdir.chdir()
    monkeypatch.setattr("asyncssh.SSHClientConnection.run", mock_asyncssh_run(1, "", "AN ERROR"))

    executor = HPCExecutor(
        username="test_user",
        address="test_address",
        ssh_key_file="ssh_key_file",
        remote_workdir="/federation/test_user/.cache/covalent",
    )
    executor._jobid = "123456"
    executor._remote_query_script_filepath = "mock.py"

    with pytest.raises(RuntimeError, match="Getting job status failed: AN ERROR"):
        await executor.get_status(asyncssh.SSHClientConnection)


@pytest.mark.asyncio
async def test_poll_scheduler_completed(tmpdir, monkeypatch):
    """Test that polling the status works."""
    tmpdir.chdir()
    monkeypatch.setattr("asyncssh.SSHClientConnection.run", mock_asyncssh_run(0, "COMPLETED", ""))

    executor = HPCExecutor(
        username="test_user",
        address="test_address",
        ssh_key_file="ssh_key_file",
        remote_workdir="/federation/test_user/.cache/covalent",
    )

    # Check completed status does not give any errors
    executor._jobid = "123456"
    executor._remote_query_script_filepath = "mock.py"
    assert await executor._poll_scheduler(asyncssh.SSHClientConnection) is None


@pytest.mark.asyncio
async def test_poll_scheduler_canceled(tmpdir, monkeypatch):
    """Test that polling the status works."""
    tmpdir.chdir()
    monkeypatch.setattr("asyncssh.SSHClientConnection.run", mock_asyncssh_run(0, "CANCELED", ""))

    # Check canceled status reported
    executor = HPCExecutor(
        username="test_user",
        address="test_address",
        ssh_key_file="ssh_key_file",
        remote_workdir="/federation/test_user/.cache/covalent",
    )
    executor._jobid = "12345"
    executor._remote_query_script_filepath = "mock.py"

    with pytest.raises(RuntimeError, match="Status for job with native ID 12345: CANCELED."):
        await executor._poll_scheduler(asyncssh.SSHClientConnection)


@pytest.mark.asyncio
async def test_poll_scheduler_loop(tmpdir, monkeypatch):
    """Test that queued state results in continued polling."""
    tmpdir.chdir()
    monkeypatch.setattr("asyncssh.SSHClientConnection.run", mock_asyncssh_run(0, "QUEUED", ""))
    monkeypatch.setattr("asyncio.sleep", mock_sleep)

    # Check queued status works
    executor = HPCExecutor(
        username="test_user",
        address="test_address",
        ssh_key_file="ssh_key_file",
        remote_workdir="/federation/test_user/.cache/covalent",
        poll_freq=1,
    )
    executor._jobid = "12345"
    executor._remote_query_script_filepath = "mock.py"

    with pytest.raises(ValueError, match="No sleep!"):
        await executor._poll_scheduler(asyncssh.SSHClientConnection)

    monkeypatch.setattr("asyncssh.SSHClientConnection.run", mock_asyncssh_run(0, "ACTIVE", ""))
    with pytest.raises(ValueError, match="No sleep!"):
        await executor._poll_scheduler(asyncssh.SSHClientConnection)


@pytest.mark.asyncio
async def test_fetch_result(tmpdir, monkeypatch):
    """Test querying results works as expected."""
    tmpdir.chdir()
    monkeypatch.setattr(
        "asyncssh.SSHClientConnection.run", mock_asyncssh_run(1, "stdout", "stderr")
    )

    executor = HPCExecutor(
        username="test_user",
        address="test_address",
        ssh_key_file="ssh_key_file",
        remote_workdir="/federation/test_user/.cache/covalent",
    )

    executor._remote_result_filepath = Path("/path/to/file")

    with pytest.raises(FileNotFoundError, match="stderr"):
        await executor._fetch_result(asyncssh.SSHClientConnection)


@pytest.mark.asyncio
async def test_fetch_result_v2(monkeypatch, tmpdir):
    """Test querying results works as expected."""
    tmpdir.chdir()
    monkeypatch.setattr("asyncssh.SSHClientConnection.run", mock_asyncssh_run(0, "", ""))
    monkeypatch.setattr("asyncssh.scp", mock_basic_async)

    executor = HPCExecutor(
        username="test_user",
        address="test_address",
        ssh_key_file="ssh_key_file",
        remote_workdir="/federation/test_user/.cache/covalent",
    )
    executor._remote_result_filepath = Path("/path/to/test.txt")
    executor._task_results_dir = tmpdir
    executor._remote_stdout_filepath = Path(os.path.join(tmpdir, "stdout.txt"))
    executor._remote_stderr_filepath = Path(os.path.join(tmpdir, "stderr.txt"))
    results_file = Path(os.path.join(tmpdir, "test.txt"))
    pickle.dump(["result", "exception"], open("test.pkl", "wb"))

    with open(results_file, "wb") as f:
        with open("test.pkl", "rb") as f2:
            f.write(f2.read())
    with open(executor._remote_stdout_filepath, "w") as f:
        f.write("stdout")
    with open(executor._remote_stderr_filepath, "w") as f:
        f.write("stderr")

    outputs = await executor._fetch_result(conn=asyncssh.SSHClientConnection)
    assert outputs[0] == "result"
    assert outputs[1] == "stdout"
    assert outputs[2] == "stderr"
    assert outputs[3] == "exception"


@pytest.mark.asyncio
async def test_run(tmpdir, monkeypatch, proc_mock, conn_mock):
    """Test calling run works as expected."""
    tmpdir.chdir()
    monkeypatch.setattr("asyncssh.scp", mock_basic_async)
    monkeypatch.setattr("covalent_hpc_plugin.HPCExecutor._fetch_result", mock_fetch_result())

    executor = HPCExecutor(
        username="test_user",
        address="test_address",
        ssh_key_file="ssh_key_file",
        remote_workdir="/scratch/user/experiment1",
        create_unique_workdir=True,
        remote_conda_env="my-conda-env",
        pre_launch_cmds=["echo hello", "echo world"],
        post_launch_cmds=["echo goodbye", "echo world"],
    )

    # dummy objects
    def f(x, y):
        return x + y

    dummy_function = partial(wrapper_fn, TransportableObject(f), call_before=[], call_after=[])

    dummy_metadata = {
        "dispatch_id": "259efebf-2c69-4981-a19e-ec90cdffd026",
        "node_id": 1,
        "results_dir": "results/directory/on/remote",
    }

    dummy_args = (
        dummy_function,
        [TransportableObject(2)],
        {"y": TransportableObject(3)},
        dummy_metadata,
    )

    # mock behavior
    conn_mock.run = mock.AsyncMock(return_value=proc_mock)
    conn_mock.wait_closed = mock.AsyncMock(return_value=None)

    async def __client_connect_succeed(*_):
        return conn_mock

    # patches
    patch_ccs = mock.patch.object(HPCExecutor, "_client_connect", new=__client_connect_succeed)

    # check run/teardown method works as expected
    proc_mock.stdout = "COMPLETED"
    proc_mock.stderr = ""
    proc_mock.returncode = 0
    with patch_ccs:
        await executor.run(*dummy_args)
        await executor.teardown(dummy_metadata)

    # Test errors
    proc_mock.stderr = "FAILED"
    with patch_ccs:
        with pytest.raises(RuntimeError, match="Making remote directory failed: FAILED"):
            await executor.run(*dummy_args)

    msg = "Failed to create directory"
    proc_mock.stderr = msg
    with patch_ccs:
        with pytest.raises(RuntimeError, match=msg):
            await executor.run(*dummy_args)

    # Test job fetching error
    proc_mock.stdout = "COMPLETED"
    proc_mock.stderr = ""
    proc_mock.returncode = 0
    monkeypatch.setattr(
        "covalent_hpc_plugin.HPCExecutor._fetch_result",
        mock_fetch_result("result", "", "error", RuntimeError),
    )
    with pytest.raises(RuntimeError, match="Fetching job result failed: error"):
        with patch_ccs:
            await executor.run(*dummy_args)
    monkeypatch.setattr("covalent_hpc_plugin.HPCExecutor._fetch_result", mock_fetch_result())
