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

"""Tests for the SLURM executor plugin."""

import os
import subprocess
from copy import deepcopy
from datetime import timedelta
from functools import partial
from pathlib import Path
from unittest import mock

import aiofiles
import cloudpickle as pickle
import pytest
from covalent._results_manager.result import Result
from covalent._shared_files.config import get_config, set_config
from covalent._workflow.transport import TransportableObject
from covalent.executor.base import wrapper_fn

from covalent_hpc_plugin import HPCExecutor

aiofiles.threadpool.wrap.register(mock.MagicMock)(
    lambda *args, **kwargs: aiofiles.threadpool.AsyncBufferedIOBase(*args, **kwargs)
)

FILE_DIR = Path(__file__).resolve().parent
SSH_KEY_FILE = os.path.join(FILE_DIR, "id_rsa")
CERT_FILE = os.path.join(FILE_DIR, "id_rsa.pub")


@pytest.fixture
def proc_mock():
    return mock.Mock()


@pytest.fixture
def conn_mock():
    return mock.Mock()


def setup_module():
    """Setup the module."""
    for f in [SSH_KEY_FILE, CERT_FILE]:
        with open(f, "w") as f:
            f.write("test_file")


def teardown_module():
    """Teardown the module."""
    for f in [SSH_KEY_FILE, CERT_FILE]:
        if os.path.exists(f):
            os.remove(f)


def test_init(tmpdir):
    tmpdir.chdir()
    """Test that initialization properly sets member variables."""

    # Test with defaults
    address = "host"
    executor = HPCExecutor(address=address)
    assert executor.address == address
    assert executor.username == ""
    assert executor.ssh_key_file == "~/.ssh/id_rsa"
    assert executor.cert_file == None
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

    # Test with non-defaults
    address = "host"
    username = "username"
    executor = HPCExecutor(
        address=address,
        username=username,
        ssh_key_file=SSH_KEY_FILE,
        cert_file=CERT_FILE,
        instance="flux",
        inherit_environment=False,
        environment={"hello": "world"},
        resource_spec_kwargs={"node_count": 2},
        job_attributes_kwargs={"duration": 20},
        launcher="multiple",
        remote_python_executable="python2",
        remote_conda_env="myenv",
        remote_workdir="~/my-remote-dir",
        create_unique_workdir=True,
        cache_dir=tmpdir / "my-cache-dir",
        poll_freq=90,
    )
    assert executor.username == username
    assert executor.address == address
    assert executor.ssh_key_file == SSH_KEY_FILE
    assert executor.cert_file == CERT_FILE
    assert executor.instance == "flux"
    assert executor.inherit_environment == False
    assert executor.environment == {"hello": "world"}
    assert executor.resource_spec_kwargs == {"node_count": 2}
    assert executor.job_attributes_kwargs == {"duration": timedelta(minutes=20)}
    assert executor.launcher == "multiple"
    assert executor.remote_python_executable == "python2"
    assert executor.remote_conda_env == "myenv"
    assert executor.remote_workdir == "~/my-remote-dir"
    assert executor.create_unique_workdir == True
    assert executor.cache_dir == tmpdir / "my-cache-dir"
    assert executor.poll_freq == 90
    assert os.path.exists(tmpdir / "my-cache-dir")

    # Test poll freq is auto-raised
    executor = HPCExecutor(address=address, poll_freq=10)
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
        ssh_key_file=SSH_KEY_FILE,
        cert_file=CERT_FILE,
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
    """Test pickle script works appropriately"""
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
    assert pickle_load[1] == None


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
        remote_python_executable="python3",
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

    remote_workdir = "/federation/test_user/.cache/covalent"
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
    assert "pre_launch" not in submit_script_str


def test_submit_script(tmpdir):
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

    submmit_str = executor._format_job_script()
    assert f'JobExecutor.get_instance("local")' in submmit_str

    with open("test_submit.py", "w") as w:
        w.write(submmit_str)
    p = subprocess.run(
        "python test_submit.py", shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )
    assert p.returncode == 0
    assert p.stderr == b""
    assert p.stdout != b""
    assert int(p.stdout) > 0


def test_format_query_script():
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


# def test_query_script(tmpdir):
#     tmpdir.chdir()

#     executor = HPCExecutor(
#         username="test_user",
#         address="test_address",
#         instance="local",
#     )

#     executor._jobid = "123456"
#     query_str = executor._format_query_status_script()
#     assert f'JobExecutor.get_instance("local")' in query_str
#     with open("test_submit.py", "w") as w:
#         w.write(query_str)
#     p = subprocess.run(
#         "python test_submit.py", shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE
#     )
#     assert p.returncode == 0
#     assert p.stderr == b""
#     assert p.stdout == b""


# @pytest.mark.asyncio
# async def test_failed_submit_script(mocker, conn_mock):
#     "Test for expected errors"

#     mocker.patch("asyncssh.connect", return_value=conn_mock)

#     with pytest.raises(FileNotFoundError):
#         executor = SlurmExecutor(
#             username="test_user",
#             address="test_address",
#             ssh_key_file="/this/file/does/not/exist",
#             remote_workdir="/federation/test_user/.cache/covalent",
#             options={},
#             cache_dir="~/.cache/covalent",
#             poll_freq=60,
#         )
#         await executor._client_connect()

#     with pytest.raises(FileNotFoundError):
#         executor = SlurmExecutor(
#             username="test_user",
#             address="test_address",
#             ssh_key_file=SSH_KEY_FILE,
#             cert_file="/this/file/does/not/exist",
#             remote_workdir="/federation/test_user/.cache/covalent",
#             options={},
#             cache_dir="~/.cache/covalent",
#             poll_freq=60,
#         )
#         await executor._client_connect()

#     with pytest.raises(ValueError):
#         executor = SlurmExecutor(address="test_address", ssh_key_file=SSH_KEY_FILE)
#         await executor._client_connect()

#     with pytest.raises(ValueError):
#         executor = SlurmExecutor(username="test", ssh_key_file=SSH_KEY_FILE)
#         await executor._client_connect()

#     with pytest.raises(ValueError):
#         executor = SlurmExecutor(username="test", address="test_address")
#         await executor._client_connect()


# @pytest.mark.asyncio
# async def test_get_status(proc_mock, conn_mock):
#     """Test the get_status method."""

#     executor = SlurmExecutor(
#         username="test_user",
#         address="test_address",
#         ssh_key_file=SSH_KEY_FILE,
#         remote_workdir="/federation/test_user/.cache/covalent",
#         options={},
#         cache_dir="~/.cache/covalent",
#         poll_freq=60,
#     )

#     proc_mock.returncode = 0
#     proc_mock.stdout = "Fake Status"
#     proc_mock.stderr = "stderr"

#     conn_mock.run = mock.AsyncMock(return_value=proc_mock)

#     status = await executor.get_status({}, conn_mock)
#     assert status == Result.NEW_OBJ

#     status = await executor.get_status({"job_id": 0}, conn_mock)
#     assert status == "Fake Status"
#     assert conn_mock.run.call_count == 2


# @pytest.mark.asyncio
# async def test_poll_scheduler(proc_mock, conn_mock):
#     """Test that polling the status works."""

#     executor = SlurmExecutor(
#         username="test_user",
#         address="test_address",
#         ssh_key_file=SSH_KEY_FILE,
#         remote_workdir="/federation/test_user/.cache/covalent",
#         options={},
#         slurm_path="sample_path",
#         cache_dir="~/.cache/covalent",
#         poll_freq=60,
#     )

#     proc_mock.returncode = 0
#     proc_mock.stdout = "COMPLETED"
#     proc_mock.stderr = "stderr"

#     conn_mock.run = mock.AsyncMock(return_value=proc_mock)

#     # Check completed status does not give any errors
#     await executor._poll_scheduler(0, conn_mock)
#     conn_mock.run.assert_called_once()

#     # Now give an "error" in the get_status method and check that the
#     # correct exception is raised.
#     proc_mock.returncode = 1
#     proc_mock.stdout = "AN ERROR"
#     conn_mock.run = mock.AsyncMock(return_value=proc_mock)

#     try:
#         await executor._poll_scheduler(0, conn_mock)
#     except RuntimeError as raised_exception:
#         expected_exception = RuntimeError("Job failed with status:\n", "AN ERROR")
#         assert isinstance(raised_exception, type(expected_exception))
#         assert raised_exception.args == expected_exception.args

#     conn_mock.run.assert_called_once()


# @pytest.mark.asyncio
# async def test_query_result(mocker, proc_mock, conn_mock):
#     """Test querying results works as expected."""

#     executor = SlurmExecutor(
#         username="test_user",
#         address="test_address",
#         ssh_key_file=SSH_KEY_FILE,
#         remote_workdir="/federation/test_user/.cache/covalent",
#         options={"output": "stdout_file", "error": "stderr_file"},
#         cache_dir="~/.cache/covalent",
#         poll_freq=60,
#     )

#     # First test when the remote result file is not found by mocking the return code
#     # with a non-zero value.
#     proc_mock.returncode = 1
#     proc_mock.stdout = "stdout"
#     proc_mock.stderr = "stderr"

#     conn_mock.run = mock.AsyncMock(return_value=proc_mock)

#     try:
#         await executor._query_result(
#             result_filename="mock_result", task_results_dir="", conn=conn_mock
#         )
#     except Exception as raised_exception:
#         expected_exception = FileNotFoundError(1, "stderr")
#         assert isinstance(raised_exception, type(expected_exception))
#         assert raised_exception.args == expected_exception.args

#     # Now mock result files.
#     proc_mock.returncode = 0
#     conn_mock.run = mock.AsyncMock(return_value=proc_mock)

#     mocker.patch("asyncssh.scp", return_value=mock.AsyncMock())

#     # Don't actually try to remove result files:
#     async_os_remove_mock = mock.AsyncMock(return_value=None)
#     mocker.patch("aiofiles.os.remove", side_effect=async_os_remove_mock)

#     # Mock the opening of specific result files:
#     expected_results = [1, 2, 3, 4, 5]
#     expected_error = None
#     expected_stdout = "output logs"
#     expected_stderr = "output errors"
#     pickle_mock = mocker.patch(
#         "cloudpickle.loads", return_value=(expected_results, expected_error)
#     )
#     unpatched_open = open

#     def mock_open(*args, **kwargs):
#         if args[0] == "mock_result":
#             return mock.mock_open(read_data=None)(*args, **kwargs)
#         elif args[0] == executor.options["output"]:
#             return mock.mock_open(read_data=expected_stdout)(*args, **kwargs)
#         elif args[0] == executor.options["error"]:
#             return mock.mock_open(read_data=expected_stderr)(*args, **kwargs)
#         else:
#             return unpatched_open(*args, **kwargs)

#     with mock.patch("aiofiles.threadpool.sync_open", mock_open):
#         result, stdout, stderr, exception = await executor._query_result(
#             result_filename="mock_result", task_results_dir="", conn=conn_mock
#         )

#         assert result == expected_results
#         assert exception == expected_error
#         assert stdout == expected_stdout
#         assert stderr == expected_stderr
#         pickle_mock.assert_called_once()


# @pytest.mark.asyncio
# async def test_run(mocker, proc_mock, conn_mock):
#     """Test calling run works as expected."""
#     executor1 = SlurmExecutor(
#         username="test_user",
#         address="test_address",
#         ssh_key_file="~/.ssh/id_rsa",
#     )

#     executor2 = SlurmExecutor(
#         username="test_user",
#         address="test_address",
#         ssh_key_file="~/.ssh/id_rsa",
#         remote_workdir="/scratch/user/experiment1",
#         create_unique_workdir=True,
#         conda_env="my-conda-env",
#         options={"nodes": 1, "c": 8, "qos": "regular"},
#         srun_options={"slurmd-debug": 4, "n": 12, "cpu_bind": "cores"},
#         srun_append="nsys profile --stats=true -t cuda --gpu-metrics-device=all",
#         prerun_commands=[
#             "module load package/1.2.3",
#             "srun --ntasks-per-node 1 dcgmi profile --pause",
#         ],
#         postrun_commands=[
#             "srun --ntasks-per-node 1 dcgmi profile --resume",
#             "python ./path/to/my/post_process.py -j $SLURM_JOB_ID",
#         ],
#     )
#     for executor in [executor1, executor2]:
#         # dummy objects
#         def f(x, y):
#             return x + y

#         dummy_function = partial(wrapper_fn, TransportableObject(f), call_before=[], call_after=[])

#         dummy_metadata = {
#             "dispatch_id": "259efebf-2c69-4981-a19e-ec90cdffd026",
#             "node_id": 1,
#             "results_dir": "results/directory/on/remote",
#         }

#         dummy_args = (
#             dummy_function,
#             [TransportableObject(2)],
#             {"y": TransportableObject(3)},
#             dummy_metadata,
#         )

#         dummy_error_msg = "dummy_error_message"

#         # mock behavior
#         conn_mock.run = mock.AsyncMock(return_value=proc_mock)
#         conn_mock.wait_closed = mock.AsyncMock(return_value=None)

#         def reset_proc_mock():
#             proc_mock.stdout = ""
#             proc_mock.stderr = ""
#             proc_mock.returncode = 0

#         async def __client_connect_fail(*_):
#             return conn_mock

#         async def __client_connect_succeed(*_):
#             return conn_mock

#         async def __poll_scheduler_succeed(*_):
#             return

#         async def __query_result_fail(*_):
#             return None, proc_mock.stdout, proc_mock.stderr, dummy_error_msg

#         async def __query_result_succeed(*_):
#             return "result", "", "", None

#         # patches
#         patch_ccf = mock.patch.object(SlurmExecutor, "_client_connect", new=__client_connect_fail)
#         patch_ccs = mock.patch.object(
#             SlurmExecutor, "_client_connect", new=__client_connect_succeed
#         )
#         patch_pss = mock.patch.object(
#             SlurmExecutor, "_poll_scheduler", new=__poll_scheduler_succeed
#         )
#         patch_qrf = mock.patch.object(SlurmExecutor, "_query_result", new=__query_result_fail)
#         patch_qrs = mock.patch.object(SlurmExecutor, "_query_result", new=__query_result_succeed)

#         # check failed ssh connection handled as expected
#         with patch_ccf:
#             msg = f"Could not connect to host: '{executor.address}' as user: '{executor.username}'"
#             with pytest.raises(Exception) as exc_info:
#                 await executor.run(*dummy_args)
#                 assert exc_info.type is RuntimeError
#                 assert exc_info.value.args == (msg,)

#         # check failed creation of remote directory handled as expected
#         msg = "Failed to create directory"
#         proc_mock.stderr = msg
#         with patch_ccs:
#             with pytest.raises(Exception) as exc_info:
#                 await executor.run(*dummy_args)
#                 assert exc_info.type is RuntimeError
#                 assert exc_info.value.args == (msg,)
#         reset_proc_mock()

#         # check run call completes with no other errors when `slurm_path` specified
#         executor.slurm_path = "/path/to/slurm"
#         proc_mock.stdout = "53034272 COMPLETED"
#         with patch_ccs, patch_qrs:
#             mocker.patch("asyncssh.scp", return_value=mock.AsyncMock())
#             await executor.run(*dummy_args)
#         executor.slurm_path = None
#         reset_proc_mock()

#         # check failed verification of slurm installation handled as expected
#         msg = "Please provide `slurm_path` to run sbatch command"
#         proc_mock.returncode = 1
#         with patch_ccs:
#             mocker.patch("asyncssh.scp", return_value=mock.AsyncMock())
#             with pytest.raises(Exception) as exc_info:
#                 await executor.run(*dummy_args)
#                 assert exc_info.type is RuntimeError
#                 assert exc_info.value.args == (msg,)
#         reset_proc_mock()

#         # check failed `cmd_sbatch` run on remote handled as expected
#         executor.slurm_path = "/path/to/slurm"
#         proc_mock.returncode = 1
#         with patch_ccs, patch_pss:
#             mocker.patch("asyncssh.scp", return_value=mock.AsyncMock())
#             with pytest.raises(Exception) as exc_info:
#                 await executor.run(*dummy_args)
#                 assert exc_info.type is RuntimeError
#                 assert exc_info.value.args == ("",)
#         executor.slurm_path = None
#         reset_proc_mock()

#         # check failed query handled as expected
#         proc_mock.stdout = "64145383 FAILED"
#         with patch_ccs, patch_pss, patch_qrf:
#             mocker.patch("asyncssh.scp", return_value=mock.AsyncMock())
#             with pytest.raises(Exception) as exc_info:
#                 await executor.run(*dummy_args)
#                 assert exc_info.type is RuntimeError
#                 assert exc_info.value.args == (dummy_error_msg,)
#         reset_proc_mock()

#         # check run call completes with no other errors
#         proc_mock.stdout = "75256494 COMPLETED"
#         with patch_ccs, patch_qrs:
#             mocker.patch("asyncssh.scp", return_value=mock.AsyncMock())
#             await executor.run(*dummy_args)
#         reset_proc_mock()


# @pytest.mark.asyncio
# async def test_teardown(mocker, proc_mock, conn_mock):
#     """Test calling run works as expected."""
#     executor = SlurmExecutor(
#         username="test_user",
#         address="test_address",
#         ssh_key_file="~/.ssh/id_rsa",
#         remote_workdir="/scratch/user/experiment1",
#         create_unique_workdir=True,
#         conda_env="my-conda-env",
#         options={"nodes": 1, "c": 8, "qos": "regular"},
#         srun_options={"slurmd-debug": 4, "n": 12, "cpu_bind": "cores"},
#         srun_append="nsys profile --stats=true -t cuda --gpu-metrics-device=all",
#         prerun_commands=[
#             "module load package/1.2.3",
#             "srun --ntasks-per-node 1 dcgmi profile --pause",
#         ],
#         postrun_commands=[
#             "srun --ntasks-per-node 1 dcgmi profile --resume",
#             "python ./path/to/my/post_process.py -j $SLURM_JOB_ID",
#         ],
#     )

#     # dummy objects
#     def f(x, y):
#         return x + y

#     dummy_function = partial(wrapper_fn, TransportableObject(f), call_before=[], call_after=[])

#     dummy_metadata = {
#         "dispatch_id": "259efebf-2c69-4981-a19e-ec90cdffd026",
#         "node_id": 1,
#         "results_dir": "results/directory/on/remote",
#     }

#     dummy_args = (
#         dummy_function,
#         [TransportableObject(2)],
#         {"y": TransportableObject(3)},
#         dummy_metadata,
#     )

#     # mock behavior
#     conn_mock.run = mock.AsyncMock(return_value=proc_mock)
#     conn_mock.wait_closed = mock.AsyncMock(return_value=None)

#     async def __client_connect_succeed(*_):
#         return conn_mock

#     async def __query_result_succeed(*_):
#         return "result", "", "", None

#     async def __perform_cleanup(*_, **__):
#         return

#     # patches
#     patch_ccs = mock.patch.object(SlurmExecutor, "_client_connect", new=__client_connect_succeed)
#     patch_qrs = mock.patch.object(SlurmExecutor, "_query_result", new=__query_result_succeed)
#     patch_pc = mock.patch.object(SlurmExecutor, "perform_cleanup", new=__perform_cleanup)

#     # check teardown method works as expected
#     proc_mock.stdout = "86367505 COMPLETED"
#     proc_mock.stderr = ""
#     proc_mock.returncode = 0
#     with patch_ccs, patch_qrs, patch_pc:
#         mocker.patch("asyncssh.scp", return_value=mock.AsyncMock())
#         await executor.run(*dummy_args)
#         await executor.teardown(dummy_metadata)
#         await executor.teardown(dummy_metadata)
#         await executor.teardown(dummy_metadata)
#         await executor.teardown(dummy_metadata)
