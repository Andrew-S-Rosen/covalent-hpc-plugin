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

"""Tests for Covalent custom async executor."""

import covalent as ct
from covalent._workflow.transport import TransportableObject
import covalent._results_manager.results_manager as rm
from covalent_executor_template.custom import CustomExecutor
from functools import partial
from covalent.executor.base import wrapper_fn


def test_init():
    """Test that initialization properly sets member variables."""

    arguments = {
        "executor_input1": "input1",
        "executor_input2": 5,
        "kwarg": "custom param",
        "current_env_on_conda_fail": True,
    }

    executor = CustomExecutor(**arguments)

    assert executor.executor_input1 == "input1"
    assert executor.executor_input2 == 5
    assert executor.kwargs["kwarg"] == "custom param"
    assert executor.current_env_on_conda_fail == True

def test_deserialization(mocker):
    """Test that the input function and its arguments are deserialized."""

    executor = CustomExecutor(executor_input1="input1")

    def simple_task(x, kw):
        return x

    transportable_func = TransportableObject(simple_task)
    input_function = partial(wrapper_fn, transportable_func, [], [])
    arg = TransportableObject(5)
    args = [arg]
    kwarg = TransportableObject(None)
    kwargs = {"kw": kwarg}

    deserialized_func_mock = mocker.patch.object(
        transportable_func,
        "get_deserialized",
        return_value = simple_task,
    )

    deserialized_args_mock = mocker.patch.object(
        arg,
        "get_deserialized",
        return_value=[5],
    )
    
    deserialized_kwargs_mock = mocker.patch.object(
        kwarg,
        "get_deserialized",
        return_value={},
    )

    executor.run(
        function=input_function,
        args=args,
        kwargs=kwargs,
        task_metadata={"dispatch_id": 0, "node_id": 0},
    )

    deserialized_func_mock.assert_called_once()
    deserialized_args_mock.assert_called_once()
    deserialized_kwargs_mock.assert_called_once()

def test_function_call(mocker):
    """Test that the deserialized function is called with correct arguments."""

    executor = CustomExecutor(executor_input1="input1")

    simple_task = mocker.MagicMock(return_value=0)
    simple_task.__name__ = "simple_task"

    transport_function = TransportableObject(simple_task)

    deserialized_func_mock = mocker.patch.object(
        transport_function,
        "get_deserialized",
        return_value = simple_task,
    )

    args = [TransportableObject(5)]
    kwargs = {"kw_arg": TransportableObject(10)}

    executor.run(
        function = partial(wrapper_fn, transport_function, [], []),
        args = args,
        kwargs = kwargs,
        task_metadata={"dispatch_id": 0, "node_id": 0},
    )

    deserialized_func_mock.assert_called_once()
    simple_task.assert_called_once_with(5, kw_arg=10)

def test_final_result():
    """Functional test to check that the result of the function execution is as expected."""


    executor = CustomExecutor(executor_input1="input1")

    @ct.electron(executor=executor)
    def simple_task(x):
        return x

    @ct.lattice
    def sample_workflow(a):
        c = simple_task(a)
        return c

    dispatch_id = ct.dispatch(sample_workflow)(5)
    print(dispatch_id)
    result = ct.get_result(dispatch_id=dispatch_id, wait=True)
    print(result)

    rm._delete_result(dispatch_id)

    # The custom executor has a doubling of each electron's result, for illustrative purporses.
    assert result.result == 10
