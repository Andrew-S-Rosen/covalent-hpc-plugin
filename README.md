&nbsp;

<div align="center">

<img src="https://raw.githubusercontent.com/AgnostiqHQ/covalent/master/doc/source/_static/covalent_readme_banner.svg" width=150%>

</div>

## Covalent HPC Plugin

[Covalent](https://www.covalent.xyz) is a Pythonic workflow tool used to execute tasks on advanced computing hardware. This executor plugin uses [PSI/J](https://exaworks.org/psij-python/index.html) to allow Covalent to seamlessly interface with a variety of common high-performance computing job schedulers and pilot systems (e.g. Slurm, PBS, IBM Spectrum LSF, Flux, Cobalt, RADICAL-Pilot). For workflows to be deployable, users must have SSH access to the login node, access to the job scheduler, and write access to the remote filesystem.

## Installation

### Server Environment

To use this plugin with Covalent, simply install it using `pip` in whatever Python environment you use to run the Covalent server:

```
pip install git+https://github.com/arosen93/covalent-hpc-plugin.git
```

### HPC Environment

Additionally, on the remote machine(s) where you plan to execute Covalent workflows with this plugin, ensure that the remote Python environment has both Covalent and PSI/J installed:

```
pip install covalent psij-python
```

Note that the Python major and minor version numbers on both the local and remote machines must match to ensure the `cloudpickle` dependency can reliably (un)pickle the various objects.

## Usage

### Configuration File

By default, when you install the `covalent-hpc-plugin` and `import covalent` for the first time, your Covalent [configuration file](https://docs.covalent.xyz/docs/user-documentation/how-to/customization/) (found at `~/.config/covalent/covalent.conf` by default) will automatically be updated to include the following sections. These are not all of the available parameters but are simply the default values.

```
[executors.hpc]
address = ""
username = ""
ssh_key_file = "~/.ssh/id_rsa"
instance = "slurm"
inherit_environment = true
launcher = "single"
remote_python_executable = "python"
remote_workdir = "~/covalent-workdir"
create_unique_workdir = false
cache_dir = "~/.cache/covalent"
poll_freq = 60

[executors.hpc.environment]

[executors.hpc.resource_spec_kwargs]
node_count = 1
exclusive_node_use = false
process_count = 1
processes_per_node = 1
cpu_cores_per_process = 1
gpu_cores_per_process = 0

[executors.hpc.job_attributes_kwargs]
duration = 10
```

A full description of the various input parameters are described in the docstrings of the `HPCExecutor` class, reproduced below:

https://github.com/arosen93/covalent-hpc-plugin/blob/9688f04d0fac3caaa8accb6eb9da6715c8c5f264/covalent_hpc_plugin/hpc.py#L114-L181

Two of the most important sets of parameters are `resource_spec_kwargs` and `job_attributes_kwargs`, which used to specify the resources required for the job (e.g. number of nodes, number of processes per node, etc.) and the job attributes (e.g. duration, queue name, etc.), respectively. The `resource_spec_kwargs` is a dictionary of keyword arguments passed to PSI/J's [`ResourceSpecV1`](https://exaworks.org/psij-python/docs/v/0.9.0/.generated/psij.html#psij.resource_spec.ResourceSpecV1) class, whereas `job_attributes_kwargs` is a dictionary of keyword arguments passed to PSI/J's [`JobAttributes`](https://exaworks.org/psij-python/docs/v/0.9.0/.generated/psij.html#psij.JobAttributes) class. The allowed types are shown below:

https://github.com/arosen93/covalent-hpc-plugin/blob/367a84acd2114b31cf603f6b8a6e3f46c246c44a/covalent_hpc_plugin/hpc.py#L85-L111

With the configuration file appropriately set up, one can run a workflow on the HPC machine as follows:

```python
import covalent as ct

@ct.electron(executor="HPCExecutor")
def add(a, b):
    return a + b

@ct.lattice
def workflow(a, b):
    return add(a, b)


dispatch_id = ct.dispatch(workflow)(1, 2)
result = ct.get_result(dispatch_id)
```

### Instantiating the `HPCExecutor` Class

If you wish to modify the various parameters within your Python script rather than solely relying on the the Covalent configuration file, it is possible to do that as well by instantiating a custom instance of the `HPCExecutor` class. An example with some commonly used parameters is shown below. By defauly, any parameters not specified in the `HPCExecutor` will be inherited from the configuration file.

```python
import covalent as ct

executor = ct.executor.HPCExecutor(
    address="coolmachine.university.edu",
    username="UserName",
    ssh_key_file="~/.ssh/id_resa",
    instance="slurm",
    remote_conda_env="myenv",
    environment={"HELLO": "WORLD"},
    resource_spec_kwargs={
        "node_count": 2,
        "processes_per_node": 24
    },
    job_attributes_kwargs={
        "duration": 30, # minutes
        "queue_name": "debug",
        "project_name": "AccountName",
    },
    launcher="single",
    remote_conda_env="myenv",
    remote_workdir="~/covalent-workdir",
)
```

## Working Example: Perlmutter

The following is a minimal working example to submit a Covalent job on NERSC's [Perlmutter](https://docs.nersc.gov/systems/perlmutter/) machine. This example assumes you have an account named "UserName", a project named "ProjectName", and a Conda environment named `myenv` on Perlmutter with both Covalent and PSI/J installed. It also assumes that you have used the [sshproxy](https://docs.nersc.gov/connect/mfa/#sshproxy) utility to generate a certificate file in order to bypass the need for multi-factor authentication.

```python
import covalent as ct

executor = ct.executor.HPCExecutor(
    address="perlmutter-p1.nersc.gov",
    username="UserName",
    ssh_key_file="~/.ssh/nersc",
    cert_file="~/.ssh/nersc-cert.pub",
    remote_conda_env="myenv",
    job_attributes_kwargs={
        "project_name": "ProjectName",
        "custom_attributes": {"slurm.constraint": "cpu", "slurm.qos": "debug"},
    },
)

@ct.electron(executor=executor)
def add(a, b):
    return a + b

@ct.lattice
def workflow(a, b):
    return add(a, b)


dispatch_id = ct.dispatch(workflow)(1, 2)
result = ct.get_result(dispatch_id)
```

## Release Notes

Release notes are available in the [Changelog](CHANGELOG.md).

## Credit

This plugin was developed by [Andrew S. Rosen](https://github.com/arosen93), building off of prior work by the Agnostiq team on the [covalent-slurm-plugin](https://github.com/AgnostiqHQ/covalent-slurm-plugin).

If you use this plugin, be sure to cite Covalent as follows:

> W. J. Cunningham, S. K. Radha, F. Hasan, J. Kanem, S. W. Neagle, and S. Sanand.
> _Covalent._ Zenodo, 2022. https://doi.org/10.5281/zenodo.5903364

## License

Covalent is licensed under the GNU Affero GPL 3.0 License. Covalent may be distributed under other licenses upon request. See the [LICENSE](https://github.com/AgnostiqHQ/covalent-executor-template/blob/main/LICENSE) file or contact the [support team](mailto:support@agnostiq.ai) for more details.
