&nbsp;

<div align="center">

<img src="https://raw.githubusercontent.com/AgnostiqHQ/covalent/master/doc/source/_static/covalent_readme_banner.svg" width=150%>

</div>

## Covalent HPC Plugin

Covalent is a Pythonic workflow tool used to execute tasks on advanced computing hardware. This executor plugin uses [PSI/J](https://exaworks.org/psij-python/index.html) to allow Covalent to seamlessly interface with a variety of common high-performance computing job schedulers (e.g. Slurm, PBS, LSF, Flux, Cobalt). For workflows to be deployable, users must have SSH access to the login node on their desired machine and PSI/J installed in their remote machine's Python environment.

## Installation

To use this plugin with Covalent, simply install it using `pip`:

```
pip install git+...
```

## Usage

TODO.

## Release Notes

Release notes are available in the [Changelog](CHANGELOG.md).

## Citation

Please use the following citation in any publications:

> W. J. Cunningham, S. K. Radha, F. Hasan, J. Kanem, S. W. Neagle, and S. Sanand.
> _Covalent._ Zenodo, 2022. https://doi.org/10.5281/zenodo.5903364

## License

Covalent is licensed under the GNU Affero GPL 3.0 License. Covalent may be distributed under other licenses upon request. See the [LICENSE](https://github.com/AgnostiqHQ/covalent-executor-template/blob/main/LICENSE) file or contact the [support team](mailto:support@agnostiq.ai) for more details.
