# zw-simulation-summarizer

Summarize the output from large-scale simulation of a Zimbabwean
agro-pastoral system.  See https://github.com/mveitzel/ZAPMM for the
simulation.

This repo is unlikely to be of interest to anyone not involved with this
research project.

# Usage

Run `process_model_output.py --help` to show complete usage information.

Simple usage: `process_model_output.py --min-harvest 10 --min-woodland 10`


# Suggested directory structure
* Have a subdirectory `raw_data` containing output from the simulation.
  (`*.dat` files, including `RunSoftwareTests.dat`)
* The program will create the directory `intermediate/`, containing intermediate
  data files extracted from the raw data.
* By default, the output CSV will be written in the parent directory of those
  two subdirectories.  (Default can be overridden with `--output-file` flag.)

