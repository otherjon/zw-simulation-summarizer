#!/usr/bin/env python

import sys
import os.path
import re
import time
import argparse
import csv


PER_YEAR_FIELDS = (
  "[step]",
  "calendar-year",
  "rainfall",
  "crop-eaten",
  "current-harvest",
  "mean previous-harvests-list",
  "count cows",
  "total-woodland-biomass",
  "subsidy-used",
  "total-number-of-births",
  "count-cows-in-crops",
  "timer",
)
PER_RUN_FIELDS = (
  "[run number]",
  "model-mode",
  "times-per-day-farmers-move-cows",
  "invincible-fences",
  "key-resources",
  "subsidy",
  "cow-proportion-to-save",
  "rainfall-type",
  "muonde-projects",
  "rain-site",
  "how-long-to-store-grain",
  "use-muonde-thresholds",
  "clumpiness",
  "total-mud-crop-perimeter",
  "wood-to-build-fence-per-meter",
  "termite-activity",
  "hours-to-plough-ha",
  "crop-growth-slope",
  "zero-crop-growth-intercept",
  "muonde-efficiency",
  "woodland-growth-slope",
  "cow-maintenance-energy-rate",
  "cow-working-energy-per-hour",
  "kcal-per-kg-of-browse",
  "kcal-per-kg-of-crop",
  "kcal-per-kg-of-cow",
  "production-efficiency",
  "catabolism-efficiency",
  "min-cow-mass",
  "max-cow-mass",
  "calf-birth-mass",
  "livestock-not-reproduction-rate-per-year",
  "morans-i",
  "gearys-c",
  "total-crop-perimeter",
  "average-contiguous-crop-cluster-size",
  "proportion-crops",
  "model-setup-time",
)
CLUSTER_HEADER_FIELDS = (
  'nlogo-file', 'source-file-from-cluster', 'behaviorspace-name',
  'date-and-time-of-run')
PER_RUN_OUTPUT_FIELDS = (
  ("run number",) + PER_RUN_FIELDS[1:] + CLUSTER_HEADER_FIELDS)
PER_YEAR_OUTPUT_FIELDS = ("step",) + PER_YEAR_FIELDS[1:]


def parse_cmdline(argv):
  desc=("Process output files from running model on HPC cluster.\n"
        "Produce summary data from raw results.")
  parser = argparse.ArgumentParser(description=desc)
  parser.add_argument('--cluster-dir', default=".", help='directory in which '
                      'raw cluster output files can be found')
  parser.add_argument('--output-dir',
                      help='Directory in which to create output files (defaults to CLUSTER_DIR/output)')
  default_per_run_output_template = (
    'ProcessedData-PerRun-Run%(run number)06d-%(behaviorspace-name)s.csv')
  default_per_year_output_template = (
    'ProcessedData-PerYear-Run%(run number)06d-%(behaviorspace-name)s.csv')

  parser.add_argument(
    '--per-run-output-template', default=default_per_run_output_template,
    help='Output filename template for PER-RUN processed results')
  parser.add_argument(
    '--per-year-output-template', default=default_per_year_output_template,
    help='Output filename template for PER-YEAR processed results')

  parser.add_argument('--overwrite', action="store_true",
                      default=False, help='Overwrite existing output file')

  result = parser.parse_args(argv)

  if not os.path.exists(os.path.join(result.cluster_dir, "ModelList.txt")):
    print "ERROR: ModelList.txt not found in cluster dir"
    sys.exit(1)

  return parser.parse_args(argv)


def get_files(args):
  model_list_file = os.path.join(args.cluster_dir, "ModelList.txt")
  datafiles = []
  with open(model_list_file) as f:
    for line in f:
      if line.endswith('\n'): line = line[:-1]
      if line == "RunSoftwareTests.dat": continue
      datafiles.append(os.path.join(args.cluster_dir, line))

  return datafiles


def process_files(args, filenames):
  for filename in filenames:
    per_run_data, per_year_data = read_file(filename)
    write_processed_data(args, per_run_data, per_year_data)


def extract_dict_from_row(row, fieldnames):
  def fieldname_map(fieldname):
    if fieldname == "[run number]":
      return "run number"
    if fieldname == "[step]":
      return "step"
    else:
      return fieldname

  def field_val_map(data):
    m = re.match('"(.*)"$', data)
    if m: return m.group(1)
    m = re.match("[0-9+-]+$", data)
    if m: return int(data)
    m = re.match("[0-9.+-]+$", data)
    if m: return float(data)
    return data

  data = {
    fieldname_map(fieldname): field_val_map(row[fieldname])
    for fieldname in fieldnames
  }
  return data


def read_file(filename):
  expected_fields = PER_RUN_FIELDS + PER_YEAR_FIELDS + ("[run number]",)

  fields_verified = False
  with open(filename) as f:
    behaviorspace_header_line = f.readline()
    nlogo_filename_line = f.readline()
    behaviorspace_name = f.readline()
    date_and_time = f.readline()
    pxycor_headers = f.readline()
    pxycor_data = f.readline()

    cluster_file_data = extract_dict_from_row(
      {
        "nlogo-file": nlogo_filename_line.strip(),
        "behaviorspace-name": behaviorspace_name.strip(),
        "date-and-time-of-run": date_and_time.strip(),
        "source-file-from-cluster": filename,
      }, CLUSTER_HEADER_FIELDS)

    # Actual CSV data starts here!
    reader = csv.DictReader(f)
    per_run_data = {}
    per_year_data = {}
    for row in reader:
      if not fields_verified:
        if set(expected_fields) != set(row.keys()):
          print "ERROR: Field name mismatch!"
          missing = [field for field in expected_fields
                     if field not in row.keys()]
          extra = [field for field in row.keys()
                   if field not in expected_fields]
          if missing: print "  Missing fields: %r" % sorted(missing)
          if extra: print "Extra fields: %r" % sorted(extra)
          sys.exit(1)
        fields_verified = True

      run_num = row["[run number]"]
      if run_num not in per_run_data:
        per_run_data[run_num] = cluster_file_data.copy()
        per_run_data[run_num].update(
          extract_dict_from_row(row, PER_RUN_FIELDS))
      per_year_data.setdefault(run_num, {})
      per_year_data[run_num][row["calendar-year"]] = (
        extract_dict_from_row(row, PER_YEAR_FIELDS))
    # End per-CSV-row processing

  # File is now closed
  return per_run_data, per_year_data


def write_processed_data(args, per_run_data, per_year_data):
  output_dir = args.output_dir or os.path.join(args.cluster_dir, "output")
  if not os.path.exists(output_dir):
    os.mkdir(output_dir)

  for run_num in sorted(per_run_data.keys()):
    per_run_filename = os.path.join(
      output_dir, args.per_run_output_template % per_run_data[run_num])
    per_year_filename = os.path.join(
      output_dir, args.per_year_output_template % per_run_data[run_num])


    cluster_data_fields = (
      "nlogo-file", "behaviorspace-name", "date-and-time-of-run",
      "source-file-from-cluster")

    with open(per_run_filename, "w") as per_run:
      per_run_csv = csv.DictWriter(per_run, fieldnames=PER_RUN_OUTPUT_FIELDS)
      per_run_csv.writeheader()
      per_run_csv.writerow(per_run_data[run_num])


    with open(per_year_filename, "w") as per_year:
      per_year_csv = csv.DictWriter(per_year, fieldnames=PER_YEAR_OUTPUT_FIELDS)
      per_year_csv.writeheader()
      for year in sorted(per_year_data[run_num].keys()):
        data = per_year_data[run_num][year]
        per_year_csv.writerow(data)


def main():
  args = parse_cmdline(sys.argv[1:])
  datafiles = get_files(args)
  process_files(args, datafiles)


if __name__ == '__main__': main()
