#!/usr/bin/env python

import sys
import os.path
import random
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
  ("Run ID", "run number",) + PER_RUN_FIELDS[1:] + CLUSTER_HEADER_FIELDS)
PER_YEAR_OUTPUT_FIELDS = ("step",) + PER_YEAR_FIELDS[1:]

SUMMARY_FIELDS = (
  "Run ID",
  "times-per-day-farmers-move-cows",
  "invincible-fences",
  "key-resources",
  "subsidy",
  "cow-proportion-to-save",
  "rainfall-type",
  "muonde-projects",
  "rain-site",
  "how-long-to-store-grain",
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
  "final-year-timer",
  "min-cows-threshold",
  "min-harvest-threshold",
  "min-woodland-threshold",
  "min-cow-count",
  "mean-cow-count",
  "max-cow-count",
  "min-harvest",
  "mean-harvest",
  "max-harvest",
  "total-harvest",
  "min-woodland-biomass",
  "mean-woodland-biomass",
  "max-woodland-biomass",
  "max-percent-crop-eaten",
  "actual-cow-repro-rate",
  "crop-eaten-per-half-hour-per-cow",
  "subsidy-used",
  "end-year",
  "termination-reason",
)
EXCLUDE_FROM_SUMMARY = (
  "date-and-time-of-run",
  "model-mode",
  "nlogo-file",
  "source-file-from-cluster",
  "behaviorspace-name",
  "run number",
)

def parse_cmdline(argv):
  desc="""\
(1) Process raw files from HPC cluster to produce intermediate data files.
(2) Summarize intermediate data files and output summary CSV."""
  parser = argparse.ArgumentParser(description=desc)

  parser.add_argument(
    '--stage', choices=('autodetect', 'raw-to-int', 'int-to-final', 'all'),
    default='autodetect', help="Which stages of processing to run.")
  parser.add_argument(
    '--cluster-dir', default="./raw_data",
    help='directory in which raw cluster output files can be found')
  parser.add_argument('--intermediate-dir', help='Directory to put intermediate'
                      ' files (default CLUSTER_DIR/../intermediate)')
  parser.add_argument('--output-file', help='Filename of final output CSV'
                      ' (default CLUSTER_DIR/../YYYY-MM-DD_SummarizedData.csv)')
  parser.add_argument('--min-cows', help='Minimum threshold required for '
                      'number of cows in any year', type=int, default=1)
  parser.add_argument('--min-harvest', help='Minimum threshold required for '
                      'harvest (metric tons) in any year',
                      type=float, default=0.0)
  parser.add_argument('--min-woodland', help='Minimum threshold required for '
                      'woodland (hectares) in any year',
                      type=float, default=0.0)
  parser.add_argument('--perturb-cows', help='How much (in cows) to perturb '
                      'the minimum cow threshold',
                      type=int, default=0)
  parser.add_argument('--perturb-harvest', help='How much (in %) to perturb '
                      'the minimum harvest threshold',
                      type=float, default=0.0)
  parser.add_argument('--perturb-woodland', help='How much (in %) to perturb '
                      'the minimum woodland threshold',
                      type=float, default=0.0)

  default_per_run_interm_template = (
    '%(behaviorspace-name)s-%(run number)06d_PerRunData.csv')
  default_per_year_interm_template = (
    '%(behaviorspace-name)s-%(run number)06d_PerYearData.csv')
  parser.add_argument(
    '--per-run-interm-template', default=default_per_run_interm_template,
    help='Output filename template for PER-RUN processed results')
  parser.add_argument(
    '--per-year-interm-template', default=default_per_year_interm_template,
    help='Output filename template for PER-YEAR processed results')

  parser.add_argument('--overwrite', action="store_true",
                      default=False, help='Overwrite existing output file')

  args = parser.parse_args(argv)

  threshold_errors = []
  args.min_cows_threshold = args.min_cows + random.randrange(
    args.min_cows - args.perturb_cows, args.min_cows + args.perturb_cows)
  if args.min_cows - args.perturb_cows < 0:
    threshold_errors.append("min cow threshold could be negative")
  args.min_harvest_threshold = args.min_cows + random.uniform(
    args.min_harvest * (1.0 - .01 * args.perturb_harvest),
    args.min_harvest * (1.0 + .01 * args.perturb_harvest))
  if args.perturb_harvest > 100.0:
    threshold_errors.append("min harvest threshold could be negative")
  args.min_woodland_threshold = args.min_cows + random.uniform(
    args.min_woodland * (1.0 - .01 * args.perturb_woodland),
    args.min_woodland * (1.0 + .01 * args.perturb_woodland))
  if args.perturb_woodland > 100.0:
    threshold_errors.append("min woodland threshold could be negative")
  if threshold_errors:
    for e in threshold_errors:
      print "ERROR: %s" % e
    sys.exit(1)
  if (args.perturb_cows != 0 or args.perturb_harvest != 0 or
      args.perturb_woodland != 0):
    print "INFO: min cows threshold = %d" % args.min_cows_threshold
    print "INFO: min harvest threshold = %f" % args.min_harvest_threshold
    print "INFO: min woodland threshold = %f" % args.min_woodland_threshold

  args.cluster_dir = os.path.abspath(args.cluster_dir)
  if args.intermediate_dir is None:
    args.intermediate_dir = os.path.join(args.cluster_dir, "../intermediate")
  args.intermediate_dir = os.path.abspath(args.intermediate_dir)

  if args.stage == "autodetect":
    if not os.path.exists(args.intermediate_dir):
      print "INFO: %r doesn't exist, running all stages" % args.intermediate_dir
      args.stage = 'all'
    elif len([f for f in os.listdir(args.intermediate_dir)
              if f.endswith('.csv')]) == 0:
      print "INFO: No CSVs in %r, running all stages" % args.intermediate_dir
      args.stage = 'all'
    else:
      print "INFO: Intermediate data found, only running int-to-final stage"
      args.stage = 'int-to-final'

  if args.output_file is None:
    args.output_file = os.path.join(
      args.cluster_dir, "..", time.strftime("%Y-%m-%d_SummarizedData.csv"))
    if (os.path.exists(args.output_file) and not args.overwrite and
        args.stage in ("int-to-final", "all")):
      print ("ERROR: File %r already exists!\n  (use --overwrite to overwrite)"
             % args.output_file)
      sys.exit(1)

  return args


def verify_tests_pass_and_get_filenames(args):
  rst_file_basename = "RunSoftwareTests.dat"
  rst_filename = os.path.join(args.cluster_dir, rst_file_basename)
  if not os.path.exists(rst_filename):
    print "WARNING: %r does not exist, skipping check" % rst_filename
  else:
    with open(rst_filename) as f:
      behaviorspace_header_line = f.readline()
      nlogo_filename_line = f.readline()
      behaviorspace_name = f.readline()
      date_and_time = f.readline()
      pxycor_headers = f.readline()
      pxycor_data = f.readline()

      dr = csv.DictReader(f)
      for row in dr:
        # only one row
        tests_run = int(row["number-of-tests-run"])
        if tests_run < 10:
          print "ERROR: %s: only %d tests run!" % (rst_file_basename, tests_run)
          if not args.ignore_test_failure:
            sys.exit(1)
        error_count = int(row["error-count"])
        if error_count > 0:
          print "ERROR: %s: %d errors in %d tests!" % (
            rst_file_basename, error_count, tests_run)
          if not args.ignore_test_failure:
            sys.exit(1)
    print "INFO: Software tests passed (%s)" % rst_filename

  datafiles = [os.path.join(args.cluster_dir, fname)
               for fname in os.listdir(args.cluster_dir)
               if fname.endswith(".dat") and fname != rst_file_basename]
  total_size = sum([os.stat(fname).st_size for fname in datafiles])
  print "INFO: %d raw data files (total size = %.3f GB)." % (
    len(datafiles), total_size / float(1 << 30))

  return datafiles


def make_intermediate_files(args, filenames):
  ids_and_filenames = []
  print "INFO: Making intermediate files from %d raw source files" % len(filenames)
  all_per_run_data, all_per_year_data = {}, {}
  for filename in filenames:
    per_run_data, per_year_data = read_raw_file(filename)
    all_per_run_data.update(per_run_data)
    all_per_year_data.update(per_year_data)
    ids_files = write_intermediate_data(args, per_run_data, per_year_data)
    ids_and_filenames.extend(ids_files)

  with open(os.path.join(args.intermediate_dir, "INDEX"), "w") as f:
    f.write("Run ID,PerRunDataFile,PerYearDataFile\n")
    for id_str, per_run_filename, per_year_filename in ids_and_filenames:
      f.write("%s,%s,%s\n" % (id_str, per_run_filename, per_year_filename))

  return all_per_run_data, all_per_year_data


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


def read_raw_file(filename):
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

      run_num = int(row["[run number]"])
      run_id = "%s-%06d" % (cluster_file_data["behaviorspace-name"], run_num)
      if run_id not in per_run_data:
        per_run_data[run_id] = cluster_file_data.copy()
        per_run_data[run_id].update(
          extract_dict_from_row(row, PER_RUN_FIELDS))
        per_run_data[run_id]["Run ID"] = run_id
      per_year_data.setdefault(run_id, {})
      per_year_data[run_id][int(row["calendar-year"])] = (
        extract_dict_from_row(row, PER_YEAR_FIELDS))
    # End per-CSV-row processing

  # File is now closed
  return per_run_data, per_year_data


def write_intermediate_data(args, per_run_data, per_year_data):
  intermediate_dir = args.intermediate_dir
  if not os.path.exists(intermediate_dir):
    os.mkdir(intermediate_dir)

  id_filenames_list = []
  print "INFO: Writing intermediate files (%d run IDs)" % len(per_run_data)
  for run_id in sorted(per_run_data.keys()):
    per_run_filename = os.path.join(
      intermediate_dir, args.per_run_interm_template % per_run_data[run_id])
    per_year_filename = os.path.join(
      intermediate_dir, args.per_year_interm_template % per_run_data[run_id])


    with open(per_run_filename, "w") as per_run:
      per_run_csv = csv.DictWriter(per_run, fieldnames=PER_RUN_OUTPUT_FIELDS)
      per_run_csv.writeheader()
      per_run_csv.writerow(per_run_data[run_id])


    with open(per_year_filename, "w") as per_year:
      per_year_csv = csv.DictWriter(per_year, fieldnames=PER_YEAR_OUTPUT_FIELDS)
      per_year_csv.writeheader()
      for year in sorted(per_year_data[run_id].keys()):
        data = per_year_data[run_id][year]
        per_year_csv.writerow(data)

    id_filenames_list.append((run_id, per_run_filename, per_year_filename))

  return id_filenames_list


def read_intermediate_files(args):
  filedata = {}
  with open(os.path.join(args.intermediate_dir, "INDEX")) as f:
    dr = csv.DictReader(f)
    for row in dr:
      tempdict = dict(row)
      run_id = tempdict.pop("Run ID")
      filedata[run_id] = tempdict

  per_run_data, per_year_data = {}, {}
  for run_id in filedata:
    per_run_data[run_id] = {"Run ID": run_id}
    per_run_file = os.path.join(args.intermediate_dir,
                                filedata[run_id]["PerRunDataFile"])
    with open(per_run_file) as f:
      dr = csv.DictReader(f)
      for row in dr:
        # only one row in a PerRunData file
        per_run_data[run_id].update(row)

    per_year_data[run_id] = {}
    per_year_file = os.path.join(args.intermediate_dir,
                                 filedata[run_id]["PerYearDataFile"])
    with open(per_year_file) as f:
      dr = csv.DictReader(f)
      for row in dr:
        per_year_data[run_id][int(row["calendar-year"])] = dict(row)

  return per_run_data, per_year_data


def run_summary_data_from_per_year_data(args, per_run_data, per_year_data):
  min_cow_count, total_cows, max_cow_count = None, 0, None
  min_harvest, total_harvest, max_harvest = None, 0, None
  min_woodland, total_woodland, max_woodland = None, 0, None
  n_years = 0
  max_crop_eaten = 0
  total_crop_eaten = 0
  total_births = 0
  total_cows_in_crops = 0
  termination_reason = "end of simulation"

  years_in_run = sorted(per_year_data.keys())
  for year in years_in_run:
    if year == "0": continue
    n_years += 1

    cows = int(per_year_data[year]["count cows"])
    if min_cow_count is None or cows < min_cow_count: min_cow_count = cows
    if max_cow_count is None or cows > max_cow_count: max_cow_count = cows
    total_cows += cows

    if per_run_data["how-long-to-store-grain"] == 0:
      harvest = float(per_year_data[year]["current-harvest"])
    else:
      harvest = float(per_year_data[year]["mean previous-harvests-list"])

    if min_harvest is None or harvest < min_harvest: min_harvest = harvest
    if max_harvest is None or harvest > max_harvest: max_harvest = harvest
    total_harvest += harvest

    woodland = float(per_year_data[year]["total-woodland-biomass"])
    if min_woodland is None or woodland < min_woodland: min_woodland = woodland
    if max_woodland is None or woodland > max_woodland: max_woodland = woodland
    total_woodland += woodland

    crop_eaten = float(per_year_data[year]["crop-eaten"])
    if crop_eaten > 0:
      total_crop_eaten += crop_eaten * 1000  # convert metric tons to kg
      fraction_crop_eaten = crop_eaten / (crop_eaten + harvest)
      if fraction_crop_eaten > max_crop_eaten:
        max_crop_eaten = fraction_crop_eaten

    if cows < args.min_cows_threshold:
      termination_reason = "cow threshold"
      break
    if harvest < args.min_harvest_threshold:
      termination_reason = "harvest threshold"
      break
    if woodland < args.min_woodland_threshold:
      termination_reason = "woodland threshold"
      break

  # Retrieve several cumulative values from the final year's data.
  # (total-number-of-births for year Y is the sum of the number of births
  #  in all years up through and including Y, and similarly for
  #  count-cows-in-crops)
  final_year = years_in_run[-1]
  final_year_timer = float(per_year_data[final_year]["timer"])
  subsidy_used = float(per_year_data[final_year]["subsidy-used"])
  total_births = int(per_year_data[final_year]["total-number-of-births"])
  total_cows_in_crops = int(per_year_data[final_year]["count-cows-in-crops"])

  # total_cows_in_crops is measured in cows*ticks
  # (3 ticks/day, so 1 tick = 8 hours = 16 half-hours)
  # We want to convert to cows*ticks to cows*half-hours
  total_cows_in_crops = float(total_cows_in_crops) * 16

  return {
    "min-cow-count": min_cow_count,
    "mean-cow-count": None if n_years == 0 else float(total_cows) / n_years,
    "max-cow-count": max_cow_count,
    "min-harvest": min_harvest,
    "mean-harvest": None if n_years == 0 else total_harvest / n_years,
    "max-harvest": max_harvest,
    "total-harvest": total_harvest,
    "min-woodland-biomass": min_woodland,
    "mean-woodland-biomass": None if n_years == 0 else total_woodland / n_years,
    "max-woodland-biomass": max_woodland,

    "max-percent-crop-eaten": 100.0 * max_crop_eaten,
    "actual-cow-repro-rate": None if total_cows == 0 else float(total_births) / total_cows,
    "crop-eaten-per-half-hour-per-cow": (
      None if not total_cows_in_crops else
      total_crop_eaten / total_cows_in_crops),
    "subsidy-used": subsidy_used,
    "end-year": year,
    "final-year-timer": final_year_timer,
    "termination-reason": termination_reason,
    "min-cows-threshold": args.min_cows_threshold,
    "min-harvest-threshold": args.min_harvest_threshold,
    "min-woodland-threshold": args.min_woodland_threshold,
  }


def write_final_data(args, per_run_data, per_year_data):
  if os.path.exists(args.output_file) and not args.overwrite:
    print ("ERROR: File %r already exists!\n  (use --overwrite to overwrite)"
           % args.output_file)
    sys.exit(1)

  print "INFO: Writing summary output to %r" % args.output_file
  with open(args.output_file, "w") as outf:
    dw = csv.DictWriter(outf, fieldnames=SUMMARY_FIELDS)
    dw.writeheader()
    for run_id in sorted(per_run_data.keys()):
      data = {k: v for k, v in per_run_data[run_id].items()
              if k not in EXCLUDE_FROM_SUMMARY}
      data.update(run_summary_data_from_per_year_data(
        args, per_run_data[run_id], per_year_data[run_id]))
      dw.writerow(data)


def main():
  args = parse_cmdline(sys.argv[1:])
  per_run_data, per_year_data = None, None

  # Run first stage, if requested.
  if args.stage in ('raw-to-int', 'all'):
    datafiles = verify_tests_pass_and_get_filenames(args)
    per_run_data, per_year_data = make_intermediate_files(args, datafiles)

  # Run second stage, if requested.
  # Read in per_run_data/per_year_data from files, if we don't have them from
  #   running the first stage.
  if args.stage in ('int-to-final', 'all'):
    if (per_run_data, per_year_data) == (None, None):
      per_run_data, per_year_data = read_intermediate_files(args)
    write_final_data(args, per_run_data, per_year_data)


if __name__ == '__main__': main()
