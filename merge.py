#!/bin/python3
import os
import re
import shlex
import sys
import stat
from argparse import ArgumentParser
from tqdm import tqdm
from subprocess import check_call
from multiprocessing import Pool as pool
from multiprocessing import cpu_count
from tempfile import mkstemp
from shutil import rmtree, copyfileobj

def dump_filepaths(path):
  if not os.path.isdir(path):
    print('The path provided is not a directory, quitting: %s' % (path))
    sys.exit(1)
  fp_list = []
  for subdir, dirs, files in os.walk(path):
      for filename in files:
          filepath = subdir + os.sep + filename
          if os.path.isfile(filepath):
            fp_list.append(filepath)
  return fp_list


def get_lines(filepath):
  # regex compile doesn't really matter, but primes the cache table and has pretty var
  # https://stackoverflow.com/a/52607930
  rgx = re.compile(r'(?:\.?)([\w\-_+#~!$&\'\.]+(@|[ ]\(?[ ]?(at|AT)[ ]?\)?[ ])(?<!\.)[\w]+[\w\-\.]*\.[a-zA-Z-]{2,3})(?:[^\w])')

  # get our file basename
  base = os.path.basename(filepath)

  # Get our base path, so that we write to the user defined path.
  base_path = filepath.partition('data')[0]
  out_dir = '%s/temp_out' % (base_path)

  # Create a temp file in our data dir
  good = mkstemp(prefix='good_%s' % (base), dir=out_dir)[1]
  bad = mkstemp(prefix='bad_%s' % (base), dir=out_dir)[1]

  a = open(good, 'w')
  b = open(bad, 'w')

  with open(filepath) as e:
    for line in e:
      line = line.strip()
      # find all of the email addresses
      match = re.findall(rgx, line)
      # if it's a non-conforming string then we catalog the whole thing
      #   so that we can add in processors for filebeat
      if (
            len(line.split()) > 1 or
            len(match) == 0
        ):
        b.write('%s\n' % (line))
      # write the list of matches to our good file
      for i in match:
        a.write('%s\n' % (i[0]))
  # closing our file handles out
  a.close()
  b.close()


def merge_and_dump(my_args):
  # Get our user args
  path = my_args.path
  s_mem = my_args.s_mem
  cores = my_args.cores

  # Create our vars for outputs
  mad = '%s/mad_out.txt' % (path)
  run_sub = '%s/run_su_bad.txt' % (path)

  # purge the data file if it's there
  if os.path.exists(mad):
    os.unlink(mad)
  if os.path.exists(run_sub):
    os.unlink(run_sub)

  # Create our base regex for sort
  good_fp = '%s/temp_out/good_*' % (path)
  bad_fp = '%s/temp_out/bad_*' % (path)


  # iterate our good list
  cmd = 'export LC_ALL=C; sort -S %i%% --parallel=%i -u -m %s -o %s' \
    % (s_mem, cores, good_fp, mad)
  print('!! Starting merge and dump of Good Files:\n!! %s') % (cmd)
  check_call(shlex.split(cmd))



  # iterate our bad list
  cmd = 'export LC_ALL=C; sort -S %i%% --parallel=%i -u -m %s -o %s' \
    % (s_mem, cores, bad_fp, run_sub)
  print('!! Starting merge and dump of Bad Files:\n!! %s') % (cmd)
  check_call(shlex.split(cmd))

def run_su(my_args):
  # Get our user args
  path = my_args.path
  br_name = my_args.br_name

  # Create our vars for outputs
  run_sug = '%s/run_su_good.txt' % (path)

  # purge old data
  if os.path.exists(run_sug):
    os.unlink(run_sug)
  open(run_sug, 'w').close()
  if os.path.exists(run_sug):
    os.unlink(run_sug)
  open(run_sug, 'w').close()


  files = '%s/mad_out.txt' % (path)

  # Create a bash file to execute
  with open(br_name, 'w') as f:
    # set environment variables for LC_ALL=C
    f.write('export LC_ALL=C\n')

    # Command to capture and count domains
    cmd = 'cut -f 2 -d \'@\' %s | sort | uniq -c > %s ' % (files[0], run_sug)
    f.write('''
echo "!! Running command:"
echo "  !! '%s'"
echo "!! No Progress bar"
''' % (cmd))
    f.write(cmd + '\n')

  cmd = '/bin/bash %s' % (br_name)
  os.chmod(br_name, stat.S_IRWXU)
  check_call(shlex.split(cmd))
  os.unlink(files[0])



def main(my_args):
  # Get our user args
  path = my_args.path
  cores = my_args.cores

  # Gather our list of directories
  fp_list = dump_filepaths('%s/data/' % (path))

  # Create our working directory
  if not os.path.exists('%s/temp_out' % (path)):
    os.mkdir('%s/temp_out' % (path))
  else:
    rmtree('%s/temp_out' % (path))
    os.mkdir('%s/temp_out' % (path))

  # Spawn our temp files that have been split out
  with pool(cores) as p:
    # We're going to create a process bar for this
    print("!! Starting file dumping to %s/temp_out:" % (path))
    for _ in tqdm(p.imap_unordered(get_lines, fp_list), total=len(fp_list)):
      pass
  merge_and_dump(my_args)
  run_su(my_args)


if __name__ == '__main__':
  args = ArgumentParser()
  args.add_argument('--path', '-p', type=str, dest='path', \
    help="Set the path, else use pwd. Target path must have 'data' folder", default='.')
  args.add_argument('--cores', '-c', type=int, dest='cores',
    help="Set max cores, else set to cpu_count-1", default=cpu_count()-1)
  args.add_argument('--bash-run-name', '-br', type=str, dest='br_name', \
    help="Set bash run script name, else name set to ./bash_run_helper.sh", \
    default="./bash_run_helper.sh")
  args.add_argument('--sort-memory', '-S', type=int, dest='s_mem',\
    help="Set the percentage of RAM to use for sort, else 10", default=10)
  my_args = args.parse_args()
  main(my_args)

