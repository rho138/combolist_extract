#!/bin/python3
import os
import re
import shlex
import sys
from argparse import ArgumentParser
from tqdm import tqdm
from subprocess import run
from multiprocessing import Pool as pool
from multiprocessing import cpu_count
from tempfile import mkstemp
from shutil import rmtree

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
  rgx = re.compile(r'(?:\.?)([\w\-_+#~!$&\'\.]+(?<!\.)(@|[ ]\(?[ ]?(at|AT)[ ]?\)?[ ])(?<!\.)[\w]+[\w\-\.]*\.[a-zA-Z-]{2,3})(?:[^\w])')

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

  # Split out our gp_list into food/bad
  fp_list = dump_filepaths('%s/temp_out/' % (path))
  gl = []
  bl = []
  for f in fp_list:
    if re.search('good_', f):
      gl.append(f)
    else:
      bl.append(f)

  # Create our vars for outputs
  mad = '%s/mad_out.txt' % (path)
  bad = '%s/bad_out.txt' % (path)

  # purge the data file if it's there
  if os.path.exists(mad):
    os.unlink(mad)
  if os.path.exists(bad):
    os.unlink(bad)

  # open data files
  a = open(mad, 'w')
  b = open(bad, 'w')

  # iterate our good list
  print('!! Starting merge and dump of Good Files:')
  pbar = tqdm(total=len(gl))
  for file in gl:
    with open(file, 'r') as fh:
      for line in fh:
        a.write(line)
    os.unlink(file)
    pbar.update(1)

  # iterate our bad list
  print('!! Starting merge and dump of Good Files:')
  pbar = tqdm(total=len(bl))
  for file in bl:
    with open(file, 'r') as fh:
      for line in fh:
        b.write(line)
    os.unlink(file)
    pbar.update(1)

  # Close our file handles
  a.close()
  b.close()

def run_su(my_args):
  # Get our user args
  path = my_args.path
  cores = my_args.cores

  # Create our vars for outputs
  run_sug = '%s/run_su_good.txt' % (path)
  run_sub = '%s/run_su_bad.txt' % (path)

  # purge old data
  if os.path.exists(run_sug):
    os.unlink(run_sug)
  if os.path.exists(run_sub):
    os.unlink(run_sub)
  files = ['%s/mad_out.txt' % (path), '%s/mad_bad.txt' % (path)]

  # run through our main file, getting a count of domains seen
  cmd = 'LC_ALL=C sort --parallel=%i -u %s && LC_ALL=C cut -f 2 -d '@' %s |\
    LC_ALL=C uniq -c >> %s' % (cores, files[0], files[0], run_sug)
  os.unlink(files[0])
  print('!! Running command: %s\n!!   No Progress bar' % (cmd))
  run(shlex.split(cmd))

  # run through our garbage container and get a count for the bad data
  #   NOTE: may remove count if it's not worthwhile.
  cmd = 'LC_ALL=C sort --parallel=%i -u %s' \
    % (cores, files[1], files[1], run_sub)
  print('!! Running command: %s\n!!   No Progress bar' % (cmd))
  run(shlex.split(cmd))
  os.unlink(files[1])



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
  merge_and_dump()
  run_su()


if __name__ == '__main__':
  args = ArgumentParser()
  args.add_argument('--path', '-p', type=str, dest='path', \
    help="Set the path, else use pwd. Target path must have 'data' folder", default='.')
  args.add_argument('--cores', '-c', type=int, dest='cores',
    help="Set max cores, else set to cpu_count-1", default=cpu_count()-1)
  my_args = args.parse_args()
  main(my_args)
