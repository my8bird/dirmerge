#!/usr/bin/env python

import itertools
import time
import os
import glob2
import shutil
import signal
import sys

from base64      import b32encode
from collections import defaultdict
from hashlib     import md5
from argparse    import ArgumentParser
from os.path     import isdir, abspath, expandvars, splitext, join as pj

from twisted.internet         import reactor
from twisted.internet.threads import deferToThread
from twisted.internet.defer   import inlineCallbacks, returnValue, DeferredList


def parseArgs():
   parser = ArgumentParser()

   #parser.add_argument('directory', nargs = '*',
   #                    help = "Directories to search through.")

   parser.add_argument('--threads', default = 1, type = int,
                       help =
                       'Number of threads to use (controls how fast we read/write from disk).  [%(default)s]')

   parser.add_argument('--list-files', action='store_true', default = False,
                       help = 'List the files which would be checked (src files)')

   parser.add_argument('--search-glob', action = 'append',
                       help = "Glob to use to find files.  [%(default)s]")

   parser.add_argument('--dest',
                       help = "The directory to put files into.  When omitted just the statistics are printed.")

   return parser.parse_args()


def buildListOfFiles(searchGlob):
   """
   Build a master list of all files that we should check.
   """
   return [fpath for fpath in glob2.iglob(searchGlob) if os.path.isfile(fpath)]


def hashfile(filepath, blocksize = 65536):
   hasher = md5()

   with open(filepath, 'rb') as f:
      buf = f.read(blocksize)
      while len(buf) > 0:
         hasher.update(buf)
         buf = f.read(blocksize)

   file_hash = hasher.digest()
   safe_val = b32encode(file_hash)
   return (filepath, safe_val)


def progressPrinter(deferreds, frmt):
   # Wait for all of the files to finish processing
   # - Get some output as they finish
   def showProgress(val):
      num_fin = len([1 for d in deferreds if d.called])
      total = len(deferreds)
      text  = frmt % (num_fin, total, num_fin / total)

      sys.stdout.write('\r' + text + '                   ')
      return val

   return showProgress



@inlineCallbacks
def computeFileHashes(files):
   # Request the hash for all of the files
   # - The thread pool size will limit how many files are run in parrell
   # - The result of hashfile will be a tuple of (filepath, hash)
   hash_deferreds = [deferToThread(hashfile, fpath) for fpath in files]

   progress_printer = progressPrinter(hash_deferreds, 'Generating Hashes: %d/%d %2f')
   for deferred in hash_deferreds:
      deferred.addCallback(progress_printer)

   results = yield DeferredList(hash_deferreds, fireOnOneErrback = True)
   results = [val for status, val in results if status]
   # Convert to map (hash -. [fpath ...] )
   hash_to_matching_files = defaultdict(list)
   for fpath, file_hash in results:
      hash_to_matching_files[file_hash].append(fpath)

   returnValue(hash_to_matching_files)


def copyFile(src, dest):
   #print 'copy %s -> %s' % (src, dest)
   shutil.copy2(src, dest)


@inlineCallbacks
def copyFiles(destPath, filesByHash):
   print "Copying files ..."
   dest_dir = abspath(expandvars(destPath))

   if not isdir(dest_dir):
      os.makedirs(dest_dir)

   # Hash the dest dir contents
   # - This way we only copy files for NEW ones
   dest_files = [pj(dest_dir, fname) for fname in os.listdir(dest_dir)]
   dest_files_by_hash = yield computeFileHashes(dest_files)

   build_path = lambda old, sig: pj(dest_dir, '%s%s' % (sig, splitext(old)[1]))
   deferreds = [deferToThread(copyFile, src_paths[0], build_path(src_paths[0], sig))
                for sig, src_paths in filesByHash.viewitems()
                if sig not in dest_files_by_hash]

   progress_printer = progressPrinter(deferreds, 'Copied %d/%d %2f')
   for d in deferreds:
      d.addCallback(progress_printer)

   yield DeferredList(deferreds)


@inlineCallbacks
def asyncMain(args):
   try:
      files_for_globs = [buildListOfFiles(sg) for sg in args.search_glob]
      files = set(itertools.chain(*files_for_globs))
      if args.list_files:
         for f in files:
            print f

      print 'Found %d files' % len(files)

      if args.list_files:
         returnValue(None)

      files_by_hash = yield computeFileHashes(files)
      print 'Found %d unique files' % len(files_by_hash)

      if args.dest:
         yield copyFiles(args.dest, files_by_hash)
   except Exception as ex:
      ex.printTraceback()
   finally:
      reactor.stop()


if __name__ == '__main__':
   def customHandler(signum, stackframe):
      print "Got signal: %s" % signum
      reactor.callFromThread(reactor.stop)
   signal.signal(signal.SIGINT, customHandler)

   args = parseArgs()
   reactor.suggestThreadPoolSize(args.threads)

   reactor.callWhenRunning(asyncMain, args)
   reactor.run()
