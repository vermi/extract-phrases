#!/usr/bin/env python3
"""Key Phrase Extractor for Reddit Data Gatherer:

This script will take either a single text blob or a json output file from the Reddit
 Data Gatherer and attempt to extract key phrases. You can also pipe text directly to
 this script.

Output will be either a JSON file, or direct to stdout.

A keyboard interrupt (Ctrl-C) can be used to stop the extraction process and immediately
 write any collected information to disk.
"""

# Standard imports
import os.path
import argparse
import signal
import sys
import datetime as dt
from io import StringIO

# Third-party imports
import pke
import ujson as json
from nltk.corpus import stopwords
from progress.bar import Bar

# Globals
stdoutOnly = False


def now():
    return dt.datetime.now().isoformat()


def writeJson(output):
    if stdoutOnly:
        print("\n! No JSON file will be written.\n")
        print(json.dumps(output, indent=4), "\n")
    else:
        filename = "./data/{0}_{1}_{2}.json"
        filename = filename.format("textblob", "phrase_extraction", now())

        print("\n* Writing to file", filename, end=" ... ", flush=True)
        with open(filename, "w") as fp:
            fp.write(json.dumps(output, indent=4))

        print("Write complete.\n")


def sigintHandler(signal, frame):
    print("\n\n! SIGINT RECEIVED -- bailing")
    sys.exit(0)


def extract_blob(text):
    result = {
        "words": len(text.split()),
    }
    writeJson(result)


def extract_file(fileName):
    pass


def main():
    parser = argparse.ArgumentParser(description=__doc__)

    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "-t",
        help="A text blob to analyze, use - if piping. This should be big.",
        metavar="TEXT",
    )
    group.add_argument(
        "-f", help="Path to properly-formatted JSON file for analysis.", metavar="FILE"
    )

    parser.add_argument(
        "--stdout",
        dest="stdoutOnly",
        action="store_true",
        help="Specify this option to print gathered data to STDOUT instead of a JSON file. Only supports text input.",
    )

    args = parser.parse_args()

    global stdoutOnly
    stdoutOnly = args.stdoutOnly

    if args.t:
        if args.t == '-' and not sys.stdin.isatty():
            extract_blob(sys.stdin.read())
        else:
            extract_blob(args.t)
    elif args.f:
        extract_file(args.f)
    else:
        raise Exception("Something went terribly wrong. So sorry.")


if __name__ == "__main__":
    signal.signal(signal.SIGINT, sigintHandler)
    main()
