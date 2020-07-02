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
from textwrap import wrap
import datetime as dt
from io import StringIO

# Third-party imports
import pke
import ujson as json
from nltk.corpus import stopwords
from tinydb import TinyDB, Query
from markdown import Markdown
from progress.bar import Bar

# Globals
stdoutOnly = False


def unmark_element(element, stream=None):
    if stream is None:
        stream = StringIO()
    if element.text:
        stream.write(element.text)
    for sub in element:
        unmark_element(sub, stream)
    if element.tail:
        stream.write(element.tail)
    return stream.getvalue()


# patching Markdown
Markdown.output_formats["plain"] = unmark_element
__md = Markdown(output_format="plain")
__md.stripTopLevelTags = False


def unmark(text):
    return __md.convert(text)


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


def extract(corpus):
    extractor = pke.unsupervised.YAKE()
    extractor.load_document(input=corpus, language="en", normalization=None)

    stoplist = stopwords.words("english")
    extractor.candidate_selection(n=3, stoplist=stoplist)
    extractor.candidate_weighting(stoplist=stoplist)
    keyphrases = extractor.get_n_best()

    return keyphrases


def extract_blob(text):
    phrases = extract(text)
    writeJson(phrases)


def extract_file(fpath):
    if not os.path.isfile(fpath):
        print("\n! File {0} not found.".format(fpath))
        exit()

    db = TinyDB(fpath, indent=4)
    if not Query().subreddit.exists():
        print("\n! Incorrect database type. Exiting.")
        exit()

    posts_table = db.table("posts")
    comments_table = db.table("comments")

    corpus = ""

    with Bar(
        "Building corpus for extraction", max=len(posts_table) + len(comments_table)
    ) as bar:
        for document in posts_table.all():
            if document["text"] != "":
                corpus += unmark(document["text"]).replace("\n", " ").lower() + " "
            bar.next()

        for document in comments_table.all():
            if document["upvotes"] > 0 and document["text"] != "":
                corpus += unmark(document["text"]).replace("\n", " ").lower() + " "
            bar.next()

    chunks = wrap(corpus, 999999, break_long_words=False)

    phrases = []
    with Bar("Extracting phrases from chunks", max=len(chunks)) as bar:
        for x in chunks:
            phrases += extract(x)
            bar.next()

    writeJson(phrases)


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
        if args.t == "-" and not sys.stdin.isatty():
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
