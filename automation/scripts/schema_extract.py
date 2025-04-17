#! /usr/bin/env python
'''
This application will extract schema changes from 2 version of schema file

Usage:
    schema_extract.py (-h | --help)
    schema_extract.py <new_version_file> <old_version_file> <output_file>

Options:
    -h, --help          Print this screen and exit.
    <new_version_file>  New schema version file, contains new added fields.
    <old_version_file>  Base schema file.
    <output_file>       The file path which accept schema diff info.
'''
from docopt import docopt
import sys

def main(args):
    print(f'new_version_file: {args["<new_version_file>"]} \t old_version_file: {args["<old_version_file>"]} \t output_file: {args["<output_file>"]}')

if __name__ == "__main__":
    sys.exit(main(docopt(__doc__)))
