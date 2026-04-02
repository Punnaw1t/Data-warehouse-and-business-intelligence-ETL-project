# cli.py

'''This module provides the custom tool etl's CLI features.
The tools allows us to call different etl jobs based on input from the user
'''

import argparse
import pathlib
import sys
from . import __version__
from etl_jobs import EtlJobForMajor


def parse_cmd_line_arguments():
    parser = argparse.ArgumentParser(prog = "etl Major", description = "A custom etl tool that allows us to run etl jobs on the top of pyspark and other frameworks", epilog = "Thanks for using etl Major tool!")
    parser.version = f"etl Major v{__version__}"

    #  ------------------------------------------------ version
    parser.add_argument("-v", "--version", action = "version")

    #  ------------------------------------------------ destination
    parser.add_argument(
        "-dest",
        "--destination",
        metavar = "DESTINATION",
        nargs = "?",
        help = "Path to the output `parquet` file, which will contain the transformed data. "
    )

    #  ------------------------------------------------ database
    parser.add_argument(
        "--database",
        metavar = "DATABASE",                                       # Alternate display name for the argument as shown in help
        nargs = "?",                                                # number or args, ? for the optional
        help = "Name of the database, which will contain the transformed data. "
    )

    #  ------------------------------------------------ table
    parser.add_argument(
        "--table",
        metavar = "TABLE",
        nargs = "?",
        help = "Name of the database, which will contain the transformed data. "
    )

    #  ------------------------------------------------ source
    parser.add_argument(
        "-src",
        "--source",
        metavar = "SOURCE",
        nargs = "?",
        default = sys.stdout,
        help = "Path to the source `csv` file. "
    )

    # ADD NEW ARGUMENTS ABOVE
    #  ------------------------------------------------
    return parser.parse_args()

def main():
    args = parse_cmd_line_arguments()
    """EtlJobForMajor is a class that will create an instance of job to be run"""
    etl_job_for_major = EtlJobForMajor(source=args.source,
                                       destination=args.destination,
                                       database=args.database,
                                       table=args.table)
    etl_job_for_major.run()

