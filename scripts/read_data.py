# 1

from scripts.read_csv import CSVReader
from scripts.read_db import DBReader


def main(args):

    if args.read:
        return CSVReader.read_csv(args.path)
    else:
        return DBReader.get_from_db(args)

