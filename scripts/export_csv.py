"""Convert GoogleSheets source into a 1day=1row csv that can be easily used
for analysis"""
import os
import sys
from argparse import ArgumentParser
from pathlib import Path
from typing import Dict

import pandas as pd


ROOT_DIR = str(Path(Path(__file__).parent.absolute()).parent.absolute())
# noinspection PyBroadException
try:
    from ohbehave.data.transforms.data_by_date import data_by_date, sleep_summary_stats
except Exception:
    sys.path.insert(0, ROOT_DIR)
    from ohbehave.data.transforms.data_by_date import data_by_date


# TODO: if googlesheets token hasn't been refreshed recently enough, will give an error. i should handle exception and
#  ...remind user to simply run again
def export_csv(
    outdir=ROOT_DIR, modality='by_date', exclude_gaming_data=False, exclude_alcohol_data=False,
    exclude_sleep_data=False, ignore_gsheets_cache=False, verbose=False
):
    """Export csv: main function"""
    name_stub = ('' + ' sans_gaming' * exclude_gaming_data + ' sans_alcohol' * exclude_alcohol_data + ' sans_sleep' *
                 exclude_sleep_data).strip()
    name_stub = ' - ' + name_stub if name_stub else ''
    if modality == 'by_date':
        data_outpath = os.path.join(outdir, 'data' + name_stub + '.csv')
        df: pd.DataFrame = data_by_date(
            exclude_gaming_data, exclude_alcohol_data, exclude_sleep_data, ignore_gsheets_cache, verbose)
        df.to_csv(data_outpath, index=False)
        if not exclude_sleep_data:
            sleep_summary_stats_df: pd.DataFrame = sleep_summary_stats(df)
            sleep_summary_stats_df.to_csv(os.path.join(outdir, 'summary-stats - sleep.csv'), index=False)
    else:
        msg = f'Unknown modality: {modality}'
        raise NotImplementedError(msg)


def cli():
    """Command line interface."""
    parser = ArgumentParser(
        prog='Export personal data CSV', description='Personal data from GoogleSheets raw data to curated CSV.')
    parser.add_argument(
        '-g', '--exclude-gaming-data', required=False, action='store_true', help='Exclude gaming data from results?')
    parser.add_argument(
        '-a', '--exclude-alcohol-data', required=False, action='store_true', help='Exclude alcohol data from results?')
    parser.add_argument(
        '-s', '--exclude-sleep-data', required=False, action='store_true', help='Exclude sleep data from results?')
    parser.add_argument(
        '-i', '--ignore-gsheets-cache', required=False, action='store_true',
        help='Ignore default caching mechanism of download of raw data from GoogleSheets. Will always download if '
             'this flag is present.')
    parser.add_argument(
        '-v', '--verbose', required=False, action='store_true', help='Print extra info and warning statements?')
    d: Dict = vars(parser.parse_args())
    export_csv(**d)


if __name__ == '__main__':
    cli()
    print('Export CSV: Done')
