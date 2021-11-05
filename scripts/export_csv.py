"""Convert GoogleSheets source into a 1day=1row csv that can be easily used
for analysis"""
import os
import sys
from pathlib import Path

import pandas as pd

ROOT_DIR = str(Path(Path(__file__).parent.absolute()).parent.absolute())
sys.path.insert(0, ROOT_DIR)
from ohbehave.data.transforms.data_by_date import data_by_date


def export_csv(
    path=os.path.join(ROOT_DIR, 'data_by_date.csv'),
    modality='by_date'
):
    """Export csv: main function"""
    if modality == 'by_date':
        df: pd.DataFrame = data_by_date()
        df.to_csv(path, index=False)
    else:
        msg = f'Unknown modality: {modality}'
        raise NotImplementedError(msg)


if __name__ == '__main__':
    export_csv()
