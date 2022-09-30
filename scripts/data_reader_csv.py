import os
from typing import Tuple
from loguru import logger

import pandas as pd

from scripts.data_reader import DataReader


class CSVReader(DataReader):

    needed_files = ['outer_territories_age_sex', 'outer_territories_total', 'houses',
            'inner_territories_age_sex', 'inner_territories_total', 'outer_territories_soc_age_sex']

    def __init__(self, folder_path):
        self.folder_path = folder_path
        
    def get_data(self) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        filenames = sorted([f for f in os.listdir(self.folder_path) if f[:-4] in CSVReader.needed_files])
        if len(filenames) != len(CSVReader.needed_files):
            logger.error('Given input folder "{}" contgains only {} files from needed {}', self.folder_path, len(filenames), len(CSVReader.needed_files))

        dfs = {}
        for filename in filenames:
            dfs[filename] = pd.read_csv(filepath_or_buffer=os.path.join(self.folder_path, filename))


        return (dfs['outer_territories_age_sex'], dfs['outer_territories_total'], dfs['houses'],
                dfs['inner_territories_age_sex'], dfs['inner_territories_total'], dfs['outer_territories_soc_age_sex'])
