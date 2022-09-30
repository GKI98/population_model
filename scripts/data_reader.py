from typing import Tuple

import pandas as pd

class DataReader:
    def get_data(self) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        '''DataFrames are returned in order `adm_age_sex_df`, `adm_total_df`, `houses_df`, `mun_age_sex_df`, `mun_total_df`, `soc_adm_age_sex_df`'''
        raise NotImplementedError('DataReader is an abstract class')

