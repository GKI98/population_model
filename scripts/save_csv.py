import sys
from typing import Any, Iterator, Sequence

import pandas as pd
import glob
from more_itertools import sliced
import os
import shutil
import time
from tqdm import tqdm


class Saver:
    @staticmethod
    def chunking(df) -> Iterator[Sequence[Any]]:
        """Разбиение df на много маленьких df"""
        chunk_size = 20000
        index_slices = sliced(range(len(df)), chunk_size)

        return index_slices

    @staticmethod
    def cat(folder_name, name: str = 'data'):
        """Собрать кучу маленьких файлов в один"""
        
        os.chdir('./tmp_data_files')
        os.system(f'cat * > {name}.csv')
        time.sleep(60)
        os.system(f'mv {name}.csv ../{folder_name}')
        os.chdir('../')
        shutil.rmtree(f'./tmp_data_files')

    @staticmethod
    def df_to_csv(df, id, folder_name) -> None:
        folder_path = 'tmp_data_files'

        if not os.path.exists('./' + folder_path):
            os.mkdir(folder_path)

        header_path = f'./{folder_path}/0_header.csv'

        if not os.path.isfile(header_path):
            header = pd.DataFrame(df.columns).T
            header.to_csv(header_path, header=False, index=False)

        index_slices = Saver.chunking(df)

        for counter, index_slice in enumerate(tqdm(index_slices)):
            tmp_data_path = f'./{folder_path}/data_{id}_{counter}.csv'
            chunk = df.iloc[index_slice]
            chunk.to_csv(tmp_data_path, header=False, index=False)

        os.chdir(f'{os.getcwd()}/{folder_path}')
        if not os.path.exists(f'../{folder_name}'):
            os.mkdir(f'../{folder_name}')
        os.chdir('../')


if __name__ == '__main__':
    pass
