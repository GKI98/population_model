import os
import random

import pandas as pd
from more_itertools import sliced


class Saver:

    def __init__(self, output_path):
        if not os.path.exists(output_path):
            os.makedirs(output_path)
        self.output_path = output_path
        tmp_folder_name = f'tmp_{random.randint(0, 1 << 31)}'
        while os.path.exists(os.path.join(output_path, tmp_folder_name)):
            tmp_folder_name = f'tmp_{random.randint(0, 1 << 31)}'
        os.mkdir(os.path.join(output_path, tmp_folder_name))
        self.tmp_folder_name = tmp_folder_name

    def save(self, filename: str) -> None:
        with open(os.path.join(self.output_path, filename), 'w', encoding='utf-8') as out_f:
            filename = os.path.join(self.output_path, self.tmp_folder_name, 'header.csv')
            with open(filename, 'r') as f:
                out_f.write(f.read())
            os.remove(filename)
            for filename in sorted(os.listdir(os.path.join(self.output_path, self.tmp_folder_name))):
                filename = os.path.join(self.output_path, self.tmp_folder_name, filename)
                with open(filename, 'r') as f:
                    out_f.write(f.read())
                os.remove(filename)
        os.rmdir(os.path.join(self.output_path, self.tmp_folder_name))

    def df_to_csv(self, df: pd.DataFrame, id: str) -> None:
        header_path = os.path.join(self.output_path, self.tmp_folder_name, 'header.csv')

        if not os.path.isfile(header_path):
            header = pd.DataFrame(df.columns).T
            header.to_csv(header_path, header=False, index=False)

        chunk_size = 10000
        index_slices = sliced(range(len(df)), chunk_size)

        for counter, index_slice in enumerate(index_slices):
            tmp_data_path = os.path.join(self.output_path, self.tmp_folder_name, f'{id}_{counter}.csv')
            chunk = df.iloc[index_slice]
            chunk.to_csv(tmp_data_path, header=False, index=False)
