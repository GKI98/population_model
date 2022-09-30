from typing import Dict, Literal
import pandas as pd
from loguru import logger
from tqdm import tqdm

from scripts import save_db
from scripts.save_csv import Saver


def houses_soc_to_ages(year: int, set_population: int, scenario: Literal['pos', 'mod', 'neg'],
        houses_soc: pd.DataFrame, mun_soc: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    '''Распределение жителей домов (по соц. группам) по возрастам (0-100)'''

    mun_list = set(houses_soc['municipality_id'])

    logger.info('Начался расчет жителей домов по возрастам среди соц.групп')
    res = {}
    for mun in tqdm(mun_list, desc='Балансировка соц-групп по возрастам'):
        # Разрез по муниципалитетам - чтобы кусочками работать с df и не есть много памяти за раз
        houses_soc_mun = houses_soc.loc[houses_soc['municipality_id'] == mun]
        mun_soc_mun = mun_soc.loc[mun_soc['municipality_id'] == mun]

        df = pd.merge(houses_soc_mun, mun_soc_mun, on=['municipality_id', 'social_group_id'])
        df = df.sort_values(by=['house_id', 'social_group_id'])

        # Кол-во людей в соц.группе в возрасте по полу = кол-во людей в доме * вероятность быть
        # в возрасте в мун в соц группе
        df['men'] = df['men'] * df['mun_percent']
        df['women'] = df['women'] * df['mun_percent']

        # Разбиение по домикам - чтобы балансировать людей по домикам

        # Округление со сходящейся суммой по возрастам для соц.групп в доме
        df = df.drop(['mun_percent', 'municipality_id'], axis=1)

        df['men'] = df['men'].astype(float).round(2)
        df['women'] = df['women'].astype(float).round(2)

        df['men_rounded'] = df['men'].astype(int)
        df['women_rounded'] = df['women'].astype(int)

        df.insert(0, 'year', year)
        df.insert(1, 'set_population', set_population)
        df.insert(2, 'scenario', scenario)

        res[mun] = df

    return res


def main(year: int, set_population: int, scenario: Literal['pos', 'mod', 'neg'], houses_soc: pd.DataFrame, mun_soc: pd.DataFrame):
    logger.info('Началось распределение жителей домов (по соц. группам) по возрастам')

    mun_soc = mun_soc[['municipality_id', 'social_group_id', 'age', 'men', 'women']]

    return houses_soc_to_ages(year, set_population, scenario, houses_soc, mun_soc)