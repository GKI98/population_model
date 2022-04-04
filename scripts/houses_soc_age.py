# 5

import iteround
import pandas as pd
from scripts import push_to_db
from scripts.connect_db import Properties
from tqdm import tqdm
import time


# Распределить жителей домов (по соц. группам) по возрастам (0-100)
# и сохранить локально
def houses_soc_to_ages(args, houses_soc, mun_soc):

    mun_list = set(houses_soc['municipality_id'])
    soc_list = set(houses_soc['social_group_id'])

    print('Расчет жителей домов по возрастам среди соц.групп:')
    for mun in tqdm(mun_list):
        # Разрез по муниципалитетам - чтобы кусочками работать с df и не есть много памяти за раз
        houses_soc_mun = houses_soc.loc[houses_soc['municipality_id'] == mun]
        mun_soc_mun = mun_soc.loc[mun_soc['municipality_id'] == mun]

        df = pd.merge(houses_soc_mun, mun_soc_mun, on=['municipality_id', 'social_group_id'])
        df = df.sort_values(by=['house_id', 'social_group_id'])

        df['men_rounded'] = ''
        df['women_rounded'] = ''

        # Кол-во людей в соц.группе в возрасте по полу = кол-во людей в доме * вероятность быть
        # в возрасте в мун в соц группе
        df['men'] = df['men'] * df['mun_percent']
        df['women'] = df['women'] * df['mun_percent']

        # Разбиение по домикам - чтобы балансировать людей по домикам
        houses_id = set(df['house_id'])

        # Округление со сходящейся суммой по возрастам для соц.групп в доме
        for house in houses_id:
            for soc in soc_list:

                men_lst = df.query(f'social_group_id == {soc} & house_id == {house}')['men'].values
                women_lst = df.query(f'social_group_id == {soc} & house_id == {house}')['women'].values

                men_rnd = iteround.saferound(men_lst, 0)
                women_rnd = iteround.saferound(women_lst, 0)

                df.loc[(df['house_id'] == house) & (df['social_group_id'] == soc), 'men_rounded'] = men_rnd
                df.loc[(df['house_id'] == house) & (df['social_group_id'] == soc), 'women_rounded'] = women_rnd

        df = df.drop(['mun_percent', 'municipality_id'], axis=1)

        df['men'] = df['men'].astype(float).round(2)
        df['women'] = df['women'].astype(float).round(2)

        push_to_db.main(args=args, houses_df=df)


def drop_tables_if_exist(args):
    conn = Properties.connect(args.db_addr, args.db_port, args.db_name, args.db_user, args.db_pass)

    with conn, conn.cursor() as cur:
        cur.execute(f'drop table if exists social_stats.sex_age_social_houses')


def main(houses_soc, mun_soc, args, path=''):
    print('В процессе: распределение жителей домиков (по соц. группам) по возрастам')

    drop_tables_if_exist(args)

    mun_soc = mun_soc[['municipality_id', 'social_group_id', 'age', 'men', 'women']]

    houses_soc_to_ages(args, houses_soc, mun_soc)

    print('Выполнено: распределение жителей домиков (по соц. группам) по возрастам\n')


if __name__ == '__main__':
    pass
