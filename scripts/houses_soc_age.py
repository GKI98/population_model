# 5

import iteround
import pandas as pd
from scripts import push_to_db
from scripts.connect_db import Properties
import time

# Распределить жителей домов (по соц. группам) по возрастам (0-100)
# и сохранить локально


def houses_soc_to_ages(args, houses_soc, mun_soc):
    print(f'houses_soc memory usage:{round((houses_soc.memory_usage(index=True, deep=True).sum() / 10 ** 9), 2)} GB')
    print(f'mun_soc memory usage:{round((mun_soc.memory_usage(index=True, deep=True).sum() / 10 ** 9), 2)} GB')

    # soc_list = set(houses_soc['social_group_id'])
    mun_list = set(houses_soc['municipality_id'])
    len_mun_list = len(mun_list)

    for counter, mun in enumerate(mun_list):
        # Разрез по муниципалитетам - чтобы кусочками работать с df и не есть много памяти за раз
        print(f'\nРасчет МУН: {counter} / {len_mun_list}\n')

        houses_soc_mun = houses_soc.loc[houses_soc['municipality_id'] == mun]

        houses_id = set(houses_soc_mun['house_id'])

        df = pd.merge(houses_soc_mun,
                      mun_soc.loc[mun_soc['municipality_id'] == mun], on=['municipality_id', 'social_group_id'])


        # Это было ниже
        df = df.sort_values(by=['house_id'])


        print(df.head())


        # А это было выше!
        # Кол-во людей в соц.группе в возрасте по полу = кол-во людей в доме * вероятность быть в возрасте в мун в соц группе
        df['men'] = df['men'] * df['mun_percent']
        df['women'] = df['women'] * df['mun_percent']




        men_list_tmp = []
        women_list_tmp = []

        # Разбиение по домикам - чтобы балансировать людей по домикам
        print('Округление жителей домов до целых чисел для мун')

        total_h = len(houses_id)
        counter = 0

        for house in houses_id:
            counter += 1

            print(f'округление для дома: {counter} / {total_h}')

            df_slice = df.query(f'house_id == {house}')

            print('resident_number: ', df_slice['resident_number'][:1])

            print('MEN: ',df_slice['men'].sum())
            print('WOMEN: ',df_slice['women'].sum())

            men = iteround.saferound(df_slice['men'].values, 0)
            women = iteround.saferound(df_slice['women'].values, 0)

            print('MEN: ', df_slice['men'].sum())
            print('WOMEN: ', df_slice['women'].sum())

            men_list_tmp += men
            women_list_tmp += women


        df['men'] = men_list_tmp
        df['women'] = women_list_tmp

        print(f'DF SIZE:{df.memory_usage(index=True, deep=True).sum() / 10 ** 9} GB')

        df = df.drop('mun_percent', axis=1)

        push_to_db.main(args=args, houses_df=df)


def drop_tables_if_exist(args):
    conn = Properties.connect(args.db_addr, args.db_port, args.db_name, args.db_user, args.db_pass)

    with conn, conn.cursor() as cur:
        cur.execute(f'drop table if exists social_stats.sex_age_social_houses')


def main(houses_soc, mun_soc, args, path=''):
    print('В процессе: распределение жителей домиков (по соц. группам) по возрастам')

    drop_tables_if_exist(args)

    pd.set_option('display.max_rows', 10)
    pd.set_option('display.max_columns', 20)

    houses_soc = houses_soc.drop(['house_total_soc', 'house_men_soc', 'house_women_soc',
                                  'administrative_unit_id', 'prob_population', 'failure', 'living_area'], axis=1)

    # houses_soc = houses_soc.drop(['administrative_unit_id', 'prob_population', 'failure', 'living_area'], axis=1)

    mun_soc.age = mun_soc.age.astype('uint8')
    mun_soc = mun_soc[['municipality_id', 'social_group_id', 'age', 'men', 'women']]

    houses_soc.rename({'id': 'house_id'}, axis=1, inplace=True)
    # houses_soc.rename({'men': 'resident_number_men'}, axis=1, inplace=True)
    # houses_soc.rename({'women': 'resident_number_women'}, axis=1, inplace=True)

    print(houses_soc.head())

    houses_soc_to_ages(args, houses_soc, mun_soc)

    print('Выполнено: распределение жителей домиков (по соц. группам) по возрастам\n')


if __name__ == '__main__':
    pass
