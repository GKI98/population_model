# 5

import iteround
import pandas as pd
import push_to_db


# Распределить жителей домов (по соц. группам) по возрастам (0-100)
# и сохранить локально
def houses_soc_to_ages(args, houses_soc, mun_soc, path):
    print('В процессе: распределение жителей домов (по соц. группам) по возрастам')

    soc_list = set(houses_soc['social_group_id'])

    df = pd.merge(houses_soc, mun_soc[['municipality_id', 'admin_unit_parent_id', 'social_group_id', 'age',
                                       'men', 'women', 'total']],
                  on=['municipality_id', 'social_group_id'])
    df['total'] = df['total'] * df['mun_percent']
    df['men'] = df['total'] * df['mun_percent']
    df['women'] = df['total'] * df['mun_percent']

    total_list_tmp = []
    men_list_tmp = []
    women_list_tmp = []

    print('Расчет соц.группы №:')
    for soc in soc_list:
        print(soc)
        df_slice = df.query(f'social_group_id == {soc}')

        total = iteround.saferound(df_slice['total'].values, 0)
        men = iteround.saferound(df_slice['men'].values, 0)
        women = iteround.saferound(df_slice['women'].values, 0)

        total_list_tmp += total
        men_list_tmp += men
        women_list_tmp += women

    df['total'] = total_list_tmp
    df['men'] = men_list_tmp
    df['women'] = women_list_tmp

    df = df.drop('mun_percent', axis=1)
    push_to_db.main(args, df)

    # print(df)
    # df.to_csv(f'{path}houses_soc_age.csv', index=False, header=True)

    return df


def main(args, path=''):
    pd.set_option('display.max_rows', 10)
    pd.set_option('display.max_columns', 20)

    # path = '/home/gk/code/tmppycharm/ifmo_1/script/data/'
    houses_soc = pd.read_csv(f'{path}houses_soc.csv')
    houses_soc = houses_soc.drop(['house_total_soc', 'house_men_soc', 'house_women_soc'], axis=1)
    mun_soc = pd.read_csv(f'{path}mun_soc.csv')

    df = houses_soc_to_ages(args, houses_soc, mun_soc, path)
    return df


if __name__ == '__main__':
    pass
