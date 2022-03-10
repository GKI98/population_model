# 4

import iteround
import pandas as pd


# Распределить жителей домов по соц.группам
# и сохранить локально
def houses_to_soc(houses_bal, mun_soc_allages_sum, path) -> None:

    print('В процессе: распределение жителей домов по соц.группам')

    mun_percent = []
    houses_bal['mun_percent'] = ''
    mun_list = set(houses_bal['municipality_id'])

    for mun in mun_list:
        mun_sum = houses_bal.query(f'municipality_id == {mun}')['citizens_reg_bal'].sum()
        mun_houses_ppl = houses_bal.query(f'municipality_id == {mun}')['citizens_reg_bal'].values
        mun_percent += [(mun_house / mun_sum) for mun_house in mun_houses_ppl]

    houses_bal['mun_percent'] = mun_percent
    houses_soc = pd.merge(houses_bal, mun_soc_allages_sum[['municipality_id', 'social_group_id',
                                                           'total_mun_soc_sum', 'men_mun_soc_sum',
                                                           'women_mun_soc_sum']], on='municipality_id')
    houses_soc['house_total_soc'] = houses_soc['mun_percent'] * houses_soc['total_mun_soc_sum']
    houses_soc['house_men_soc'] = houses_soc['mun_percent'] * houses_soc['men_mun_soc_sum']
    houses_soc['house_women_soc'] = houses_soc['mun_percent'] * houses_soc['women_mun_soc_sum']
    houses_soc = houses_soc.sort_values(by='social_group_id')

    total_list_tmp = []
    men_list_tmp = []
    women_list_tmp = []

    soc_list = set(mun_soc_allages_sum['social_group_id'])

    for soc in soc_list:
        df_slice = houses_soc.query(f'social_group_id == {soc}')
        total = iteround.saferound(df_slice['house_total_soc'].values, 0)
        men = iteround.saferound(df_slice['house_men_soc'].values, 0)
        women = iteround.saferound(df_slice['house_women_soc'].values, 0)

        total_list_tmp += total
        men_list_tmp += men
        women_list_tmp += women

    houses_soc['house_total_soc'] = total_list_tmp
    houses_soc['house_men_soc'] = men_list_tmp
    houses_soc['house_women_soc'] = women_list_tmp

    houses_soc = houses_soc.sort_values(by='municipality_id')
    houses_soc = houses_soc.drop(['total_mun_soc_sum', 'men_mun_soc_sum', 'women_mun_soc_sum'], axis=1)
    houses_soc = houses_soc.rename(columns={"resident_number": "document_population"})
    houses_soc = houses_soc.rename(columns={"citizens_reg_bal": "resident_number"})

    print('\nhouses_soc:\n', houses_soc)

    houses_soc.to_csv(f'{path}houses_soc.csv', index=False, header=True)


def main(path=''):
    pd.set_option('display.max_rows', 10)
    pd.set_option('display.max_columns', 20)

    # path = '/home/gk/code/tmppycharm/ifmo_1/script/data/'

    houses_bal = pd.read_csv(f'{path}houses_bal.csv').drop(['Unnamed: 0'], axis=1)
    mun_soc_allages_sum = pd.read_csv(f'{path}mun_soc_allages_sum.csv')

    houses_to_soc(houses_bal, mun_soc_allages_sum, path)


if __name__ == '__main__':
    main()
