# 4

import iteround
import pandas as pd
# import time


# Распределить жителей домов по соц.группам
# и сохранить локально
def houses_to_soc(houses_bal, mun_soc_allages_sum, path):

    # mun_percent = []
    houses_bal['mun_percent'] = ''
    mun_list = set(houses_bal['municipality_id'])
    print('mun_list: ', mun_list)
    houses_bal = houses_bal.sort_values(by='municipality_id')

    for mun in mun_list:

        # Всего людей в мун
        # mun_sum = houses_bal.query(f'municipality_id == {mun}')['citizens_reg_bal'].sum()


        # print(mun_sum)

        # значение людей по домикам в мун
        mun_houses_ppl = houses_bal.query(f'municipality_id == {mun}')['citizens_reg_bal']

        # mun_houses_ppl.to_csv('mun_houses_ppl.csv')

        mun_sum = mun_houses_ppl.sum()

        print(f'mun_ {mun} _sum: ', mun_sum)

        # houses_bal.query(f'municipality_id == {mun}').to_csv('mun_houses_ppl.csv')

        # time.sleep(1000)

        # вероятность быть в домике в мун
        houses_bal.loc[houses_bal['municipality_id'] == mun, 'mun_percent'] = mun_houses_ppl / mun_sum

    houses_soc = pd.merge(houses_bal, mun_soc_allages_sum[['municipality_id', 'social_group_id',
                                                           'total_mun_soc_sum', 'men_mun_soc_sum',
                                                           'women_mun_soc_sum']], on='municipality_id')

    houses_soc = houses_soc.sort_values(by='social_group_id')

    # Вероятность быть в доме в конкретном мун * на вероятность быть в соц.группе в конкретном мун
    houses_soc['house_total_soc'] = houses_soc['mun_percent'] * houses_soc['total_mun_soc_sum']
    houses_soc['house_men_soc'] = houses_soc['mun_percent'] * houses_soc['men_mun_soc_sum']
    houses_soc['house_women_soc'] = houses_soc['mun_percent'] * houses_soc['women_mun_soc_sum']

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
    # houses_soc = houses_soc.drop(['total_mun_soc_sum', 'men_mun_soc_sum', 'women_mun_soc_sum'], axis=1)
    houses_soc = houses_soc.rename(columns={"resident_number": "document_population"})
    houses_soc = houses_soc.rename(columns={"citizens_reg_bal": "resident_number"})

    # print('\nCheckpoint 1\n')
    # time.sleep(20)

    return houses_soc


def main(df_mkd_balanced_mo, mun_soc_allages_sum, path=''):
    print('В процессе: распределение жителей домов по соц.группам')

    pd.set_option('display.max_rows', 10)
    pd.set_option('display.max_columns', 20)

    houses_bal = df_mkd_balanced_mo

    houses_soc = houses_to_soc(houses_bal, mun_soc_allages_sum, path)
    print('Выполнено: распределение жителей домов по соц.группам\n')

    return houses_soc


if __name__ == '__main__':
    # main()
    pass