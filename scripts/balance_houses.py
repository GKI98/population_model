# 3

import pandas as pd
from scripts import read_data
import iteround
from tqdm import tqdm

'''
В houses нет муниципалитета №101 !!!
'''


# Посчитать макс. и вероятное кол-во жителей в домике
def forecast_house_population(args):
    houses_df = read_data.main(args)[5]
    max_sq_liv = 9

    max_population = (houses_df['living_area'] / max_sq_liv).values
    max_population_rnd = iteround.saferound(max_population, 0)
    houses_df['max_population'] = max_population_rnd

    def vch_calc(row):
        a_omch = 0.3  # коэффициент для ожидаемой максимальной численности жителей (ОМЧ)
        a_ich = 0.7  # коэффициент для известной численности жителей (ИЧ)

        if row['failure'] is True:
            val = row['resident_number']

        elif (row['resident_number'] == 0) and (row['failure'] is False):
            val = row['max_population']

        elif row['resident_number'] > row['max_population']:
            val = row['max_population']
        else:
            val = a_omch * row['max_population'] + a_ich * row['resident_number']
        return val

    houses_df['prob_population'] = houses_df.apply(vch_calc, axis=1).round().astype(int)

    return houses_df


def balance_houses_population(houses_df_upd, mun_age_sex_df):
    mun_list = set(mun_age_sex_df['municipality_id'])

    dfh = houses_df_upd
    dfm = mun_age_sex_df

    mun_col = 'municipality_id'
    ppl_col = 'citizens_reg_bal'
    max_col = 'max_population'

    for mun in mun_list:

        prob_ppl = dfh.query(f'{mun_col} == {mun}')[max_col]
        prob_ppl_s = prob_ppl.sum()
        ppl_mun = dfm.query(f'{mun_col} == {mun}')['total'].sum()

        dfh.loc[dfh[mun_col] == mun, ppl_col] = iteround.saferound((prob_ppl / prob_ppl_s * ppl_mun).values, 0)

    dfh.to_csv('dfh.csv')

    print('sleep')
    import time
    time.sleep(10000)

    return dfh


# Сбалансировать вероятное кол-во жителей в домике
# и сохранить локально
# def balance_houses_population(houses_df_upd, mun_age_sex_df, path):
#     mun_list = set(houses_df_upd['municipality_id'])
#     houses_df_upd = houses_df_upd.assign(**{'citizens_reg_bal': houses_df_upd['prob_population']})
#
#     # Минимальное значение, до которого может сокращаться населения в доме при балансировке, кол-во человек
#     balancing_min = 5
#     # 5
#
#     # Точность балансировки, кол-во человек
#     accuracy = 1
#     # 1
#
#     counter = 0
#     df_mkd_balanced_mo = pd.DataFrame()
#     sex = 'total'
#
#     for mun in tqdm(mun_list):
#         citizens_mo_reg_bal = mun_age_sex_df.query(f'municipality_id == {mun}')[sex].sum()
#
#         # Выбрать дома, относящиеся к выбранному МО
#         df_mkd_mo = houses_df_upd.query(f'municipality_id == {mun}')
#
#         # Сделать вероятные количества жителей в домах отправной точкой для расчета сбалансированных значений
#         citizens_mo_bal = df_mkd_mo['citizens_reg_bal'].sum()
#
#         # Шаг балансировки
#         i = 0
#
#         # Если количество жителей в МО после балансировки по району БОЛЬШЕ,
#         # чем рассчитанное вероятное количество жителей для этого МО, то разница должна быть распределена
#         # между не аварийными домами МО
#         if citizens_mo_reg_bal > citizens_mo_bal:
#             while citizens_mo_reg_bal > citizens_mo_bal:
#                 df_mkd_mo_not_f = df_mkd_mo[df_mkd_mo['failure'] == 0]
#                 # Находим индекс неаварийного дома с максимальной разницей между ОМЧ и ВЧ
#                 the_house = (df_mkd_mo_not_f['citizens_reg_bal'] / df_mkd_mo_not_f['max_population']).idxmin()
#                 # Прибавляем жителей к "сбалансированной численности" этого дома
#                 df_mkd_mo.at[the_house, 'citizens_reg_bal'] = df_mkd_mo.loc[the_house, 'citizens_reg_bal'] + accuracy
#                 # Ищем новое значение сбалансированной численности для МО
#                 citizens_mo_bal = df_mkd_mo['citizens_reg_bal'].sum()
#                 i = i + 1
#
#         # Если количество жителей в МО после балансировки по району МЕНЬШЕ,
#         # чем рассчитанное вероятное количество жителей для этого МО, то разница должна быть вычтена
#         # из количества жителей домов, причем аварийные дома также участвуют в балансировке
#         elif citizens_mo_reg_bal < citizens_mo_bal:
#             while citizens_mo_reg_bal < citizens_mo_bal:
#                 df_mkd_mo_not_f = df_mkd_mo[df_mkd_mo['citizens_reg_bal'] > balancing_min]
#
#                 try:
#                     the_house = (df_mkd_mo_not_f['citizens_reg_bal'] / df_mkd_mo_not_f['max_population']).idxmax()
#                     # Вычитаем жителей из "сбалансированной численности" этого дома
#                     df_mkd_mo.at[the_house, 'citizens_reg_bal'] = df_mkd_mo.loc[
#                                                                       the_house, 'citizens_reg_bal'] - accuracy
#                 except ValueError as e:
#                     print('Численность по МУН меньше, чем минимальное распределение по домикам в этом МУН')
#                     print('\nError:Необходимо уменьшить минимальную численность населения для каждого домика\n')
#                     raise e
#
#                 # Ищем новое значение сбалансированной численности для МО
#                 citizens_mo_bal = df_mkd_mo['citizens_reg_bal'].sum()
#                 i = i + 1
#
#         df_mkd_balanced_mo = pd.concat([df_mkd_balanced_mo, df_mkd_mo])
#         counter += 1
#
#     return df_mkd_balanced_mo


def main(args, mun_age_sex_df, path=''):
    # print('В процессе: балансировка населения по домикам')
    print('Балансировка жителей домов для муниципалитетов:')

    houses_df_upd = forecast_house_population(args)
    df_mkd_balanced_mo = balance_houses_population(houses_df_upd, mun_age_sex_df)

    return df_mkd_balanced_mo


if __name__ == '__main__':
    pass
