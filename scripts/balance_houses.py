# 3

import pandas as pd
from scripts import read_data
from tqdm import tqdm
from random import uniform
from math import isnan


# Посчитать макс. и вероятное кол-во жителей в домике
def forecast_house_population(args):
    houses_df = read_data.main(args)[5]

    min_sq_liv = {
        1: 9,
        2: 10,
        3: 11,
        4: 10,
        5: 12,
        6: 14,
        7: 15,
        8: 12,
        9: 10,
        10: 11,
        11: 13,
        12: 13,
        14: 15,
        15: 12
    }


    max_population = (houses_df['living_area'] / min_sq_liv[args.city]).values
    # max_population_rnd = iteround.saferound(max_population, 0)
    houses_df['max_population'] = max_population
    houses_df['max_population'] = houses_df['max_population'].round()

    def vch_calc(row):
        a_omch = 0.3  # коэффициент для ожидаемой максимальной численности жителей (ОМЧ)
        a_ich = 0.7  # коэффициент для известной численности жителей (ИЧ)

        if row['resident_number'] is None:
            row['resident_number'] = row['max_population']
            # print(row)

        if row['failure'] is True:
            val = row['resident_number']            

        elif (row['resident_number'] == 0) and (row['failure'] is False):
            val = row['max_population']

        elif row['resident_number'] > row['max_population']:
            val = row['max_population']

        else:
            if row['max_population'] == row['resident_number']:
                rand_coef = round(uniform(0.7, 1), 3)
                val = int(round(row['max_population'] * rand_coef, 0))
            else:
                val = int(round(a_omch * row['max_population'] + a_ich * row['resident_number'], 0))

        if val < 0:
            val=0
            # print(row)
        
        val = round(val, 0)

        return val

    houses_df['prob_population'] = houses_df.apply(vch_calc, axis=1).round()

    # print('houses_df', houses_df.sum())

    return houses_df


# Сбалансировать вероятное кол-во жителей в домике
# и сохранить локально
def balance_houses_population(houses_df_upd, mun_age_sex_df):
    mun_list = set(houses_df_upd['municipality_id'])
    houses_df_upd = houses_df_upd.assign(citizens_reg_bal=houses_df_upd['prob_population'])

    # Минимальное значение, до которого может сокращаться населения в доме при балансировке, кол-во человек
    balancing_min = 1
    # 5

    # Точность балансировки, кол-во человек
    accuracy = 1
    # 1

    
    df_mkd_balanced_mo = pd.DataFrame()

    for mun in tqdm(mun_list):
        # print(mun)
        citizens_mo_reg_bal = mun_age_sex_df.query(f'municipality_id == {mun}')['total'].sum()
        

        # Выбрать дома, относящиеся к выбранному МО
        df_mkd_mo = houses_df_upd.query(f'municipality_id == {mun}')

        # Сделать вероятные количества жителей в домах отправной точкой для расчета сбалансированных значений
        citizens_mo_bal = df_mkd_mo['citizens_reg_bal'].sum()

        # print('MUN\n', citizens_mo_reg_bal)
        # print('HOUSES BEFORE BALANCE\n', citizens_mo_bal)

        # Если количество жителей в МО после балансировки по району БОЛЬШЕ,
        # чем рассчитанное вероятное количество жителей для этого МО, то разница должна быть распределена
        # между не аварийными домами МО
        if citizens_mo_reg_bal > citizens_mo_bal:
            while citizens_mo_reg_bal > citizens_mo_bal:
                # print('more', citizens_mo_reg_bal, citizens_mo_bal, end="\r")
                df_mkd_mo_not_f = df_mkd_mo[df_mkd_mo['failure'] == False]
                # Находим индекс неаварийного дома с максимальной разницей между ОМЧ и ВЧ
                the_house = (df_mkd_mo_not_f['citizens_reg_bal'] / df_mkd_mo_not_f['max_population']).idxmin()
                # Прибавляем жителей к "сбалансированной численности" этого дома
                df_mkd_mo.at[the_house, 'citizens_reg_bal'] = df_mkd_mo.loc[the_house, 'citizens_reg_bal'] + accuracy
                # Ищем новое значение сбалансированной численности для МО
                citizens_mo_bal = df_mkd_mo['citizens_reg_bal'].sum()
                

        # Если количество жителей в МО после балансировки по району МЕНЬШЕ,
        # чем рассчитанное вероятное количество жителей для этого МО, то разница должна быть вычтена
        # из количества жителей домов, причем аварийные дома также участвуют в балансировке
        elif citizens_mo_reg_bal < citizens_mo_bal:
            while citizens_mo_reg_bal < citizens_mo_bal:
                # print('less', citizens_mo_reg_bal, citizens_mo_bal, end="\r")
                df_mkd_mo_not_f = df_mkd_mo[df_mkd_mo['citizens_reg_bal'] > balancing_min]

                try:
                    the_house = (df_mkd_mo_not_f['citizens_reg_bal'] / df_mkd_mo_not_f['max_population']).idxmax()
                    # Вычитаем жителей из "сбалансированной численности" этого дома
                    df_mkd_mo.at[the_house, 'citizens_reg_bal'] = df_mkd_mo.loc[
                                                                      the_house, 'citizens_reg_bal'] - accuracy
                except ValueError as e:
                    print('Численность по МУН меньше, чем минимальное распределение по домикам в этом МУН')
                    print('\nError:Необходимо уменьшить минимальную численность населения для каждого дома\n')
                    raise e

                # Ищем новое значение сбалансированной численности для МО
                citizens_mo_bal = df_mkd_mo['citizens_reg_bal'].sum()
                
        # print('citizens_mo_reg_bal', citizens_mo_reg_bal)
        # print('df_mkd_mo', df_mkd_mo.sum())
        
        df_mkd_balanced_mo = pd.concat([df_mkd_balanced_mo, df_mkd_mo])
        # from time import sleep

        # sleep(10)
        

    return df_mkd_balanced_mo


def main(args, mun_age_sex_df):
    # print(mun_age_sex_df.municipality_id.unique())
    # print('В процессе: балансировка населения по домикам')
    print('Балансировка жителей домов для муниципалитетов:')
    # print('mun_age_sex_df',mun_age_sex_df[['total', 'municipality_id']].groupby('municipality_id').sum())

    houses_df_upd = forecast_house_population(args)
    # print(houses_df_upd.municipality_id.unique())
    

    df_mkd_balanced_mo = balance_houses_population(houses_df_upd, mun_age_sex_df)

    # print(df_mkd_balanced_mo.sum())
    

    return df_mkd_balanced_mo


if __name__ == '__main__':
    pass
