# 0.0

import pandas as pd
import numpy as np
from statistics import mean
import iteround

"""
1. Пропуски в дф заполняется средними значниями
2. Считается для каждого года начиная с 96-го коэффициент дожития
3. Берется средний коэф дожития и 20-й год последовательно умножается на коэф дожития на N лет вперед
3.1 Данные о рождаемости берутся средними за весь период наблюдений
"""


def coef_migration(city_id, scenario):
    if scenario == 'pos':
        func = 'max'
    elif scenario == 'mod':
        func = 'median'
    elif scenario == 'neg':
        func = 'min'

    period_city_total = pd.read_excel(io='./scripts/Input_data/coef_migrations.xlsx', sheet_name='coef')
    df = period_city_total.drop('id', axis=1)

    spb = df.loc[df.region == 'г.Санкт-Петербург ']
    krd = df.loc[df.region == 'Краснодарский край']
    vrn = df.loc[df.region == 'Воронежская область']
    srt = df.loc[df.region == 'Саратовская область']
    sev = df.loc[df.region == 'г. Севастополь']
    vnvg = df.loc[df.region == 'Новгородская область']
    omsk = df.loc[df.region == 'Омская область']
    kzn = df.loc[df.region == 'Республика Татарстан']
    nnvg = df.loc[df.region == 'Нижегородская область']
    vol = df.loc[df.region == 'Волгоградская область']

    city_coef = {
        1: spb,
        2: krd,
        3: vrn,
        4: srt,
        5: sev,
        6: vnvg,
        7: omsk,
        8: kzn,
        9: nnvg,
        10: vol
    }

    return 1 + city_coef.get(city_id)[func].values[0]


def replace_nan(df):
    df.index = range(101)
    # Заполнение пропусков в исходной таблице средним значением в строке
    for year in range(len(df.columns)):
        for age in range(len(df.index)):
            if np.isnan(df[1995 + year][age]):
                # Если 2020-й NaN - беру среднее за предыдущие два года
                if 1995 + year == 2020:
                    df[1995 + year][age] = mean([df[1995 + year - 1][age], df[1995 + year - 2][age]])
                else:
                    df[1995 + year][age] = df.loc[age].median(skipna=True)
            continue
    df = df.astype(int)

    return df


# Расчет коэф. дожития
def calc_survival_coef(df, scenario):
    print('В процессе: расчет коэф. дожития')

    prob_survival = [[0 for year in range(len(df.columns) - 1)] for age in range(len(df.index) - 1)]

    for year in range(len(df.columns) - 1):
        for age in range(len(df.index) - 1):
            var1 = df[1995 + year][age]
            var2 = df[1996 + year][age + 1]
            if var1 == 0:
                prob_survival[age][year] = 0
                continue
            prob_survival[age][year] = var2 / var1

    df_survival_relations = pd.DataFrame(prob_survival)
    df_survival_relations, res_index = rename_new_table_attributes(df, df_survival_relations)

    if scenario == 'pos':
        df_coef = pd.DataFrame(list(df_survival_relations.max(axis=1, skipna=True)))
    elif scenario == 'mod':
        df_coef = pd.DataFrame(list(df_survival_relations.median(axis=1, skipna=True)))
    elif scenario == 'neg':
        df_coef = pd.DataFrame(list(df_survival_relations.min(axis=1, skipna=True)))

    df_coef.rename(columns={df_coef.columns.values[0]: 'coef'}, inplace=True)
    df_coef.rename(index=res_index, inplace=True)

    # Тут я немного корректирую значение для 100 т.к. оно каждый раз увеличивалось неск раз
    # (вероятно, из-за кривых данных)
    df_coef.at[100] = df_coef.loc[99] / 1.05

    return df_coef


def rename_new_table_attributes(df, df2):
    list_headers_1 = list(df.columns.values)
    del list_headers_1[0]
    list_headers_2 = list(df2.columns.values)

    res_headers = {list_headers_2[i]: list_headers_1[i] for i in range(len(list_headers_2))}

    list_index_1 = list(df.index)
    del list_index_1[0]
    list_index_2 = list(df2.index.values)

    res_index = {list_index_2[i]: list_index_1[i] for i in range(len(list_index_2))}

    df2.rename(index=res_index, columns=res_headers, inplace=True)
    df2.columns.name = 'Вероятность дожития'

    return df2, res_index


def main(city_id, scenario, year):
    print('В процессе: прогноз изменения численности населения')

    period_city_total = pd.read_excel(io='./scripts/Input_data/changes_population.xls',
                                      skiprows=5, usecols='A,B,R:CR,CT:DN')
    df = period_city_total

    # Приведение таблицы к адекватному виду
    df = df.transpose()
    df.columns = df.iloc[0]
    df.columns = df.columns.astype(int)
    df.drop(index=df.index[0], inplace=True)
    df = replace_nan(df)

    # Коэффициент вероятности дожития
    df_coef = calc_survival_coef(df, scenario)

    # Прогноз на кол-во лет
    years_forecast = year - 2020

    for year in range(years_forecast):
        year += 1
        df.loc[1:101, 2020 + year] = df[2020 + year - 1][0:100].values * df_coef['coef'].values

        # Беру среднюю рождаемость за последние 5 лет
        df.at[0, 2020 + year] = df.iloc[0, -5:-1].median()

        # Учесть миграцию
        df.loc[:, 2020 + year] *= coef_migration(city_id, scenario)
        df.loc[:, 2020 + year] = iteround.saferound(df.loc[:, 2020 + year].values, 0)

    df = df.astype(int)
    df = df.rename_axis('Age', axis='columns')

    return df


if __name__ == '__main__':
    # pass
    # pd.set_option('display.max_rows', 10)
    # pd.set_option('display.max_columns', 100)

    f = main(1, 'mod', 2040)
    f.to_csv('/home/gk/Desktop/to_SA/mod_forecast.csv')
    print('done!')

