# 0.0

import pandas as pd
import numpy as np
from statistics import mean
import iteround
import warnings
# warnings.filterwarnings("ignore")


def replace_nan(df):
    df.index = range(101)
    # Заполнение пропусков средним значением в строке
    for i in range(len(df.columns)):
        for j in range(len(df.index)):
            if np.isnan(df[1995 + i][j]):
                # Если 2020-й NaN - беру среднее за предыдущие два года
                if 1995 + i == 2020:
                    df[1995 + i][j] = mean([df[1995 + i - 1][j], df[1995 + i - 2][j]])
                else:
                    df[1995 + i][j] = df.loc[j].median(skipna=True)
            continue
    df = df.astype(int)

    return df


# Расчет коэф. дожития
def calc_survival_coef(df):
    print('В процессе: расчет коэф. дожития')

    prob_survival = [[0 for i in range(len(df.columns) - 1)] for j in range(len(df.index) - 1)]

    for j in range(len(df.columns) - 1):
        for i in range(len(df.index) - 1):
            var1 = df[1995 + j][i]
            var2 = df[1996 + j][i + 1]
            if var1 == 0:
                prob_survival[i][j] = 0
                continue
            prob_survival[i][j] = var2 / var1
    df2 = pd.DataFrame(prob_survival)

    return df2


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


def main(path):
    pd.set_option('display.max_rows', 10)
    pd.set_option('display.max_columns', 20)

    # Чтение данных из таблицы Excel
    # ----
    period_city_total = pd.read_excel(io='./population_model/script/Input_data/'
                                      'report_17 Jun 2021 08_52_44 GMT(old_excel).xls', skiprows=5,
                                      usecols='A,B,R:CR,CT:DN')
    df = period_city_total
    # ----

    # Приведение таблицы к адекватному виду
    df = df.transpose()
    df.columns = df.iloc[0]
    df.columns = df.columns.astype(int)
    df.drop(index=df.index[0], inplace=True)

    df = replace_nan(df)
    df2 = calc_survival_coef(df)

    df2, res_index = rename_new_table_attributes(df, df2)

    coef2 = list(df2.median(axis=1, skipna=True))
    df3 = pd.DataFrame(coef2)
    df3.rename(columns={df3.columns.values[0]: 'coef'}, inplace=True)
    df3.rename(index=res_index, inplace=True)

    # Тут я немного корректирую значение для 100 т.к. оно каждый раз увеличивалось в 2 раза
    # вероятно из-за кривых данных
    df3.at[100] = df3.loc[99] / 2

    # Прогноз на кол-во лет
    years_forecast = 10

    # Обрезаю таблицу на
    df = df.iloc[1:, 1:]

    for i in range(years_forecast):
        col_num = len(list(df.columns))
        column = df.columns[-1] + 1

        # Это без пропусков в исходном датафрейме
        #    value = (df[2020 + i] * df3['coef'])

        # Это с пропусками в исходном датафрейме
        df.at[1, 2020 + i] = df.iloc[1].median()
        value = (df[2020 + i][:] * df3['coef'][:])
        value = iteround.saferound(value.values, 0)
        df.insert(loc=col_num, column=column, value=value, allow_duplicates=True)

    df = df.astype(int)
    df = df.rename_axis('Age', axis='columns')

    # Сохранить в csv
    df.to_csv(f'{path}/city_population_forecast.csv', index=True, header=True)

    print('Выполнено: прогноз изменения населения')


if __name__ == '__main__':
    pass
