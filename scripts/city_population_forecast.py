from statistics import mean
from typing import Dict, Literal, Tuple

import iteround
import numpy as np
import pandas as pd
from loguru import logger

def get_migration_coefficient(scenario: Literal['pos', 'mod', 'neg'], migration_coefficients: pd.Series) -> float:
    if scenario == 'pos':
        return migration_coefficients.max()
    if scenario == 'mod':
        return migration_coefficients.mean()
    return migration_coefficients.min()


def calc_survival_coef(df: pd.DataFrame, scenario: Literal['pos', 'mod', 'neg']) -> pd.DataFrame:
    '''Расчет коэффициентов дожития'''
    logger.info('Начался расчет коэффициентов дожития')

    prob_survival = [[0 for _year in range(len(df.columns) - 1)] for _age in range(len(df.index) - 1)]

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


def rename_new_table_attributes(df: pd.DataFrame, df2: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[int, int]]:
    list_headers_1 = list(df.columns.values)[1:]
    list_headers_2 = list(df2.columns.values)

    res_headers = {list_headers_2[i]: list_headers_1[i] for i in range(len(list_headers_2))}

    list_index_1 = list(df.index)[1:]
    list_index_2 = list(df2.index.values)

    res_index = {list_index_2[i]: list_index_1[i] for i in range(len(list_index_2))}

    df2.rename(index=res_index, columns=res_headers, inplace=True)
    df2.columns.name = 'Вероятность дожития'

    return df2, res_index


def main(scenario, year, population_changes: pd.DataFrame, migration_coefficients: pd.Series) -> pd.DataFrame:
    '''
    1. Пропуски в дф заполняется средними значниями
    2. Считается для каждого года с 96-й коэффициент дожития
    3. Берется средний коэф дожития и 20-й год последовательно умножается на коэф дожития на N лет вперед
    3.1 Данные о рождаемости берутся средними за весь период наблюдений
    '''
    logger.info('Начался прогноз изменения численности населения')

    df = population_changes

    base_year = int(df.index.max())

    # Приведение таблицы к адекватному виду
    df = df.transpose()
    df.columns = df.columns.astype(int)

    df.index = range(101)
    # Заполнение пропусков в исходной таблице средним значением в строке
    for year_ in range(len(df.columns)):
        for age in range(len(df.index)):
            if np.isnan(df[1995 + year_][age]):
                # Если base_year-й NaN - беру среднее за предыдущие два года
                if 1995 + year_ == base_year:
                    df[1995 + year_][age] = mean([df[1995 + year_ - 1][age], df[1995 + year_ - 2][age]])
                else:
                    df[1995 + year_][age] = df.loc[age].median(skipna=True)
            continue
    df = df.astype(int)

    # Коэффициент вероятности дожития
    df_coef = calc_survival_coef(df, scenario)

    # Прогноз на кол-во лет
    years_forecast = year - base_year

    migration_coefficient = get_migration_coefficient(scenario, migration_coefficients)

    for year in range(1, years_forecast + 1):
        df.loc[1:101, base_year + year] = df[base_year + year - 1][0:100].values * df_coef['coef'].values

        # Беру среднюю рождаемость за последние 5 лет
        df.at[0, base_year + year] = df.iloc[0, -5:-1].median()

        # Учесть миграцию
        df.loc[:, base_year + year] *= migration_coefficient
        df.loc[:, base_year + year] = iteround.saferound(df.loc[:, base_year + year].values, 0) # TODO: remove iteround

    df = df.astype(int)
    df = df.rename_axis('Age', axis=1)

    return df