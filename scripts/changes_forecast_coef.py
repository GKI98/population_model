from typing import Tuple

import pandas as pd
from loguru import logger


def calc_age_changes_coef(city_forecast: pd.DataFrame, base_year: int) -> pd.DataFrame:
    '''Подсчет изменения населения в прогнозируемых годах относительно базового года по возрастам'''
    changes_forecast = pd.DataFrame()
    columns = list(city_forecast.columns)

    if base_year not in columns:
        logger.error('Заданый базовый год для подсчета изменения населения ({}) отсутствует во входных данных ({})',
                base_year, ', '.join(map(str, sorted(columns))))
    for col in columns:
        changes_forecast[col] = city_forecast[col].div(city_forecast[base_year])

    return changes_forecast


def calc_total_changes_percent(city_forecast: pd.DataFrame, base_year: int) -> pd.DataFrame:
    changes_forecast = pd.DataFrame()
    columns = list(city_forecast.columns)
    for col in columns:
        changes_forecast[col] = city_forecast[col].div(city_forecast[base_year])

    city_years_age_sum = changes_forecast.sum()
    city_years_age_ratio = changes_forecast.div(city_years_age_sum.iloc[:], axis=1)

    return city_years_age_ratio


def main(city_forecast: pd.DataFrame, base_year: int) -> Tuple[pd.DataFrame, pd.DataFrame]:
    '''Dataframes are returned in order `changes_forecast`, `city_years_age_ratio`'''
    logger.info('Начался расчет прогноза изменения численности населения')

    city_forecast.drop(city_forecast.iloc[:, 0:24], inplace=True, axis=1)

    changes_forecast = calc_age_changes_coef(city_forecast, base_year)
    city_years_age_ratio = calc_total_changes_percent(city_forecast, base_year)

    return changes_forecast, city_years_age_ratio
