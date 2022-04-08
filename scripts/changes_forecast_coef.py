# 0.1

import pandas as pd


def calc_age_changes_coef(city_forecast):
    # Посчитать изменения населения в прогнозируемых годах относительно 2019 г. по возрастам
    changes_forecast = pd.DataFrame()
    columns = list(city_forecast.columns)

    for col in columns:
        changes_forecast[col] = city_forecast[col].div(city_forecast[2019])
    # changes_forecast.drop(2019, axis=1, inplace=True)

    death_coef = 1.1
    changes_forecast.loc[-1] = list(changes_forecast.iloc[[0]].values[0] * death_coef)  # adding a row
    changes_forecast.index = changes_forecast.index + 1  # shifting index
    index = pd.Index(range(0,101))
    changes_forecast = changes_forecast.set_index(index)
    changes_forecast.sort_index(inplace=True)
    changes_forecast[2019][0] = 1

    return changes_forecast


def calc_total_changes_percent(city_forecast):
    changes_forecast = pd.DataFrame()
    columns = list(city_forecast.columns)
    for col in columns:
        changes_forecast[col] = city_forecast[col].div(city_forecast[2019])
    # changes_forecast.drop(2019, axis=1, inplace=True)

    death_coef = 1.1

    changes_forecast.loc[-1] = list(changes_forecast.iloc[[0]].values[0] * death_coef)  # adding a row
    changes_forecast.index = changes_forecast.index + 1  # shifting index
    changes_forecast.sort_index(inplace=True)

    city_years_age_sum = changes_forecast.sum()
    city_years_age_ratio = changes_forecast.div(city_years_age_sum.iloc[:], axis='columns')

    return city_years_age_ratio


def main(city_forecast, path):
    print('В процессе: расчет прогноза изменения численности населения')

    city_forecast.drop(city_forecast.iloc[:, 0:23], inplace=True, axis=1)

    changes_forecast = calc_age_changes_coef(city_forecast)
    city_years_age_ratio = calc_total_changes_percent(city_forecast)

    # print('Выполнено: расчет прогноза изменения численности населения')

    return changes_forecast, city_years_age_ratio


if __name__ == '__main__':
    pass
