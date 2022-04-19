# 0.1

import pandas as pd


def calc_age_changes_coef(city_forecast):
    # Посчитать изменения населения в прогнозируемых годах относительно 2019 г. по возрастам
    changes_forecast = pd.DataFrame()
    columns = list(city_forecast.columns)

    for col in columns:
        changes_forecast[col] = city_forecast[col].div(city_forecast[2019])

    return changes_forecast


def calc_total_changes_percent(city_forecast):
    changes_forecast = pd.DataFrame()
    columns = list(city_forecast.columns)
    for col in columns:
        changes_forecast[col] = city_forecast[col].div(city_forecast[2019])

    city_years_age_sum = changes_forecast.sum()
    city_years_age_ratio = changes_forecast.div(city_years_age_sum.iloc[:], axis='columns')

    return city_years_age_ratio


def main(city_forecast, path=''):
    print('В процессе: расчет прогноза изменения численности населения')

    city_forecast.drop(city_forecast.iloc[:, 0:24], inplace=True, axis=1)

    changes_forecast = calc_age_changes_coef(city_forecast)
    city_years_age_ratio = calc_total_changes_percent(city_forecast)

    # print('Выполнено: расчет прогноза изменения численности населения')


    return changes_forecast, city_years_age_ratio


if __name__ == '__main__':
    df = pd.read_csv('/home/gk/Desktop/to_SA/mod_forecast.csv')
    df = df.drop('Unnamed: 0', axis=1)
    df = df.astype(int)

    main(city_forecast=df)
