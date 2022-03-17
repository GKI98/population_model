# 0.1

import pandas as pd


# def read_data(path):
#     # Прочитать таблицу с прогнозом изменения населения на N лет вперед
#     city_forecast = pd.read_csv(f'{path}/city_population_forecast.csv')
#     city_forecast.drop(city_forecast.iloc[:, 0:24], inplace=True, axis=1)
#
#     return city_forecast


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

    # print(changes_forecast)

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


# def save_data(df, path, file_name):
#     # Сохранить в csv
#     df.to_csv(f'{path}/{file_name}.csv', index=True, header=True)


def main(city_forecast, path):
    print('\nВ процессе: расчет прогноза изменения численности населения')

    pd.set_option('display.max_rows', 10)
    pd.set_option('display.max_columns', 20)

    # city_forecast = read_data(path)
    city_forecast.drop(city_forecast.iloc[:, 0:23], inplace=True, axis=1)

    changes_forecast = calc_age_changes_coef(city_forecast)
    city_years_age_ratio = calc_total_changes_percent(city_forecast)

    # save_data(changes_forecast, path, file_name='changes_forecast')
    # save_data(city_years_age_ratio, path, file_name='city_forecast_years_age_ratio')

    print('Выполнено: расчет прогноза изменения численности населения\n')

    return changes_forecast, city_years_age_ratio


if __name__ == '__main__':
    pass
