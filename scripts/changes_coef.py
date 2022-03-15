# 0.2

# import pandas as pd


def main(changes_forecast, city_forecast_years_age_ratio, city_population_forecast, year=2020, path=''):
    print('В процессе: расчет коэффициентов изменения численности населения')

    # path = '/home/gk/code/tmppycharm/ifmo_1/scripts/data/'

    # changes_forecast = pd.read_csv(f'{path}/changes_forecast.csv')
    # city_forecast_years_age_ratio = pd.read_csv(f'{path}/city_forecast_years_age_ratio.csv')

    # Изменение в прогнозируемой численности в сравнении с 2019 годом (отношение к численности в 2019 по возрастам)
    coef_ages = changes_forecast[year]

    # Состав населения в % в прогнозируемом году
    year_ratio = city_forecast_years_age_ratio[year]

    # city_population_forecast = pd.read_csv(f'{path}/city_population_forecast.csv').drop(columns=['Unnamed: 0'])
    population_sum = city_population_forecast.sum()

    change_coef = population_sum[year] / population_sum[2019]

    print('Выполнено: расчет коэффициентов изменения численности населения')

    return coef_ages, year_ratio, change_coef


if __name__ == '__main__':
    # main()
    pass

