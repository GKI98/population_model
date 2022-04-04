# 0.2


def main(changes_forecast_df, city_forecast_years_age_ratio_df, city_population_forecast_df, year=2020, path=''):
    print('В процессе: расчет коэффициентов изменения численности населения')

    # Изменение в прогнозируемой численности в сравнении с 2019 годом (отношение к численности в 2019 по возрастам)
    coef_ages = changes_forecast_df[year]

    # Состав населения в % в прогнозируемом году
    year_ratio = city_forecast_years_age_ratio_df[year]

    # city_population_forecast = pd.read_csv(f'{path}/city_population_forecast.csv').drop(columns=['Unnamed: 0'])
    population_sum = city_population_forecast_df.sum()

    change_coef = population_sum[year] / population_sum[2019]

    # print('Выполнено: расчет коэффициентов изменения численности населения')

    return coef_ages, year_ratio, change_coef


if __name__ == '__main__':
    pass

