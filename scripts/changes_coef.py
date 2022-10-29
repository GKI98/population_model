# 0.2
import pandas as pd


def main(args, changes_forecast_df, city_forecast_years_age_ratio_df, city_population_forecast_df, year):
    print('В процессе: расчет коэффициентов изменения численности населения')

    if args.city == 5:

        forecast = pd.read_csv(f'./scripts/Input_data/sev/sev_forecast_{args.scenario}.csv', index_col=[0])
        # coef_ages = pd.Series()
        
        coef_ages = pd.Series(forecast[f'total_{year}'].div(forecast['total_2022']))
        
    # Изменение в прогнозируемой численности в сравнении с 2019 годом (отношение к численности в 2019 по возрастам)
    else: coef_ages = changes_forecast_df[year]

    # Состав населения в % в прогнозируемом году
    # year_ratio = city_forecast_years_age_ratio_df[year]
    # city_population_forecast = pd.read_csv(f'{path}/city_population_forecast.csv').drop(columns=['Unnamed: 0'])
    

    if args.city != 5:
        population_sum = city_population_forecast_df.sum()
        change_coef = population_sum[year] / population_sum[2020]

    elif args.city == 5:
        population_sum = forecast.sum()
        change_coef = population_sum[f'total_{year}'] / population_sum['total_2022']

    # print('Выполнено: расчет коэффициентов изменения численности населения')

    return coef_ages, change_coef


if __name__ == '__main__':
    pass

