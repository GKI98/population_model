# 0.2
import pandas as pd


def main(args, changes_forecast_df, city_forecast_years_age_ratio_df, city_population_forecast_df, year):
    print('В процессе: расчет коэффициентов изменения численности населения')

    if args.city_id == 5:
        forecast = pd.read_csv(f'./scripts/Input_data/sev/sev_forecast_{args.scenario}.csv', index=False)
        coef_ages = pd.DataFrame()
        for col in forecast.columns:
            coef_ages[col] = coef_ages[col].div(coef_ages[2022])
        coef_ages = coef_ages[year]
    # Изменение в прогнозируемой численности в сравнении с 2019 годом (отношение к численности в 2019 по возрастам)
    else: coef_ages = changes_forecast_df[year]

    # Состав населения в % в прогнозируемом году
    # year_ratio = city_forecast_years_age_ratio_df[year]

    # city_population_forecast = pd.read_csv(f'{path}/city_population_forecast.csv').drop(columns=['Unnamed: 0'])
    

    if args.city_id != 5:
        population_sum = city_population_forecast_df.sum()
        change_coef = population_sum[year] / population_sum[2019]
    else:
        population_sum = forecast.sum()
        change_coef = population_sum[year] / population_sum[2022]

    # print('Выполнено: расчет коэффициентов изменения численности населения')

    return coef_ages, change_coef


if __name__ == '__main__':
    pass

