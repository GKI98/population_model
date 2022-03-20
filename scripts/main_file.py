from scripts import city_population_forecast
from scripts import changes_forecast_coef
from scripts import process_data
from scripts import houses_soc_age
from scripts import houses_soc
from scripts import balance_houses
from scripts import push_to_db


def make_calc(args, path='', year=2022, set_population=0):
    city_forecast_df = city_population_forecast.main(path=path)
    changes_forecast_df, city_forecast_years_age_ratio_df = changes_forecast_coef.main(city_forecast=city_forecast_df,
                                                                                       path=path)
    mun_soc, mun_age_sex_df, adm_age_sex_df, mun_soc_allages_sum = \
        process_data.main(year=year, changes_forecast_df=changes_forecast_df,
                          city_forecast_years_age_ratio_df=city_forecast_years_age_ratio_df,
                          city_population_forecast_df=city_forecast_df,
                          path=path, set_population=set_population, args=args)

    print(mun_soc)
    push_to_db.main(args=args, mun_soc_df=mun_soc)
    print('MUN_SOC обавлено в БД')

    # Удаление использованных таблиц для освобождения памяти
    del city_forecast_df
    del changes_forecast_df
    del city_forecast_years_age_ratio_df
    del adm_age_sex_df

    df = balance_houses.main(args, mun_age_sex_df, path=path)

    # Уменьшение размерности столбцов для освобождения памяти
    df.municipality_id = df.municipality_id.astype('uint16')
    df.administrative_unit_id = df.administrative_unit_id.astype('uint16')
    df.living_area = df.living_area.astype('float16')
    df.resident_number = df.resident_number.astype('uint16')
    df.max_population = df.max_population.astype('uint16')
    df.prob_population = df.prob_population.astype('uint16')
    df.citizens_reg_bal = df.citizens_reg_bal.astype('uint16')

    df = houses_soc.main(df_mkd_balanced_mo=df, mun_soc_allages_sum=mun_soc_allages_sum, path=path)

    # Уменьшение размерности столбцов для освобождения памяти
    df.document_population = df.document_population.astype('uint16')
    df.mun_percent = df.mun_percent.astype('float16')
    df.social_group_id = df.social_group_id.astype('uint16')
    df.house_total_soc = df.house_total_soc.astype('uint16')
    df.house_men_soc = df.house_men_soc.astype('uint16')
    df.house_women_soc = df.house_women_soc.astype('uint16')

    houses_soc_age.main(houses_soc=df, mun_soc=mun_soc, args=args, path=path)


def main(args):
    year = args.year
    set_population = args.population
    make_calc(args=args, year=year, set_population=set_population)


if __name__ == '__main__':
    pass
