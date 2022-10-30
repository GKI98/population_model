from scripts import city_population_forecast
from scripts import changes_forecast_coef
from scripts import process_data
from scripts import houses_soc_age
from scripts import houses_soc
from scripts import balance_houses
# from scripts import save_db
# from scripts.save_csv import Saver


# def save_mun_soc(args, mun_soc) -> None:
#     if args.save == 'db':
#         save_db.main(args=args, mun_soc_df=mun_soc)
#
#     elif args.save == 'loc':
#         mun_soc_df_new = mun_soc.copy()
#         mun_soc_df_new = mun_soc_df_new.drop(['admin_unit_parent_id', 'men_age_allmun_percent',
#                                               'women_age_allmun_percent', 'total_age_allmun_percent', 'total'], axis=1)
#
#         mun_soc_df_new.insert(0, 'city_id', args.city)
#         mun_soc_df_new.insert(1, 'year', args.year)
#         mun_soc_df_new.insert(2, 'set_population', args.population)
#
#         Saver.df_to_csv(df=mun_soc_df_new)
#         Saver.cat(name='mun_soc')


def make_calc(args, year, set_population):
    city_forecast_df = city_population_forecast.main(city_id=args.city, scenario=args.scenario, year=args.year)
    # print(city_forecast_df)
    # city_forecast_df.to_csv('city_forecast_df.csv')
    # print(city_forecast_df.sum(axis=0)[-5:])
    # print('PPPPP', city_forecast_df.sum(axis=0)[2025] / city_forecast_df.sum(axis=0)[2021])
    # 1/0

    changes_forecast_df, city_forecast_years_age_ratio_df = changes_forecast_coef.main(city_forecast=city_forecast_df)
    

    # print(changes_forecast_df, '\n')
    # print(city_forecast_years_age_ratio_df)
    # 1/0


    mun_soc, mun_age_sex_df, adm_age_sex_df, mun_soc_allages_sum = \
        process_data.main(args=args, year=year, changes_forecast_df=changes_forecast_df,
                          city_forecast_years_age_ratio_df = city_forecast_years_age_ratio_df,
                          city_population_forecast_df=city_forecast_df,
                          set_population=set_population)

    # save_mun_soc(args, mun_soc)


    # Удаление использованных таблиц для освобождения памяти
    del city_forecast_df
    del changes_forecast_df
    del adm_age_sex_df

    df = balance_houses.main(args, mun_age_sex_df)

    del mun_age_sex_df

    df = houses_soc.main(houses_bal=df, mun_soc_allages_sum=mun_soc_allages_sum)

    houses_soc_age.main(houses_soc=df, mun_soc=mun_soc, args=args)

    print('done!')


def main(args):
    year = args.year
    set_population = args.population
    make_calc(args=args, year=year, set_population=set_population)


if __name__ == '__main__':
    pass
