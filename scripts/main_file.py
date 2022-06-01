from scripts import city_population_forecast
from scripts import changes_forecast_coef
from scripts import process_data
from scripts import houses_soc_age
from scripts import houses_soc
from scripts import balance_houses
from scripts import save_db
from scripts.save_csv import Saver


# def save_mun_soc(args, mun_soc) -> None:
#     if args.save == 'db':
#         save_db.main(args=args, mun_soc_df=mun_soc)
#
#     elif args.save == 'loc':
#         mun_soc_df_new = mun_soc.copy()
#         mun_soc_df_new = mun_soc_df_new.drop(['men_age_allmun_percent',
#                                               'women_age_allmun_percent', 'total_age_allmun_percent', 'total'], axis=1)
#
#         mun_soc_df_new.insert(0, 'city_id', args.city)
#         mun_soc_df_new.insert(1, 'year', args.year)
#         mun_soc_df_new.insert(2, 'set_population', args.population)
#
#         Saver.df_to_csv(df=mun_soc_df_new)
#         Saver.cat(name='mun_soc')


def make_calc(args, path='', year=2022, set_population=0):
    # Прогноз численности населения на заданный год
    city_forecast_df = city_population_forecast.main(city_id=args.city, scenario=args.scenario, year=args.year)

    # Взятие отношений (как коэффициентов) из прогнозируемого года к 2019 (известному году)
    changes_forecast_df, city_forecast_years_age_ratio_df = changes_forecast_coef.main(city_forecast=city_forecast_df,
                                                                                       path=path)

    # Расчет количества жителей в муниципальных образованиях по возрастам и соц. группам
    mun_soc, mun_age_sex_df, mun_soc_allages_sum = \
        process_data.main(year=year, changes_forecast_df=changes_forecast_df,
                          city_forecast_years_age_ratio_df=city_forecast_years_age_ratio_df,
                          city_population_forecast_df=city_forecast_df,
                          path=path, set_population=set_population, args=args)

    # Удаление использованных таблиц для освобождения памяти
    del city_forecast_df
    del changes_forecast_df
    del city_forecast_years_age_ratio_df

    # Сохранение данных об общем количестве житеелй в муниципальных образованиях в прогнозируемом году
    # save_mun_soc(args, mun_soc)

    # расчет максимальной и вероятной численности
    # балансировка общей численности жителей в домах
    df = balance_houses.main(args, mun_age_sex_df, path=path)

    # Удаление использованных таблиц для освобождения памяти
    del mun_age_sex_df

    # Распределение общей численности социальных групп по домам
    df = houses_soc.main(houses_bal=df, mun_soc_allages_sum=mun_soc_allages_sum, path=path)

    # Распределение социальных групп в домах по возрастам
    houses_soc_age.main(houses_soc=df, mun_soc=mun_soc, args=args, path=path)


def main(args):
    year = args.year
    set_population = args.population
    make_calc(args=args, year=year, set_population=set_population)


if __name__ == '__main__':
    pass
