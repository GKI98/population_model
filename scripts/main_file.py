# last
import os
import shutil

from scripts import city_population_forecast
from scripts import changes_forecast_coef
from scripts import process_data
from scripts import houses_soc_age
from scripts import houses_soc
from scripts import balance_houses
from scripts import push_to_db


# def check_dir_existence(dir_path='./Output_data') -> None:
#     if dir_path and dir_path[-1] == '/':
#         dir_path = dir_path[:-1]
#
#     if not dir_path:
#         dir_path = './Output_data'
#
#     dir_exists = os.path.exists(dir_path)
#     if not dir_exists:
#         os.makedirs(dir_path)


def del_tmp_files(path='') -> None:
    folder_one_path = './population_model'
    if os.path.exists(folder_one_path) and os.path.exists(f'{folder_one_path}/.idea'):
        shutil.rmtree(folder_one_path)

    # if not path:
    #     folder_two_path = './Output_data'
    # else:
    #     folder_two_path = path
    #
    # if os.path.exists(folder_two_path):
    #     shutil.rmtree(folder_two_path)


def make_calc(args, path='', year=2023, set_population=0):
    city_forecast_df = city_population_forecast.main(path=path)
    changes_forecast_df, city_forecast_years_age_ratio_df = changes_forecast_coef.main(city_forecast=city_forecast_df, path=path)
    mun_soc, mun_age_sex_df, adm_age_sex_df, mun_soc_allages_sum = process_data.main(year=year, changes_forecast_df=changes_forecast_df,
                                                                city_forecast_years_age_ratio_df=city_forecast_years_age_ratio_df,
                                                                city_population_forecast_df=city_forecast_df,
                                                                path=path, set_population=set_population, args=args)
    city_forecast_df = None
    changes_forecast_df = None
    city_forecast_years_age_ratio_df = None
    adm_age_sex_df = None

    df = balance_houses.main(args, mun_age_sex_df, path=path)
    df = houses_soc.main(df_mkd_balanced_mo=df, mun_soc_allages_sum=mun_soc_allages_sum, path=path)
    df = houses_soc_age.main(houses_soc=df, mun_soc=mun_soc, args=args, path=path)
    push_to_db.main(args=args, houses_df=df)


def main(args):
    year = args.year
    set_population = args.population
    # path = args.path

    # check_dir_existence(path)
    make_calc(args=args, year=year, set_population=set_population)
    del_tmp_files()


if __name__ == '__main__':
    pass
