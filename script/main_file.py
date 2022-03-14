# last
import os
import shutil

import city_population_forecast
import changes_forecast_coef
import process_data
import houses_soc_age
import houses_soc
import balance_houses
import push_to_db


def check_dir_existence(dir_path='./Output_data') -> None:
    if dir_path and dir_path[-1] == '/':
        dir_path = dir_path[:-1]

    if not dir_path:
        dir_path = './Output_data'

    dir_exists = os.path.exists(dir_path)
    if not dir_exists:
        os.makedirs(dir_path)


def del_tmp_files(path='') -> None:
    folder_one_path = './population_model'
    if os.path.exists(folder_one_path) and os.path.exists(f'{folder_one_path}/.idea'):
        shutil.rmtree(folder_one_path)

    if not path:
        folder_two_path = './Output_data'
    else:
        folder_two_path = path

    if os.path.exists(folder_two_path):
        shutil.rmtree(folder_two_path)


def make_calc(args, path='', year=2023, set_population=0):
    city_population_forecast.main(path=path)
    changes_forecast_coef.main(path=path)
    process_data.main(year=year, path=path, set_population=set_population, args=args)
    df_mkd_balanced_mo = balance_houses.main(args, path=path)
    houses_soc_df = houses_soc.main(df_mkd_balanced_mo=df_mkd_balanced_mo, path=path)
    df = houses_soc_age.main(houses_soc_df, args=args, path=path)
    push_to_db.main(args=args, df=df)


def main(args):
    year = args.year
    set_population = args.population
    path = args.path

    check_dir_existence(path)
    make_calc(args, path, year, set_population)
    del_tmp_files(path)


if __name__ == '__main__':
    pass
