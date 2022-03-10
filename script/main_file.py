# last
import os

import city_population_forecast
import changes_forecast_coef
import process_data
import houses_to_ages
import houses_soc
import balance_houses

# import cli


def check_dir_existence(dir_path='./Output_data') -> None:
    if dir_path and dir_path[-1] == '/':
        dir_path = dir_path[:-1]

    if not dir_path:
        dir_path = './Output_data'

    dir_exists = os.path.exists(dir_path)
    if not dir_exists:
        os.makedirs(dir_path)


def del_tmp_files(path='') -> None:
    # os.remove(f'{path}houses_bal.csv')
    os.remove(f'{path}changes_forecast.csv')
    os.remove(f'{path}city_forecast_years_age_ratio.csv')
    os.remove(f'{path}city_population_forecast.csv')
    os.remove(f'{path}houses_soc.csv')
    os.remove(f'{path}mun_age_sex_df.csv')
    os.remove(f'{path}adm_age_sex_df.csv')
    os.remove(f'{path}mun_soc_allages_sum.csv')

    folder_path = './population_model'
    if os.path.exists(folder_path) and os.path.exists(f'{folder_path}/.idea'):
        os.rmdir(folder_path)



def make_calc(args, path='', year=2023, city_id=1, set_population=0):
    city_population_forecast.main(path=path)
    changes_forecast_coef.main(path=path)
    process_data.main(year=year, city_id=city_id, path=path, set_population=set_population, args=args)
    balance_houses.main(args, path=path)
    # houses_soc.main(path=path)
    # houses_to_ages.main(path=path)


def main(args):
    # args = cli.main().args

    year = getattr(args, 'year')
    set_population = getattr(args, 'set_population')
    city_id = getattr(args, 'city_id')
    path = getattr(args, 'path')

    check_dir_existence(path)
    make_calc(args, path, year, city_id, set_population)
    del_tmp_files(path)


if __name__ == '__main__':
    pass
