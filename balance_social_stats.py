import os
from typing import Literal, Optional

import click
import pandas as pd
from loguru import logger
from tqdm import tqdm

from scripts import (balance_houses, changes_forecast_coef,
                     city_population_forecast, houses_soc, houses_soc_age,
                     process_data, save_db)
from scripts.data_reader_csv import CSVReader
from scripts.data_reader_factory import DataReaderFactory
from scripts.save_csv import Saver


@click.command('balance_social_stats')
@click.option('--db_addr', '-H', envvar='DB_ADDR', default='localhost', type=str, show_default=True, show_envvar=True,
        help='Postgres DBMS address')
@click.option('--db_port', '-P', envvar='DB_PORT', default=5432, type=int, show_default=True, show_envvar=True,
        help='Postgres DBMS port number')
@click.option('--db_name', '-D', envvar='DB_NAME', default='city_db_final', type=str, show_default=True, show_envvar=True,
        help='Postgres database name')
@click.option('--db_user', '-U', envvar='DB_USER', default='postgres', type=str, show_default=True, show_envvar=True,
        help='Postgres user')
@click.option('--db_pass', '-W', envvar='DB_PASS', default='postgres', type=str, show_default=True, show_envvar=True,
        help='Postgres user password')
@click.option('--year', '-y', envvar='YEAR', required=True, type=int, show_envvar=True, help='Year for calculations')
@click.option('--city_id', '-c', envvar='CITY_ID', default=1, type=int, show_envvar=True, help='City_id for calculations')
@click.option('--set_population', '-p', envvar='SET_POPULATION', default=0, show_default='non-set', show_envvar=True,
        help='Set city summary population for the year')
@click.option('--scenario', '-s', envvar='SCENARIO', default='mod', type=click.Choice(('pos', 'mod', 'neg')),
        show_default=True, show_envvar=True, help='Set the scenario for calculations')
@click.option('--local_input_path', '-i', envvar='LOCAL_INPUT_PATH', default=None, type=click.Path(exists=True, dir_okay=True),
        show_default='non-set', show_envvar=True,
        help='Input files from the given folder (must contain files {}) instead of the database. This option overrides database settings'\
                .format(', '.join(list(map(lambda x: f'"{x}"', CSVReader.needed_files)))))
@click.option('--local_output_file', '-o', envvar='LOCAL_OUTPUT_FILE', default=None, type=click.Path(exists=False, file_okay=True),
        show_default='non-set', show_envvar=True,
        help='Output balanced results to the given file  in csv format (file must not exist).'
                ' This options DOES NOT disable saving to the database UNTIL --local_input_path was set')
@click.option('--no_save_to_db', '-ndb', envvar='NO_SAVE_TO_DB', is_flag=True, help='Disable saving to the database (--local_output_file must be set)')
@click.option('--population_changes_path', '-dp', envvar='POPULATION_CHANGES_PATH', type=click.Path(exists=True, file_okay=True),
        default='input_data/changes_population.csv', show_default=True, show_envvar=True, help='Path to population changes document')
@click.option('--migration_coefficients_path', '-dm', envvar='MIGRATION_COEFFICIENTS_PATH', type=click.Path(exists=True, file_okay=True),
        default='input_data/coef_migrations.csv', show_default=True, show_envvar=True, help='Path to migration coefficients document')
@click.option('--base_population_year', '-b', envvar='BASE_POPULATION_YEAR', required=True, type=int, show_envvar=True,
        help='Base year from which calculations are performed. Must be in input files/tables')
def main(db_addr: str, db_port: int, db_name: str, db_user: str, db_pass: str, year: int, city_id: int, set_population: Optional[int],
        scenario: Literal['pos', 'mod', 'neg'], local_input_path: Optional[str], local_output_file: Optional[str], no_save_to_db: bool,
        population_changes_path: str, migration_coefficients_path: str, base_population_year: int):
    '''Calculate the number of people living in the given city houses according to inner+outer territories statistics'''
    if local_output_file is not None:
        local_output_dir, local_output_file = os.path.split(local_output_file)
    else:
        local_output_dir = None
    if local_output_dir is None and no_save_to_db:
        logger.error('No saving options are enabled, exiting with no calculations')
        exit(1)
    if local_input_path is not None and not no_save_to_db:
        logger.warning('As the data was imported locally, no changed to the database will be made. Setting --no_save_to_db')
        no_save_to_db = True
    if local_output_dir is not None and not os.path.exists(local_output_dir):
        try:
            os.makedirs(local_output_dir)
            logger.warning('Папка для выходного файла ("{}") не существует, была создана', local_output_dir)
        except Exception as ex:
            logger.error('Невозможно создать папку ("{}") - {!r}', local_output_dir, ex)

    migration_coefficients_full = pd.read_csv(migration_coefficients_path)
    if migration_coefficients_full[migration_coefficients_full.columns[0]].dtype == object:
        migration_coefficients_full = migration_coefficients_full.drop(migration_coefficients_full.columns[0], axis=1)
    migration_coefficients = migration_coefficients_full.iloc[0]
    del migration_coefficients_full
    
    population_changes = pd.read_csv(population_changes_path, index_col=0)
    city_forecast_df = city_population_forecast.main(city_id, scenario, year, population_changes, migration_coefficients)

    changes_forecast_df, city_forecast_years_age_ratio_df = changes_forecast_coef.main(city_forecast_df, base_population_year)

    if local_input_path is not None:
        reader = DataReaderFactory.from_files(local_input_path)
    else:
        reader = DataReaderFactory.from_database(db_addr, db_port, db_name, db_user, db_pass, city_id)
    mun_soc, mun_age_sex_df, adm_age_sex_df, mun_soc_allages_sum = \
        process_data.main(reader, changes_forecast_df, city_forecast_df, year, set_population, base_population_year)

    # Удаление использованных таблиц для освобождения памяти
    del city_forecast_df, changes_forecast_df, city_forecast_years_age_ratio_df, adm_age_sex_df

    df = balance_houses.main(reader, mun_age_sex_df)

    del mun_age_sex_df

    df = houses_soc.main(df, mun_soc_allages_sum)

    municipalities_values = houses_soc_age.main(year, set_population, scenario, df, mun_soc)

    logger.info('Выполнено распределение жителей домиков (по соц. группам) по возрастам')
    if local_output_dir is not None:
        logger.info('Выполняется сохранение в локальные файлы')
        saver = Saver(local_output_dir)
        for mun in list(municipalities_values.keys()):
            saver.df_to_csv(municipalities_values[mun], mun)
            del municipalities_values[mun]
        saver.save(local_output_file)

    if local_input_path is None and not no_save_to_db:
        logger.info('Выполняется обновление в БД')
        for mun, df in tqdm(municipalities_values.items(), desc='Обновление населения по домам МО'):
            save_db.main(db_addr, db_port, db_name, db_user, db_pass, df)
    
    logger.success('Балансировка сценария "{}" для {} года завершена', scenario, year)


if __name__ == '__main__':
    main()
