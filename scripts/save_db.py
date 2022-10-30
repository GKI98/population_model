import time
import pandas as pd
import psycopg2
from loguru import logger
from tqdm import tqdm


def insert_sex_age_social_houses(db_addr: str, db_port: int, db_name: str, db_user: str, db_pass: str, df: pd.DataFrame):
    df = df[['year', 'scenario', 'house_id', 'social_group_id', 'age', 'men', 'women',
              'men_rounded', 'women_rounded']]
    
    query_update = 'UPDATE social_stats.sex_age_social_houses SET ' + \
            ', '.join(f'men_{age}=%s' for age in range(0, 101)) + \
            ', ' + \
            ', '.join(f'women_{age}=%s' for age in range(0, 101)) + \
            ' WHERE year=%s and scenario=%s and house_id=%s and social_group_id=%s'

    query_insert = 'INSERT INTO social_stats.sex_age_social_houses (year, scenario, house_id, social_group_id, {}, {})' \
            .format(', '.join(f'men_{age}' for age in range(0, 101)), ', '.join(f'women_{age}' for age in range(0, 101))) + \
                    ' VALUES (%s, %s, %s, %s, {})'.format(', '.join(('%s',) * 202))
    while True:
        try:
            conn = psycopg2.connect(host=db_addr, port=db_port, dbname=db_name, user=db_user, password=db_pass,
                    connect_timeout=10, application_name='Update_social_stats')

            with conn, conn.cursor() as cur:
                cur.execute('CREATE TABLE IF NOT EXISTS social_stats.sex_age_social_houses ('
                        ' id Serial PRIMARY KEY NOT NULL,'
                        ' year smallint NOT NULL,'
                        ' scenario social_stats_scenario NOT NULL,'
                        ' house_id integer REFERENCES functional_objects(id) NOT NULL,'
                        ' social_group_id integer REFERENCES social_groups(id) NOT NULL,' +
                        ', '.join(f'men_{age} smallint NOT NULL' for age in range(0, 101)) +
                        ', ' +
                        ', '.join(f'women_{age} smallint NOT NULL' for age in range(0, 101)) +
                        ', UNIQUE(year, scenario, social_group_id, house_id)'
                ')')

                for (year, scenario, house_id, social_group_id), df_groupped in \
                        tqdm(df.groupby(['year', 'scenario', 'house_id', 'social_group_id']), desc=f'Обновление социального расселения', leave=False):
                    ages = pd.Series(name='ages', index=range(0, 101), dtype=int)
                    df_tmp = df_groupped[['age', 'men', 'women']].set_index('age').join(ages, how='right')
                    men = list(df_tmp['men'].fillna(0))
                    women = list(df_tmp['women'].fillna(0))
                    year = int(year)
                    house_id = int(house_id)
                    social_group_id = int(social_group_id)
                    try:
                        cur.execute(query_update, (*men, *women, year, scenario, house_id, social_group_id))
                        if cur.rowcount == 0:
                            cur.execute(query_insert, (year, scenario, house_id, social_group_id, *men, *women))
                    except Exception as ex:
                        logger.error("Ошибка при сохранении значений расселения в БД: {!r}", ex)
                        raise
        except Exception as ex:
            logger.error('Ошибка при сохранении значений расселения в БД: {}. Повторная попытка через 20 секунд', ex)
            time.sleep(20)


def insert_population_houses(db_addr: str, db_port: int, db_name: str, db_user: str, db_pass: str, df: pd.DataFrame):
    df = df[['year', 'scenario', 'house_id', 'document_population', 'max_population', 'resident_number']].drop_duplicates()

    query_update = 'UPDATE social_stats.population_houses SET document_population = %s, max_population=%s, population=%s' \
            ' WHERE year=%s and scenario=%s and house_id=%s'
    query_insert = 'INSERT INTO social_stats.population_houses (year, scenario, house_id, document_population,' \
            ' max_population, population) VALUES (%s, %s, %s, %s, %s, %s)'

    while True:
        try:
            conn = psycopg2.connect(host=db_addr, port=db_port, dbname=db_name, user=db_user, password=db_pass,
                    connect_timeout=10, application_name='Update_social_stats')

            with conn, conn.cursor() as cur:
                cur.execute('CREATE TABLE IF NOT EXISTS social_stats.population_houses ('
                        ' id Serial PRIMARY KEY NOT NULL,'
                        ' year smallint NOT NULL,'
                        ' scenario social_stats_scenario NOT NULL,'
                        ' house_id integer REFERENCES functional_objects(id) NOT NULL,'
                        ' document_population integer NOT NULL,'
                        ' max_population smallint NOT NULL,'
                        ' population smallint NOT NULL,'
                        ' UNIQUE(year, scenario, house_id)'
                ')')
                
                for _, (year, scenario, house_id, document_population, max_population, resident_number) in df.iterrows():
                    try:
                        cur.execute(query_update, (document_population, max_population, resident_number, year, scenario, house_id))
                        if cur.rowcount == 0:
                            cur.execute(query_insert, (year, scenario, house_id, document_population, max_population, resident_number))
                    except Exception as ex:
                        logger.error("Ошибка при сохранении значений населения в БД: {!r}", ex)
                        raise
                return
        except Exception as ex:
            logger.error('Ошибка при сохранении значений населения в БД: {}. Повторная попытка через 20 секунд', ex)
            time.sleep(20)


def main(db_addr, db_port, db_name, db_user, db_pass, houses_df):
    insert_population_houses(db_addr, db_port, db_name, db_user, db_pass, houses_df)
    insert_sex_age_social_houses(db_addr, db_port, db_name, db_user, db_pass, houses_df)