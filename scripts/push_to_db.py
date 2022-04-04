# 0
import pandas as pd
import psycopg2
from scripts.connect_db import Properties
from more_itertools import sliced
# import time


def sex_age_social_houses(args, df, table_name='social_stats.sex_age_social_houses'):
    # print('push sex_age_social_houses')
    # REFERENCES functional_objects(id)
    create_query = \
        f'''
        CREATE TABLE IF NOT EXISTS {table_name}(
        house_id int NOT NULL,
        municipality_id int NOT NULL,
        document_population integer,
        max_population integer,
        resident_number integer,
        social_group_id int NOT NULL , 
        age integer,
        men real,
        women real,
        men_rounded integer,
        women_rounded integer
        );
        '''

    push_db(args, df, table_name, create_query)


def create_municipality_sex_age_social(args, mun_soc_df, table_name='social_stats.municipality_sex_age_social'):
    print('create municipality_sex_age_social')

    df = mun_soc_df
    create_query = \
        f'''
        CREATE TABLE IF NOT EXISTS {table_name}(
        age integer,
        municipality_id int NOT NULL , 
        social_group_id int NOT NULL ,
        men integer,
        women integer
        );
        '''
    push_db(args, df, table_name, create_query)


def chunking(df):
    chunk_size = 10000
    index_slices = sliced(range(len(df)), chunk_size)

    return index_slices, chunk_size


def insert_df(cur, df, table_name):
    cols = ','.join(list(df.columns))
    values_space = '%s,' * len(list(df.columns))
    values_space = values_space[:-1]
    query = f"INSERT INTO {table_name} ({cols}) VALUES ({values_space})"

    # print('\nChunking df')
    index_slices, chunk_size = chunking(df)

    for index_slice in index_slices:
        chunk = df.iloc[index_slice]
        tuples = [tuple(x) for x in chunk.to_numpy()]
        try:
            cur.executemany(query, tuples)
        except (Exception, psycopg2.DatabaseError) as e:
            print("Error: %s" % e)
            raise e

    del index_slices
    del tuples
    del chunk
    del df


def push_db(args, df, table_name, create_query):

    conn = Properties.connect(args.db_addr, args.db_port, args.db_name, args.db_user, args.db_pass)

    with conn, conn.cursor() as cur:

        cur.execute(create_query)
        insert_df(cur, df, table_name)

    # print(f'{table_name} успешно добавлена в бд')


def drop_tables_if_exist(args):
    conn = Properties.connect(args.db_addr, args.db_port, args.db_name, args.db_user, args.db_pass)

    with conn, conn.cursor() as cur:
        cur.execute(f'drop table if exists social_stats.municipality_sex_age_social')


def main(args, houses_df=pd.DataFrame(), mun_soc_df=pd.DataFrame()):
    if not houses_df.empty:
        sex_age_social_houses(args, houses_df)

    if not mun_soc_df.empty:
        drop_tables_if_exist(args)

        mun_soc_df_new = mun_soc_df.copy()
        mun_soc_df_new = mun_soc_df_new.drop(['admin_unit_parent_id', 'men_age_allmun_percent',
                                              'women_age_allmun_percent', 'total_age_allmun_percent', 'total'], axis=1)

        create_municipality_sex_age_social(args, mun_soc_df=mun_soc_df_new)


if __name__ == '__main__':
    pass