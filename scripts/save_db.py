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
        city_id int NOT NULL,
        year int NOT NULL,
        set_population int NOT NULL,
        house_id int NOT NULL,
        document_population integer,
        max_population integer,
        resident_number integer,
        social_group_id int NOT NULL , 
        age int,
        men real,
        women real,
        men_rounded integer,
        women_rounded integer
        );
        '''

    push_db(args, df, table_name, create_query)


def create_municipality_sex_age_social(args, mun_soc_df, table_name='social_stats.municipality_sex_age_social'):

    create_query = \
        f'''
        CREATE TABLE IF NOT EXISTS {table_name}(
        city_id int NOT NULL,
        year int NOT NULL,
        set_population int NOT NULL,
        municipality_id int NOT NULL,
        age int, 
        social_group_id int NOT NULL,
        men integer,
        women integer
        );
        '''
    push_db(args, mun_soc_df, table_name, create_query)


def chunking(df):
    chunk_size = 10000
    index_slices = sliced(range(len(df)), chunk_size)

    return index_slices, chunk_size


def insert_df(cur, df, table_name):
    cols_lst = list(df.columns)
    cols = ','.join(cols_lst)
    values_space = '%s,' * len(list(df.columns))
    values_space = values_space[:-1]

    set_cols_lst = cols_lst[3:]
    set_cols = ','.join(set_cols_lst)
    excluded_cols_space = ','.join(['EXCLUDED.' + col for col in cols_lst][3:])

    query = f"INSERT INTO  ({cols}) VALUES ({values_space}) " \
            f"ON CONFLICT (city_id, year, set_population) " \
            f"DO UPDATE SET ({set_cols}) = ({excluded_cols_space});"

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


# def drop_tables_if_exist(args, table_name:str):
#     conn = Properties.connect(args.db_addr, args.db_port, args.db_name, args.db_user, args.db_pass)
#
#     with conn, conn.cursor() as cur:
#         cur.execute(f'drop table if exists social_stats.{table_name}')


def main(args, houses_df=pd.DataFrame(), mun_soc_df=pd.DataFrame()):
    if not houses_df.empty:
        sex_age_social_houses(args, houses_df)

    if not mun_soc_df.empty:
        mun_soc_df_new = mun_soc_df.copy()
        mun_soc_df_new = mun_soc_df_new.drop(['admin_unit_parent_id', 'men_age_allmun_percent',
                                              'women_age_allmun_percent', 'total_age_allmun_percent', 'total'], axis=1)

        mun_soc_df_new.insert(0, 'city_id', args.city)
        mun_soc_df_new.insert(1, 'year', args.year)
        mun_soc_df_new.insert(2, 'set_population', args.population)

        create_municipality_sex_age_social(args, mun_soc_df=mun_soc_df_new)


if __name__ == '__main__':
    pass