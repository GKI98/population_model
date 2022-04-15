# 0
import pandas as pd
import psycopg2
from scripts.connect_db import Properties
from more_itertools import sliced


def sex_age_social_houses(args, df, table_name='social_stats.sex_age_social_houses'):

    create_query = \
        f'''
        CREATE TABLE IF NOT EXISTS {table_name}(
        year int NOT NULL,
        set_population int NOT NULL,
        scenario varchar NOT NULL,
        house_id int NOT NULL REFERENCES functional_objects(id),
        document_population integer,
        max_population integer,
        resident_number integer,
        social_group_id int NOT NULL REFERENCES social_groups(id), 
        age int,
        men real,
        women real,
        men_rounded integer,
        women_rounded integer,
        primary key(
        year, set_population, scenario, house_id, social_group_id, age)
        );
        '''

    push_db(args, df, table_name, create_query)


def create_municipality_sex_age_social(args, mun_soc_df, table_name='social_stats.municipality_sex_age_social'):

    create_query = \
        f'''
        CREATE TABLE IF NOT EXISTS {table_name}(
        year int NOT NULL,
        set_population int NOT NULL,
        scenario varchar NOT NULL,
        municipality_id int NOT NULL REFERENCES municipalities(id),
        age int, 
        social_group_id int NOT NULL REFERENCES social_groups(id),
        men integer,
        women integer,
        primary key(
        year, set_population, scenario, municipality_id, social_group_id, age)
        );
        '''
    push_db(args, mun_soc_df, table_name, create_query)


def chunking(df):
    chunk_size = 10000
    index_slices = sliced(range(len(df)), chunk_size)

    return index_slices, chunk_size


def insert_df(cur, df, table_name):
    cols = ','.join(list(df.columns))
    values_space = '%s,' * len(list(df.columns))
    values_space = values_space[:-1]

    if table_name == 'social_stats.sex_age_social_houses':
        special_constraint = 'house_id,'
        set_cols_lst = ['document_population', 'max_population', 'resident_number', 'men', 'women', 'men_rounded',
                        'women_rounded']
        set_cols = ','.join(set_cols_lst)
        excluded_cols_space = ','.join(['EXCLUDED.' + col for col in set_cols_lst])

        query = f"INSERT INTO {table_name} ({cols}) VALUES ({values_space}) " \
                f"ON CONFLICT (year, set_population, scenario, {special_constraint} social_group_id, age) " \
                f"DO UPDATE SET ({set_cols}) = ({excluded_cols_space});"

    elif table_name == 'social_stats.municipality_sex_age_social':
        special_constraint = 'municipality_id,'
        set_cols_lst = ['men', 'women']
        set_cols = ','.join(set_cols_lst)
        excluded_cols_space = ','.join(['EXCLUDED.' + col for col in set_cols_lst])

        query = f"INSERT INTO {table_name} ({cols}) VALUES ({values_space}) " \
                f"ON CONFLICT (year, set_population, scenario, {special_constraint} social_group_id, age) " \
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

        mun_soc_df_new.insert(0, 'year', args.year)
        mun_soc_df_new.insert(1, 'set_population', args.population)
        mun_soc_df_new.insert(2, 'scenario', args.scenario)

        create_municipality_sex_age_social(args, mun_soc_df=mun_soc_df_new)


if __name__ == '__main__':
    pass