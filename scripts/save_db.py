# 0
import pandas as pd
import psycopg2
from scripts.connect_db import Properties
from more_itertools import sliced


def insert_sex_age_social_houses(args, df):
    df1 = df.loc[['year', 'scenario', 'house_id', 'social_group_id', 'age', 'men', 'women',
                  'men_rounded', 'women_rounded']]

    create_query = \
        f'''
        CREATE TABLE IF NOT EXISTS social_stats.sex_age_social_houses (
        year int NOT NULL,
        scenario varchar NOT NULL,
        house_id int NOT NULL REFERENCES functional_objects(id),
        social_group_id int NOT NULL REFERENCES social_groups(id), 
        age int,
        men real,
        women real,
        men_rounded int,
        women_rounded int,
        primary key(year, scenario, house_id, social_group_id, age)
        );
        '''

    conn = Properties.connect(args.db_addr, args.db_port, args.db_name, args.db_user, args.db_pass)
    with conn, conn.cursor() as cur:
        cur.execute(create_query)

    query = f"INSERT INTO 'social_stats.sex_age_social_houses (year, scenario, house_id, social_group_id, age, " \
            f"men, women, men_rounded, women_rounded) " \
            f"VALUES  (%s %s %s %s %s %s %s %s %s)" \
            f"ON CONFLICT (year, scenario, house_id, social_group_id, age) " \
            f"DO UPDATE SET (men, women, men_rounded, women_rounded) = " \
            f"(EXCLUDED.men, EXCLUDED.women, EXCLUDED.men_rounded, EXCLUDED.women_rounded);"

    chunk_size = 10000
    index_slices = sliced(range(len(df1)), chunk_size)

    for index_slice in index_slices:
        chunk = df1.iloc[index_slice]
        tuples = [tuple(x) for x in chunk.to_numpy()]
        try:
            cur.executemany(query, tuples)
        except (Exception, psycopg2.DatabaseError) as e:
            print("Error: %s" % e)
            raise e

    del index_slices, tuples, chunk, df1


def insert_population_houses(args, df):
    df2 = df.loc[['year', 'scenario', 'house_id', 'document_population', 'max_population', 'resident_number']]

    create_query = \
        f'''
        CREATE TABLE IF NOT EXISTS social_stats.sex_age_social_houses (
        year int NOT NULL,
        scenario varchar NOT NULL,
        house_id int NOT NULL REFERENCES functional_objects(id),
        document_population int,
        max_population int,
        resident_number,     
        primary key(year, scenario, house_id)
        );
        '''

    conn = Properties.connect(args.db_addr, args.db_port, args.db_name, args.db_user, args.db_pass)
    with conn, conn.cursor() as cur:
        cur.execute(create_query)

    query = f"INSERT INTO 'social_stats.population_houses (year, scenario, house_id, document_population, " \
            f"max_population, population" \
            f"VALUES  (%s %s %s %s %s %s)" \
            f"ON CONFLICT (year, scenario, house_id) " \
            f"DO UPDATE SET (document_population, max_population, population) = " \
            f"(EXCLUDED.document_population, EXCLUDED.max_population, EXCLUDED.population);"

    chunk_size = 10000
    index_slices = sliced(range(len(df2)), chunk_size)

    for index_slice in index_slices:
        chunk = df2.iloc[index_slice]
        tuples = [tuple(x) for x in chunk.to_numpy()]
        try:
            cur.executemany(query, tuples)
        except (Exception, psycopg2.DatabaseError) as e:
            print("Error: %s" % e)
            raise e

    del index_slices, tuples, chunk, df2


def main(args, houses_df=pd.DataFrame(), mun_soc_df=pd.DataFrame()):
    if not houses_df.empty:
        insert_sex_age_social_houses(args, houses_df)
        insert_population_houses(args, houses_df)


if __name__ == '__main__':
    pass