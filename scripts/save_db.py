# 0
# import pandas as pd
import psycopg2
from scripts.connect_db import Properties
from more_itertools import sliced


def insert_sex_age_social_houses(db_addr, db_port, db_name, db_user, db_pass, df):
    df1 = df[['year', 'scenario', 'house_id', 'social_group_id', 'age', 'men', 'women',
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

    conn = Properties.connect(db_addr, db_port, db_name, db_user, db_pass)
    with conn, conn.cursor() as cur:
        cur.execute(create_query)

        query_update = "UPDATE social_stats.sex_age_social_houses SET men = %s, women=%s, men_rounded=%s, " \
                       "women_rounded=%s WHERE year=%s and scenario=%s " \
                       "and house_id=%s and social_group_id=%s and age=%s " \

        query_insert = f"INSERT INTO social_stats.sex_age_social_houses (year, scenario, house_id, social_group_id," \
                       f" age, men, women, men_rounded, women_rounded) VALUES  (%s, %s, %s, %s, %s, %s, %s, %s, %s)"

        # query = f"INSERT INTO social_stats.sex_age_social_houses (year, scenario, house_id, social_group_id, age, " \
        #         f"men, women, men_rounded, women_rounded) " \
        #         f"VALUES  (%s, %s, %s, %s, %s, %s, %s, %s, %s) " \
        #         f"ON CONFLICT (year, scenario, house_id, social_group_id, age) " \
        #         f"DO UPDATE SET (men, women, men_rounded, women_rounded) = " \
        #         f"(EXCLUDED.men, EXCLUDED.women, EXCLUDED.men_rounded, EXCLUDED.women_rounded);"

        chunk_size = 10000
        index_slices = sliced(range(len(df1)), chunk_size)

        for index_slice in index_slices:
            chunk = df1.iloc[index_slice]
            tuples = [tuple(x) for x in chunk.to_numpy()]
            for t in tuples:
                try:

                    cur.execute(query_update, (t[5], t[6], t[7], t[8], t[0], t[1], t[2], t[3], t[4]))
                    if cur.rowcount == 0:
                        # print(f'sex_age_social_houses: {t[6], t[5], t[8], t[7], t[0], t[1], t[2], t[3], t[4]}'
                        #       f' \n rowcount: {cur.rowcount}')
                        cur.execute(query_insert, t)
                except (Exception, psycopg2.DatabaseError) as e:
                    print("Error: %s" % e)
                    raise e

    del index_slices, tuples, chunk, df1


def insert_population_houses(db_addr, db_port, db_name, db_user, db_pass, df):
    df2 = df[['year', 'scenario', 'house_id', 'document_population', 'max_population', 'resident_number']]

    create_query = \
        f'''
        CREATE TABLE IF NOT EXISTS social_stats.sex_age_social_houses (
        year int NOT NULL,
        scenario varchar NOT NULL,
        house_id int NOT NULL REFERENCES functional_objects(id),
        document_population int,
        max_population int,
        population int,     
        primary key(year, scenario, house_id)
        );
        '''

    conn = Properties.connect(db_addr, db_port, db_name, db_user, db_pass)
    with conn, conn.cursor() as cur:
        cur.execute(create_query)

        query_update = "UPDATE social_stats.population_houses SET document_population = %s, max_population=%s, " \
                       "population=%s WHERE year=%s and scenario=%s and house_id=%s"

        query_insert = f"INSERT INTO social_stats.population_houses (year, scenario, house_id, document_population, " \
                       f"max_population, population) VALUES  (%s, %s, %s, %s, %s, %s)"

        # query = f"INSERT INTO social_stats.population_houses (year, scenario, house_id, document_population, " \
        #         f"max_population, population " \
        #         f"VALUES  (%s, %s, %s, %s, %s, %s) " \
        #         f"ON CONFLICT (year, scenario, house_id) " \
        #         f"DO UPDATE SET (document_population, max_population, population) = " \
        #         f"(EXCLUDED.document_population, EXCLUDED.max_population, EXCLUDED.population);"

        chunk_size = 10000
        index_slices = sliced(range(len(df2)), chunk_size)

        for index_slice in index_slices:
            chunk = df2.iloc[index_slice]
            tuples = [tuple(x) for x in chunk.to_numpy()]
            for t in tuples:
                try:
                    cur.execute(query_update, (t[3], t[4], t[5], t[0], t[1], t[2]))
                    if cur.rowcount == 0:
                        # print(f'population_houses: {t[3], t[4], t[5], t[0], t[1], t[2]} \n rowcount: {cur.rowcount}')
                        cur.execute(query_insert, t)
                except (Exception, psycopg2.DatabaseError) as e:
                    print("Error: %s" % e)
                    raise e

    del index_slices, tuples, chunk, df2


def main(db_addr, db_port, db_name, db_user, db_pass, houses_df):
    insert_sex_age_social_houses(db_addr, db_port, db_name, db_user, db_pass, houses_df)
    insert_population_houses(db_addr, db_port, db_name, db_user, db_pass, houses_df)


if __name__ == '__main__':
    pass
    # test_df = pd.read_csv('/home/gk/Desktop/output_data/data.csv', nrows=10)
    # main(db_addr='10.32.1.101', db_port=5432, db_name='city_db_final', db_user='postgres', db_pass='postgres',
    #      houses_df=test_df)
    # print('done')