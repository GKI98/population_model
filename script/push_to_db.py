# 0
import pandas as pd
import psycopg2
from connect_db import Properties


def sex_age_social_houses(args, df, table_name='social_stats.sex_age_social_houses'):
    print('push 1')
    create_query = \
        f'''
        CREATE TABLE {table_name}(
        house_id SERIAL PRIMARY KEY NOT NULL REFERENCES functional_objects(id), 
        municipality_id SERIAL NOT NULL, 
        administrative_unit_id SERIAL NOT NULL,
        living_area real,
        document_population integer,
        failure bool,
        max_population integer,
        prob_population integer,
        resident_number integer,
        social_group_id serial NOT NULL, 
        age integer,
        men integer,
        women integer,
        total integer);
        '''
    push_db(args, df, table_name, create_query)


def create_municipality_sex_age_social(args, table_name='social_stats.municipality_sex_age_social'):
    print('push 4')
    df = pd.read_csv('./Output_data/mun_soc.csv')
    df = df[['admin_unit_parent_id', 'municipality_id', 'age', 'social_group_id', 'men', 'women', 'total']]
    create_query = \
        f'''
        CREATE TABLE {table_name}(
        admin_unit_parent_id serial,
        municipality_id serial,
        age integer,
        social_group_id serial,
        men integer,
        women integer,
        total integer
        );
        '''
    push_db(args, df, table_name, create_query)


def insert_df(cur, df, table_name):
    print('push 2')
    tuples = [tuple(x) for x in df.to_numpy()]
    cols = ','.join(list(df.columns))
    values_space = '%s,' * len(list(df.columns))
    values_space = values_space[:-1]
    query = f"INSERT INTO {table_name} ({cols}) VALUES ({values_space})"
    print(query)

    try:
        cur.executemany(query, tuples)

    except (Exception, psycopg2.DatabaseError) as e:
        print("Error: %s" % e)

        raise e


def push_db(args, df, table_name, create_query):
    print('push 3')

    conn = Properties.connect(args.db_addr, args.db_port, args.db_name, args.db_user, args.db_pass)
    # conn = Properties.connect()

    with conn, conn.cursor() as cur:
        cur.execute(f'drop table if exists {table_name}')
        cur.execute(create_query)
        print('push 2')
        insert_df(cur, df, table_name)

        check_query = f"select * from {table_name} limit 5"
        cur.execute(check_query)
        print('push x')
        records = cur.fetchall(5)

        for row in records:
            print(row)

    print(f'{table_name} успешно добавлена в бд')


def main(args, df):
    print('push 0')
    sex_age_social_houses(args, df)
    create_municipality_sex_age_social(args)


if __name__ == '__main__':
    pass
    # data = {
    #     'house_id': [1, 2],
    #     'municipality_id': [1, 2],
    #     'administrative_unit_id': [1, 2],
    #     'living_area': [1, 2],
    #     'document_population': [1, 2],
    #     'failure': [False, True],
    #     'max_population': [10, 11],
    #     'prob_population': [10, 11],
    #     'resident_number': [10, 11],
    #     'social_group_id': [1, 11],
    #     'age': [10, 11],
    #     'men': [1.0, 2.0],
    #     'women': [1.0, 2.0],
    #     'total': [2.0, 3.0]
    # }
    # df = pd.DataFrame(data)
    # main(df)
