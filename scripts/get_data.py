# 1

from scripts.connect_db import Properties
import pandas as pd

pd.set_option('display.max_rows', 10)
pd.set_option('display.max_columns', 20)


def get_columns(cur, query: str):
    cur.execute(f'{query} LIMIT 0')
    col_names = [col.name for col in cur.description]

    return col_names


def get_table(cur, query: str, set_index_id: bool = False):
    cur.execute(query)
    if set_index_id:
        print(get_columns(cur, query))
        df = pd.DataFrame(cur.fetchall(), columns=get_columns(cur, query)).set_index('id')
    else:
        df = pd.DataFrame(cur.fetchall(), columns=get_columns(cur, query))

    return df


def main(args):
    print('\nВ процессе: получение данных из БД')
    conn = Properties.connect(args.db_addr, args.db_port, args.db_name, args.db_user, args.db_pass)

    with conn, conn.cursor() as cur:
        # houses
        houses_q = f'SELECT f.id, p.municipality_id, p.administrative_unit_id, b.living_area, ' \
                   f'b.resident_number, b.failure FROM buildings b ' \
                   f'INNER JOIN functional_objects f ON b.physical_object_id = f.physical_object_id ' \
                   f'INNER JOIN physical_objects p ON b.physical_object_id = p.id ' \
                   f'WHERE b.living_area IS NOT NULL AND p.city_id = {args.city}'

        cur.execute(houses_q)
        houses_df = pd.DataFrame(cur.fetchall(), columns=get_columns(cur, query=houses_q))

        # administrative_units
        administrative_units_q = f'SELECT id, name, population FROM administrative_units WHERE city_id = {args.city}'
        administrative_units_df = get_table(cur, administrative_units_q)

        # municipalities
        municipalities_q = f'SELECT id, admin_unit_parent_id, name, population FROM municipalities ' \
                           f'WHERE city_id = {args.city}'
        municipalities_df = get_table(cur, municipalities_q)

        # age_sex_administrative_units
        age_sex_administrative_units_q = 'SELECT * FROM age_sex_administrative_units'
        age_sex_administrative_units_df = get_table(cur, age_sex_administrative_units_q).sort_values(by=['age'])

        # age_sex_municipalities
        age_sex_municipalities_q = 'SELECT * FROM age_sex_municipalities'
        age_sex_municipalities_df = get_table(cur, age_sex_municipalities_q).sort_values(by=['age']).sort_values(
            by=['age'])

        # age_sex_social_administrative_units
        age_sex_social_administrative_units_q = 'SELECT * FROM age_sex_social_administrative_units'
        age_sex_social_administrative_units_df = get_table(cur, age_sex_social_administrative_units_q).sort_values(
            by=['age'])

    print('Выполнено: получение данных из БД')

    return administrative_units_df, municipalities_df, age_sex_administrative_units_df, age_sex_municipalities_df, \
           age_sex_social_administrative_units_df, houses_df
