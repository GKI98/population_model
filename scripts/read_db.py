from scripts.connect_db import Properties
import pandas as pd


class DBReader:

    @staticmethod
    def get_columns(cur, query: str):
        cur.execute(f'{query} LIMIT 0')
        col_names = [col.name for col in cur.description]

        return col_names

    @staticmethod
    def get_table(cur, query: str, set_index_id: bool = False):
        cur.execute(query)
        if set_index_id:
            print(DBReader.get_columns(cur, query))
            df = pd.DataFrame(cur.fetchall(), columns=DBReader.get_columns(cur, query)).set_index('id')
        else:
            df = pd.DataFrame(cur.fetchall(), columns=DBReader.get_columns(cur, query))

        return df

    @staticmethod
    def get_from_db(args):
        conn = Properties.connect(args.db_addr, args.db_port, args.db_name, args.db_user, args.db_pass)

        with conn, conn.cursor() as cur:
            
            if args.city == 5:
                adms = (85,86,87,88)
                extra_condition = f'and administrative_unit_id in {adms}'
            else:
                extra_condition = ''
                
            # houses
            houses_q =f'SELECT f.id, p.municipality_id, p.administrative_unit_id, ' \
                       f'b.resident_number, b.storeys_count, b.failure, ' \
                       f'CASE WHEN b.living_area IS NOT NULL THEN b.living_area ' \
                       f'ELSE ST_Area(geometry::geography) * 0.61212 * b.storeys_count END AS living_area ' \
                       f'FROM buildings b ' \
                       f'JOIN functional_objects f ON b.physical_object_id = f.physical_object_id ' \
                       f'JOIN physical_objects p ON b.physical_object_id = p.id ' \
                       f'WHERE p.city_id = {args.city} AND f.city_service_type_id = ' \
                       f'(SELECT id FROM city_service_types WHERE code = \'houses\') ' \
                       f'{extra_condition}'
    
            cur.execute(houses_q)
            houses_df = pd.DataFrame(cur.fetchall(), columns=DBReader.get_columns(cur, query=houses_q))
            houses_df = houses_df[houses_df['living_area'] > 0]
            houses_df['failure'].fillna(False, inplace=True)
            
            # administrative_units
            adm_total_q = f'SELECT id, name, population FROM administrative_units WHERE city_id = {args.city}'
            adm_total_df = DBReader.get_table(cur, adm_total_q)

            # municipalities
            mun_total_q = f'SELECT id, admin_unit_parent_id, name, population FROM municipalities WHERE city_id={args.city}'
            mun_total_df = DBReader.get_table(cur, mun_total_q)

            # age_sex_administrative_units
            adm_age_sex_q = 'SELECT * FROM age_sex_administrative_units'
            adm_age_sex_df = DBReader.get_table(cur, adm_age_sex_q).sort_values(by=['age'])

            # age_sex_municipalities
            mun_age_sex_q = 'SELECT * FROM age_sex_municipalities'
            mun_age_sex_df = DBReader.get_table(cur, mun_age_sex_q).sort_values(by=['age']).sort_values(by=['age'])

            # age_sex_social_administrative_units
            soc_adm_age_sex_q = 'SELECT * FROM age_sex_social_administrative_units'
            soc_adm_age_sex_df = DBReader.get_table(cur, soc_adm_age_sex_q).sort_values(by=['age'])

            city_division_type = DBReader.get_table(cur, f'SELECT city_division_type FROM cities WHERE id={args.city}').values[0][0]

            if city_division_type != 'ADMIN_UNIT_PARENT':
                adm_total_df, mun_total_df = mun_total_df, adm_total_df
                adm_age_sex_df, mun_age_sex_df = mun_age_sex_df, adm_age_sex_df

            return adm_total_df, mun_total_df, adm_age_sex_df, mun_age_sex_df, soc_adm_age_sex_df, houses_df
