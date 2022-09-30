from typing import Tuple

import pandas as pd
import psycopg2
from loguru import logger

from scripts.data_reader import DataReader


class DBReader(DataReader):

    def __init__(self, db_addr: str, db_port: int, db_name: str, db_user: str, db_pass: str, city_id: int):
        logger.info('Подключение к базе данных')
        self.conn = psycopg2.connect(host=db_addr, port=db_port, dbname=db_name, user=db_user,
                password=db_pass, connect_timeout=10, application_name='DBReader for balancing_social_stats')
        self.city_id = city_id

    @staticmethod
    def get_table(cur: 'psycopg2.cursor', query: str, *args, set_index_id: bool = False):
        cur.execute(query, args)
        if set_index_id:
            logger.debug(DBReader.get_columns(cur, query))
            df = pd.DataFrame(cur.fetchall(), columns=[d.name for d in cur.description]).set_index('id')
        else:
            df = pd.DataFrame(cur.fetchall(), columns=[d.name for d in cur.description])

        return df

    def get_data(self) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        logger.info('Получение данных из БД')

        with self.conn, self.conn.cursor() as cur:
            # houses
            houses_q = 'SELECT f.id, p.municipality_id, p.administrative_unit_id, b.living_area,' \
                       '    b.resident_number, b.failure FROM buildings b' \
                       ' JOIN functional_objects f ON b.physical_object_id = f.physical_object_id ' \
                       ' JOIN physical_objects p ON b.physical_object_id = p.id ' \
                       ' WHERE b.living_area IS NOT NULL AND p.city_id = %s AND f.city_service_type_id = ' \
                       '    (SELECT id FROM city_service_types WHERE code = %s)'
    
            cur.execute(houses_q, (self.city_id, 'houses'))
            houses_df = pd.DataFrame(cur.fetchall(), columns=[d.name for d in cur.description])

            # administrative_units
            adm_total_q = f'SELECT id, name, population FROM administrative_units WHERE city_id = %s'
            adm_total_df = DBReader.get_table(cur, adm_total_q, self.city_id)

            # municipalities
            mun_total_q = f'SELECT id, admin_unit_parent_id, name, population FROM municipalities WHERE city_id = %s'
            mun_total_df = DBReader.get_table(cur, mun_total_q, self.city_id)

            # age_sex_administrative_units
            adm_age_sex_q = 'SELECT * FROM social_stats.age_sex_administrative_units' \
                    ' WHERE administrative_unit_id IN (SELECT id FROM administrative_units WHERE city_id = %s)'
            adm_age_sex_df = DBReader.get_table(cur, adm_age_sex_q, self.city_id).sort_values(by=['age'])

            # age_sex_municipalities
            mun_age_sex_q = 'SELECT * FROM social_stats.age_sex_municipalities' \
                    ' WHERE municipality_id IN (SELECT id FROM municipalities WHERE city_id = %s)'
            mun_age_sex_df = DBReader.get_table(cur, mun_age_sex_q, self.city_id).sort_values(by=['age']).sort_values(by=['age'])

            # age_sex_social_administrative_units
            soc_adm_age_sex_q = 'SELECT * FROM social_stats.age_sex_social_administrative_units' \
                    ' WHERE administrative_unit_id IN (SELECT id FROM administrative_units WHERE city_id = %s)'
            soc_adm_age_sex_df = DBReader.get_table(cur, soc_adm_age_sex_q, self.city_id).sort_values(by=['age'])

            city_division_type = DBReader.get_table(cur, 'SELECT city_division_type FROM cities WHERE id = %s', self.city_id).values[0][0]

            if city_division_type != 'ADMIN_UNIT_PARENT':
                logger.info('Swapping administrative units and municipalities as city_division_type = {}', city_division_type)
                adm_total_df, mun_total_df = adm_total_df, mun_total_df
                adm_age_sex_df, mun_age_sex_df = adm_age_sex_df, mun_age_sex_df

            return adm_total_df, mun_total_df, adm_age_sex_df, mun_age_sex_df, soc_adm_age_sex_df, houses_df
