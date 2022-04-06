# 0

import psycopg2


class Properties:
    @staticmethod
    def connect(db_addr: str = '10.32.1.62', db_port: int = 5432, db_name: str = 'city_db_final',
                db_user: str = 'postgres', db_pass: str = 'postgres'):

        conn = psycopg2.connect(host=db_addr, port=db_port, dbname=db_name, user=db_user, password=db_pass,
                                options="-c search_path=maintenance,provision,public,social_stats,topology")

        return conn
