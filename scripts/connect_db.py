# 0

import psycopg2


class Properties:
    @staticmethod
    def connect(db_addr: str = '127.0.0.1', db_port: int = 5432, db_name: str = 'city_db_final',
                db_user: str = 'gk', db_pass: str = 'postgres'):
        print('\nВ процессе: соединение с БД')

        conn = psycopg2.connect(host=db_addr, port=db_port, dbname=db_name, user=db_user, password=db_pass,
                                options="-c search_path=maintenance,provision,public,social_stats,topology")

        print('Выполнено: соединение с БД\n')
        return conn
