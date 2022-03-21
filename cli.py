import argparse
from scripts import main_file


def main(main_file):

    # Сюда вставить то что сокол предлагал про переменные из среды

    parser = argparse.ArgumentParser(prog='CLI-parser', description='')

    # Group_1 "Connection to DB"
    parser_conn = parser.add_argument_group(title='Connection options')
    # parser_conn.add_argument('--db-addr', nargs='?', const=0, default='172.17.0.1', type=str.lower)
    parser_conn.add_argument('--db-addr', nargs='?', const=0, default='10.32.1.62', type=str.lower)
    # parser_conn.add_argument('--db-addr', nargs='?', const=0, default='127.0.0.1', type=str.lower)

    parser_conn.add_argument('--db-port', nargs='?', const=0, default=5432, type=int)
    parser_conn.add_argument('--db-name', nargs='?', const=0, default='city_db_final', type=str.lower)

    parser_conn.add_argument('--db-user', nargs='?', const=0, default='postgres', type=str.lower)
    # parser_conn.add_argument('--db-user', nargs='?', const=0, default='gk', type=str.lower)

    parser_conn.add_argument('--db-pass', nargs='?', const=0, default='postgres', type=str.lower)

    # Group_2 "Data args"
    parser_data_info = parser.add_argument_group(title='data_info')
    parser_data_info.add_argument('--year', nargs='?', const=0, default=2022, type=int, help='Год прогнозирования')
    parser_data_info.add_argument('--city-id', dest='city', nargs='?', const=0, default=1, type=int, help='Город')
    parser_data_info.add_argument('--set-population', dest='population', nargs='?', const=0, default=0, type=int,
                                  help='Задать число жителей в год прогнозирования (суммарно по городу)')
    # parser_data_info.add_argument('--path', nargs='?', const=0, default='./Output_data', type=str,
    #                               help='Папка сохранения')
    parser_data_info.add_argument('--update-in-db', dest='update', default=False, action='store_true',
                                  help='Update buildings.resident_number in database')

    args = parser.parse_args()
    main_file.main(args)


if __name__ == '__main__':
    main(main_file)








