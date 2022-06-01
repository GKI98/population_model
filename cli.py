import argparse
from scripts import main_file


def main():
    parser = argparse.ArgumentParser(prog='CLI-parser', description='')

    # Group_1 "Connection to DB"
    parser_conn = parser.add_argument_group(title='Connection options')
    # parser_conn.add_argument('--db-addr', nargs='?', const=0, default='172.17.0.1', type=str.lower) # это докер
    parser_conn.add_argument('--db-addr', nargs='?', const=0, default='10.32.1.101', type=str.lower) # это просто база сервера
    # parser_conn.add_argument('--db-addr', nargs='?', const=0, default='127.0.0.1', type=str.lower) # это локальная база

    parser_conn.add_argument('--db-port', nargs='?', const=0, default=5432, type=int)
    parser_conn.add_argument('--db-name', nargs='?', const=0, default='city_db_final', type=str.lower)
    parser_conn.add_argument('--db-user', nargs='?', const=0, default='postgres', type=str.lower)
    parser_conn.add_argument('--db-pass', nargs='?', const=0, default='postgres', type=str.lower)

    # Group_2 "Data args"
    parser_data_info = parser.add_argument_group(title='Data info')
    parser_data_info.add_argument('--year', nargs='?', const=0, default=2022, type=int, help='Год прогнозирования')
    parser_data_info.add_argument('--city-id', dest='city', nargs='?', const=0, default=1, type=int, help='Город')
    parser_data_info.add_argument('--set-population', dest='population', nargs='?', const=0, default=0, type=int,
                                  help='Задать число жителей в год прогнозирования (суммарно по городу)')
    parser_data_info.add_argument('--scenario', '-sc', dest='scenario', default='mod', choices=('pos', 'mod', 'neg'),
                                  help='Сценарий изменения численности населения')
    parser_data_info.add_argument('--round', '-rnd', default=True, action='store_true', help='True, False')

    # Group_3 "Reading/Saving data"
    parser_saver = parser.add_argument_group(title='Reading options')
    parser_saver.add_argument('-read', '-r', dest='read', default=False, action='store_true',
                              help='Откуда брать данные? (локально / бд) dafault: бд')
    parser_saver.add_argument('--folder-path', '--p', dest='path', nargs='?', const=0, default='./outputs', help='Путь до файлов')
    parser_saver = parser.add_argument_group(title='Saving options')
    parser_saver.add_argument('-save', '-s', dest='save', default='db', choices=('db', 'loc'),
                              help='Куда сохранять результат? (локально / бд) dafault: бд')

    args = parser.parse_args()
    main_file.main(args)


if __name__ == '__main__':
    main()
