# 0

from sqlalchemy import create_engine
from connect_db import Properties


def main(args, df):
    db_addr = getattr(args, 'db_addr')
    db_port = getattr(args, 'db_port')
    db_name = getattr(args, 'db_name')
    db_user = getattr(args, 'db_user')
    db_pass = getattr(args, 'db_pass')


    # establish connections
    conn_string = f'postgresql://{db_user}:{db_pass}@{db_addr}/{db_name}'

    db = create_engine(conn_string)
    conn = db.connect()
    conn1 = Properties.connect(db_addr, db_port, db_name, db_user, db_pass)

    conn1.autocommit = True
    cursor = conn1.cursor()

    # drop table if it already exists
    cursor.execute('drop table if exists city_db_final')

    sql = \
        '''
        CREATE TABLE houses_soc_age(
        id int NOT NULL, 
        municipality_id int NOT NULL, 
        administrative_unit_id int NOT NULL,
        living_area float,
        document_population int,
        failure bool,
        max_population int,
        prob_population int,
        resident_number int,
        social_group_id int, 
        age int,
        men float,
        women float,
        total float);
        '''

    cursor.execute(sql)

    # import the csv file to create a dataframe
    # data = {
    #     'id': [1],
    #     'municipality_id': [1],
    #     'administrative_unit_id': [1],
    #     'living_area': [10],
    #     'document_population': [100],
    #     'failure': [False],
    #     'max_population': [11],
    #     'prob_population': [9],
    #     'resident_number': [5],
    #     'social_group_id': [2],
    #     'age': [3],
    #     'men': [1],
    #     'women': [1],
    #     'total': [1]
    # }

    # df = pd.DataFrame(data)

    # converting data to sql
    df.to_sql('houses_soc_age', conn, if_exists='replace')

    # fetching all rows
    sql1 = '''select * from houses_soc_age;'''
    cursor.execute(sql1)
    # for i in cursor.fetchall():
    #     print(i)

    conn1.commit()
    conn1.close()


if __name__ == '__main__':
    pass
