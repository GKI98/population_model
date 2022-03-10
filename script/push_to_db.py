import psycopg2.extras as extras
import psycopg2
from connect_db import Properties


def execute_values(conn, cur, df, table):
    tuples = [tuple(x) for x in df.to_numpy()]

    cols = ','.join(list(df.columns))
    query = f"INSERT INTO {table} VALUES {cols}"
    try:
        extras.execute_values(cur, query, tuples)

    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cur.close()
        return 1

    print("the dataframe is inserted")
    cur.close()


def main(args, df):
    db_addr = getattr(args, 'db_addr')
    db_port = getattr(args, 'db_port')
    db_name = getattr(args, 'db_name')
    db_user = getattr(args, 'db_user')
    db_pass = getattr(args, 'db_pass')

    conn = Properties.connect(db_addr, db_port, db_name, db_user, db_pass)

    if args.update_in_db:
        with conn, conn.cursor() as cur:
            create_table_houses_soc_age = \
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
            cur.execute(create_table_houses_soc_age)

            df.to_sql('houses_soc_age', con=conn, index=False)
            execute_values(conn=conn, cur=cur, df=df, table='houses_soc_age')
