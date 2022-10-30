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
                extra_condition2 = f'and id in {adms}'

            elif args.city == 2:
                adms = (64,65,66,67,68)
                extra_condition = f'and administrative_unit_id in {adms}'
                extra_condition2 = f'and id in {adms}'
            else:
                extra_condition = ''
                extra_condition2 = ''
            
            # administrative_units
            # print('adm_total_df')
            adm_total_q = f'SELECT id, name, population, municipality_parent_id ' \
                          f'FROM administrative_units '\
                          f'WHERE city_id = {args.city} {extra_condition2}'

            adm_total_df = DBReader.get_table(cur, adm_total_q)

            

            # municipalities
            # print('mun_total_df')
            mun_total_q = f'SELECT id, admin_unit_parent_id, name, population ' \
                          f'FROM municipalities ' \
                          f'WHERE city_id={args.city}'
            mun_total_df = DBReader.get_table(cur, mun_total_q)

            # print(mun_total_df)
            # 1/0
            
            # 1/0

            if args.city in (2, 5):
                adm_age_sex_df = pd.read_csv(f'./scripts/Input_data/{args.city}/{args.city}_age_sex_administrative_units.csv', index_col=0)
                mun_age_sex_df = pd.read_csv(f'./scripts/Input_data/{args.city}/{args.city}_age_sex_municipalities.csv', index_col=0)
                
                adm_age_sex_df = adm_age_sex_df[adm_age_sex_df['administrative_unit_id'].isin(adms)]

            else:
                # age_sex_administrative_units
                # print('age_sex_administrative_units')
                adm_age_sex_q = 'SELECT * FROM age_sex_administrative_units'
                adm_age_sex_df = DBReader.get_table(cur, adm_age_sex_q).sort_values(by=['age'])

                # age_sex_municipalities
                # print('age_sex_municipalities')
                mun_age_sex_q = 'SELECT * FROM age_sex_municipalities'
                mun_age_sex_df = DBReader.get_table(cur, mun_age_sex_q).sort_values(by=['age']).sort_values(by=['age'])

            

            city_division_type = DBReader.get_table(cur, f'SELECT city_division_type FROM cities WHERE id={args.city}').values[0][0]

            if city_division_type != 'ADMIN_UNIT_PARENT':
                # print(mun_age_sex_df)
                # print(adm_age_sex_df)
                # print(mun_total_df)
                # print(adm_total_df)
                
                adm_total_df.rename(columns={'administrative_unit_id': 'municipality_id', \
                                    'municipality_parent_id': 'admin_unit_parent_id'}, inplace=True)
                adm_age_sex_df.rename(columns={'administrative_unit_id': 'municipality_id', \
                                    'municipality_parent_id': 'admin_unit_parent_id'}, inplace=True)

                mun_total_df.rename(columns={'municipality_parent_id': 'admin_unit_parent_id', \
                                    'admin_unit_parent_id': 'municipality_parent_id' }, inplace=True)
                mun_age_sex_df.rename(columns={'municipality_id': 'administrative_unit_id', \
                                    'admin_unit_parent_id': 'municipality_parent_id'}, inplace=True)

                adm_total_df, mun_total_df = mun_total_df, adm_total_df
                adm_age_sex_df, mun_age_sex_df = mun_age_sex_df, adm_age_sex_df

                # print(mun_age_sex_df)
                # 1/0


                units = adm_total_df["id"].unique()
                if len(units) == 1:
                    soc_adm_age_sex_q = f'SELECT * FROM age_sex_social_municipalities where municipality_id in ({units[0]})'
                    
                else:
                    soc_adm_age_sex_q = f'SELECT * FROM age_sex_social_municipalities where municipality_id in ({tuple(units)})'

                soc_adm_age_sex_df = DBReader.get_table(cur, soc_adm_age_sex_q).sort_values(by=['age'])
                soc_adm_age_sex_df.rename(columns={'municipality_id':'administrative_unit_id'}, inplace=True)
                

                

                # print('houses')
                
                if args.city == 2:
                    # houses_df = pd.read_csv('/home/gk/Desktop/krd_houses.csv')
                houses_q =f'SELECT f.id, p.municipality_id as administrative_unit_id, p.administrative_unit_id as municipality_id, ' \
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

                houses_df = houses_df[houses_df['living_area']>0]
                houses_df['failure'].fillna(False, inplace=True)


                if args.city == 2:
                    mun_total_df = mun_total_df[mun_total_df['id'].isin(adms)]
                    # print('mun_total_df', mun_total_df.sum())
                    adm_total_df['population'] = mun_total_df['population'].sum()
                    # print(adm_total_df['population'])
                

                

                # print(mun_age_sex_df)
                # print(mun_total_df, '\n\n\n\n\n')

            else:
                # print('houses')
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

                if args.city == 5:
                    houses_df = pd.read_csv('/home/gk/Desktop/sev_houses.csv')
                houses_df = houses_df[houses_df['living_area']>0]
                houses_df['failure'].fillna(False, inplace=True)
                

                # age_sex_social_administrative_units
                # print('age_sex_social_administrative_units')
                
                

                if args.city == 5:
                    # print('soc_adm_age_sex_q')
                    soc_adm_age_sex_q = f'SELECT * FROM age_sex_social_administrative_units where administrative_unit_id in {adms}'
                    soc_adm_age_sex_df = DBReader.get_table(cur, soc_adm_age_sex_q).sort_values(by=['age'])
                
                else: 
                    soc_adm_age_sex_q = f'SELECT * FROM age_sex_social_administrative_units where administrative_unit_id in {tuple(adm_total_df["id"].unique())}'
                    soc_adm_age_sex_df = DBReader.get_table(cur, soc_adm_age_sex_q).sort_values(by=['age'])
            
            # print(adm_total_df, mun_total_df, adm_age_sex_df, mun_age_sex_df, soc_adm_age_sex_df)

            return adm_total_df, mun_total_df, adm_age_sex_df, mun_age_sex_df, soc_adm_age_sex_df, houses_df
