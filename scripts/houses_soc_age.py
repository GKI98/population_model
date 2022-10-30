import pandas as pd
from scripts import save_db
from multiprocessing import Pool
from scripts.save_csv import Saver
import random
import random
from tqdm import tqdm
tqdm.pandas()


class Sequence:
    val = 0

    def get_range_ends(self, length: int) -> tuple:
        '''Returns ends as [left, right)'''
        
        old_val = self.val
        self.val += length
        
        return old_val, self.val


class MissSoc:
    missing_val = 0


def generate_rounds(df) -> None:
    df__ = df.copy()

    for sex in ['men', 'women']:
        base_num = df__[df__[f'{sex}'] > 0][f'{sex}'].min()
        
        try:
            df__[f'ratio_{sex}'] = df__[f'{sex}'].apply(lambda x: x / base_num)
            df__ = df__.sort_values(by=f'ratio_{sex}')
            
            s = Sequence()

            df__[f'seq_{sex}'] = df__[f'ratio_{sex}'].apply(s.get_range_ends)
            right_borader = df__[f'seq_{sex}'].iat[-1][1]

            data_was = df__[f'{sex}'].values
            data_now = []

            for _, data in enumerate(data_was, 1):
                data_now.append(round(data + MissSoc.missing_val))

                MissSoc.missing_val = round(data + MissSoc.missing_val - data_now[-1], 2)
                

            total_soc = sum(data_now)

            lst = list()
            for _ in range(total_soc):
                lst.append(random.uniform(0, right_borader-1))            
            
            for dice in lst:           
                idx = df__.loc[df__[f'seq_{sex}'].apply(lambda rng: dice >= rng[0] and dice < rng[1])].iloc[0].name
                df.loc[idx, f'{sex}_rounded'] += 1
                
        except ValueError as ex:
            print(ex)
            df.loc[:, f'{sex}_rounded'] = 0

    
    return df


def parallel_feature_calculation(df, processes):
    # calculate features in parallel by splitting the dataframe into partitions and using parallel processes
    
    # houses_list = df.house_id.unique()

    df__ = df.copy()
    
    df__['men_rounded'] = 0
    df__['women_rounded'] = 0

    # counter = 0

    for soc in df__['social_group_id'].unique():
        print (soc)
        df_ = df__.loc[df__['social_group_id']==soc].copy()
        # missing_val = 0

        
        # print('running\n')

        try:
            features = []
            pool = Pool(processes)

            # print('collecting features')
            for house in df_['house_id'].unique():
                features.append(pool.apply_async(generate_rounds, (df_[df_['house_id']==house],)))

            # print('doing calcs')
            for feature in features:
                house_df = feature.get()
                # print('house_df', house_df.sum())

                df_.loc[house_df.index, ['men_rounded', 'women_rounded']] = house_df

                # print('df_', df_.sum())
                
        except:
            pool.terminate()
            break

        finally:
            pool.close()
            pool.join()
            
        # print('done')

    return df_



def houses_soc_to_ages(args, houses_soc, mun_soc):
    '''
    Распределить жителей домов (по соц. группам) по возрастам (0-100)
    '''

    mun_list = set(houses_soc['municipality_id'])
    # soc_list = set(houses_soc['social_group_id'])

    print('Расчет жителей домов по возрастам среди соц.групп:')
    for mun in tqdm(mun_list):
        # Разрез по муниципалитетам - чтобы кусочками работать с df и не есть много памяти за раз
        houses_soc_mun = houses_soc.loc[houses_soc['municipality_id'] == mun]
        mun_soc_mun = mun_soc.loc[mun_soc['municipality_id'] == mun]

        df = pd.merge(houses_soc_mun, mun_soc_mun, on=['municipality_id', 'social_group_id'])
        df = df.sort_values(by=['house_id', 'social_group_id'])

        del houses_soc_mun
        del mun_soc_mun

        # print('check', mun)

        # Кол-во людей в соц.группе в возрасте по полу = кол-во людей в доме * вероятность быть
        # в возрасте в мун в соц группе
        df['men'] = df['men'] * df['mun_percent']
        df['women'] = df['women'] * df['mun_percent']

        df = df.drop(['mun_percent', 'municipality_id'], axis=1)

        df['men'] = df['men'].astype(float).round(2)
        df['women'] = df['women'].astype(float).round(2)

        df = parallel_feature_calculation(df, 4)
        # generate_rounds(df)

        df.insert(0, 'year', args.year)
        df.insert(1, 'set_population', args.population)
        df.insert(2, 'scenario', args.scenario)
        df['resident_number'].round()

        # print('saving', mun)

        df.reset_index(drop=True).to_feather(f'output_data_{args.city}_{args.year}_{args.scenario}/{mun}_data.feather')

    #     print('saving...', mun)
    #     if args.save == 'db':
    #         save_db.main(args.db_addr, args.db_port, args.db_name, args.db_user, args.db_pass, df)
        
    #     elif args.save == 'loc':  
    #         Saver.df_to_csv(df=df, id=mun, folder_name=f'{args.year}_{args.scenario}')
    
    # print('saving_2...')
    # if args.save == 'loc':
    #     Saver.cat(folder_name=f'{args.year}_{args.scenario}')


def main(houses_soc, mun_soc, args):
    print('В процессе: распределение жителей домов (по соц. группам) по возрастам')

    mun_soc = mun_soc[['municipality_id', 'social_group_id', 'age', 'men', 'women']]

    print(houses_soc, '\n', mun_soc)

    import os
    os.mkdir(f'output_data_{args.city}_{args.year}_{args.scenario}')
    houses_soc_to_ages(args, houses_soc, mun_soc)

    print('Выполнено: распределение жителей домиков (по соц. группам) по возрастам\n')
