# 5

import pandas as pd
from scripts import save_db
from tqdm import tqdm
from scripts.save_csv import Saver
import random


class Sequence:
    val = 0

    def get_range_ends(self, length: int) -> tuple:
        '''Returns ends as [left, right)'''
        old_val = self.val
        self.val += length
        
        return old_val, self.val


def generate_rounds(df) -> None:
    
    df['men_rounded'] = 0
    df['women_rounded'] = 0

    for soc in df['social_group_id'].unique():
        df_ = df.loc[df['social_group_id']==soc].copy()
        missing_val = 0
        # print('\n', soc, '\n')

        for house in tqdm(df['house_id'].unique()):
            # print(house)
            df__ = df_.loc[df_['house_id']==house].copy()
                
        
            for sex in ['men', 'women']:
                # print(sex)
                base_num = df__[df__[f'{sex}'] > 0][f'{sex}'].min()
                
                try:
                    df__[f'ratio_{sex}'] = df__[f'{sex}'].apply(lambda x: x / base_num)
                    df__ = df__.sort_values(by=f'ratio_{sex}')
                    
                    s = Sequence()

                    df__[f'seq_{sex}'] = df__[f'ratio_{sex}'].apply(s.get_range_ends)
                    right_borader = df__[f'seq_{sex}'].iat[-1][1]


                    # total_soc = round(df__[f'{sex}'].sum())

                    data_was = df__[f'{sex}'].values
                    data_now = []

                    for _, data in enumerate(data_was, 1):
                        data_now.append(round(data + missing_val))
                        missing_val = round(data + missing_val - data_now[-1], 2)

                    total_soc = sum(data_now)
                    # print('missing: ', missing_val)
                    # print('total sum: ', total_soc)
                    # print('total_was: ', sum(data_was))


                    # initial_total_soc = df__[f'{sex}'].sum()
                    # print(initial_total_soc)
                    # total_soc = round(initial_total_soc + missing_val)
                    # print(total_soc)

                    # missing_val += round((total_soc - initial_total_soc), 2)
                    # print(missing_val)

                    lst = list()
                    for i in range(total_soc):
                        lst.append(random.uniform(0, right_borader-1))            
                    
                    for dice in lst:           
                        idx = df__.loc[df__[f'seq_{sex}'].apply(lambda rng: dice >= rng[0] and dice < rng[1])].iloc[0].name
                        df.loc[idx, f'{sex}_rounded'] += 1
                        
                
                except ValueError:
                    # print(soc, sex)
                    df.loc[df['social_group_id']==soc, f'{sex}_rounded'] = 0

        # idxs = (df['social_group_id']==soc) & (df['house_id']==house)
        # df.loc[idxs] = df.loc[idxs].join(pd.Series(, name=''))


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

        # Кол-во людей в соц.группе в возрасте по полу = кол-во людей в доме * вероятность быть
        # в возрасте в мун в соц группе
        df['men'] = df['men'] * df['mun_percent']
        df['women'] = df['women'] * df['mun_percent']

        df = df.drop(['mun_percent', 'municipality_id'], axis=1)

        df['men'] = df['men'].astype(float).round(2)
        df['women'] = df['women'].astype(float).round(2)

        # generate_rounds(df)

        df.insert(0, 'year', args.year)
        df.insert(1, 'set_population', args.population)
        df.insert(2, 'scenario', args.scenario)
        
        if args.save == 'db':
            save_db.main(args.db_addr, args.db_port, args.db_name, args.db_user, args.db_pass, df)
        
        elif args.save == 'loc':  
            Saver.df_to_csv(df=df, id=mun, folder_name=f'{args.year}.{args.scenario}')
    
    if args.save == 'loc':
        Saver.cat(folder_name=f'{args.year}_{args.scenario}')


def main(houses_soc, mun_soc, args):
    print('В процессе: распределение жителей домов (по соц. группам) по возрастам')

    mun_soc = mun_soc[['municipality_id', 'social_group_id', 'age', 'men', 'women']]

    houses_soc_to_ages(args, houses_soc, mun_soc)

    print('Выполнено: распределение жителей домиков (по соц. группам) по возрастам\n')


if __name__ == '__main__':
    df = pd.read_csv('../2019.mod/data.csv')
    df = df.query('social_group_id==40')
    generate_rounds(df)
    df.to_csv('../2019.mod/data_2.csv')
