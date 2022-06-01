# 5

import iteround
import pandas as pd
from scripts import save_db
from tqdm import tqdm

# Распределить жителей домов (по соц. группам) по возрастам (0-100)
# и сохранить локально
from scripts.save_csv import Saver


def houses_soc_to_ages(args, houses_soc, mun_soc):

    mun_list = set(houses_soc['municipality_id'])
    soc_list = set(houses_soc['social_group_id'])

    print('Расчет жителей домов по возрастам среди соц.групп:')
    for mun in tqdm(mun_list):
        # Разрез по муниципалитетам - чтобы кусочками работать с df и не есть много памяти за раз
        houses_soc_mun = houses_soc.loc[houses_soc['municipality_id'] == mun]
        mun_soc_mun = mun_soc.loc[mun_soc['municipality_id'] == mun]

        df = pd.merge(houses_soc_mun, mun_soc_mun, on=['municipality_id', 'social_group_id'])
        df = df.sort_values(by=['house_id', 'social_group_id'])

        df['men_rounded'] = ''
        df['women_rounded'] = ''

        # Кол-во людей в соц.группе в возрасте по полу = кол-во людей в доме * вероятность быть
        # в возрасте в мун в соц группе
        df['men'] = df['men'] * df['mun_percent']
        df['women'] = df['women'] * df['mun_percent']

        if args.round:
            # Округление со сходящейся суммой по возрастам для соц.групп в доме
            # Разбиение по домикам - чтобы балансировать людей по домикам
            houses_id = set(df['house_id'])

            for house in houses_id:
                for soc in soc_list:

                    men_lst = df.query(f'social_group_id == {soc} & house_id == {house}')['soc_men'].values
                    women_lst = df.query(f'social_group_id == {soc} & house_id == {house}')['soc_women'].values

                    men_rnd = iteround.saferound(men_lst, 0)
                    women_rnd = iteround.saferound(women_lst, 0)

                    df.loc[(df['house_id'] == house) & (df['social_group_id'] == soc), 'soc_men_rounded'] = men_rnd
                    df.loc[(df['house_id'] == house) & (df['social_group_id'] == soc), 'soc_women_rounded'] = women_rnd
        else:
            df.men_rounded = 0
            df.women_rounded = 0

        df = df.drop(['mun_percent', 'municipality_id'], axis=1)

        df['men'] = df['men'].astype(float).round(2)
        df['women'] = df['women'].astype(float).round(2)

        df.insert(0, 'year', args.year)
        # df.insert(1, 'set_population', args.population)
        df.insert(2, 'scenario', args.scenario)
        
        if args.save == 'db':
            save_db.main(args=args, houses_df=df)
        
        elif args.save == 'loc':  
            Saver.df_to_csv(df=df, id=mun)
    
    if args.save == 'loc':
        Saver.cat()


def main(houses_soc, mun_soc, args, path=''):
    print('В процессе: распределение жителей домов (по соц. группам) по возрастам')

    mun_soc = mun_soc[['municipality_id', 'social_group_id', 'age', 'men', 'women']]

    houses_soc_to_ages(args, houses_soc, mun_soc)

    print('Выполнено: распределение жителей домиков (по соц. группам) по возрастам\n')


if __name__ == '__main__':
    pass
