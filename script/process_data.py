# 2

import iteround
import pandas as pd
import warnings
import changes_coef
import get_data
warnings.filterwarnings("ignore")


# Посчитать % коэф. жителей в возрасте и МУН
# и сохранить локально
def calc_percent(adm_age_sex_df, adm_list, mun_age_sex_df, mun_list, path) -> None:
    print('В процессе: расчет кол-ва жителей по возрастам')

    for age in range(0, 101):
        for sex in ['men', 'women', 'total']:

            # Расчет для АДМ

            # По возрасту среди всех адм
            adm_age_sex_slice = adm_age_sex_df[adm_age_sex_df['age'] == age][sex]
            adm_age_sex_sum = adm_age_sex_df[adm_age_sex_df['age'] == age][sex].sum()

            try:
                adm_age_sex_df[f'{sex}_age_adm_percent'][adm_age_sex_df['age'] == age] = \
                    adm_age_sex_slice / adm_age_sex_sum
            except KeyError as e:
                adm_age_sex_df[f'{sex}_age_adm_percent'] = adm_age_sex_slice / adm_age_sex_sum

            # По адм среди всех возрастов
            for adm_id in adm_list:
                adm_age_sex_mun_id_slice = adm_age_sex_df.query(f"administrative_unit_id == {adm_id}")[sex]
                adm_age_sex_mun_id_sum = adm_age_sex_df.query(f"administrative_unit_id == {adm_id}")[sex].sum()

                try:
                    adm_age_sex_df[f'{sex}_age_adm_percent'][(adm_age_sex_df['administrative_unit_id'] == adm_id)] = \
                        adm_age_sex_mun_id_slice / adm_age_sex_mun_id_sum
                except KeyError as e:
                    adm_age_sex_df[f'{sex}_age_adm_percent'] = adm_age_sex_mun_id_slice / adm_age_sex_mun_id_sum

            # Расчет для МО

            # По возрасту среди всех мун
            mun_age_sex_slice = mun_age_sex_df.loc[mun_age_sex_df['age'] == age][sex]
            mun_age_sex_sum = mun_age_sex_df.loc[mun_age_sex_df['age'] == age][sex].sum()

            try:
                mun_age_sex_df[f'{sex}_age_allmun_percent'][mun_age_sex_df['age'] == age] = \
                    mun_age_sex_slice / mun_age_sex_sum
            except KeyError as e:
                mun_age_sex_df[f'{sex}_age_allmun_percent'] = mun_age_sex_slice / mun_age_sex_sum

            # По мун среди всех возрастов
            for mun_id in mun_list:
                mun_sex_mun_id_slice = mun_age_sex_df.query(f"municipality_id == {mun_id}")[sex]
                mun_sex_mun_id_sum = mun_age_sex_df.query(f"municipality_id == {mun_id}")[sex].sum()

                try:
                    mun_age_sex_df[f'{sex}_mun_allages_percent'][(mun_age_sex_df['municipality_id'] == mun_id)] = \
                        mun_sex_mun_id_slice / mun_sex_mun_id_sum
                except KeyError as e:
                    mun_age_sex_df[f'{sex}_mun_allages_percent'] = mun_sex_mun_id_slice / mun_sex_mun_id_sum
                    # print(f'Exception: {e}')

    # path = '/home/gk/code/tmppycharm/ifmo_1/script/data/'
    mun_age_sex_df.to_csv(f'{path}mun_age_sex_df.csv', index=False, header=True)
    adm_age_sex_df.to_csv(f'{path}adm_age_sex_df.csv', index=False, header=True)


# Посчитать население по соц.группам по возрасту для МУН
# и сохранить локально
def calc_mun_soc_age(mun_age_sex_df, soc_adm_age_sex_df, path) -> None:
    print('В процессе: расчет соц.групп по возрастам')

    mun_soc = pd.merge(mun_age_sex_df[['admin_unit_parent_id', 'municipality_id', 'age', 'men_age_allmun_percent',
                                       'women_age_allmun_percent', 'total_age_allmun_percent']],
                       soc_adm_age_sex_df[['admin_unit_parent_id', 'social_group_id', 'age', 'men', 'women', 'total']],
                       left_on=['admin_unit_parent_id', 'age'],
                       right_on=['admin_unit_parent_id', 'age']).sort_values(by=['age'])

    for sex in ['men', 'women', 'total']:
        mun_sex_soc_slice = mun_soc[sex]
        mun_sex_soc_percent_slice = mun_soc[f'{sex}_age_allmun_percent']
        mun_soc_sex = (mun_sex_soc_slice * mun_sex_soc_percent_slice).tolist()
        mun_soc_sex = [0.0 if pd.isna(x) else x for x in mun_soc_sex]
        mun_soc[sex] = iteround.saferound(mun_soc_sex, 0)

    # print('\nmun_soc\n',mun_soc)

    # path = '/home/gk/code/tmppycharm/ifmo_1/script/data/'
    mun_soc.to_csv(f'{path}mun_soc.csv', index=False, header=True)



# Посчитать суммарное кол-во по людей по соц.группам по АДМ
def calc_adm_soc_sum(soc_list, adm_list, soc_adm_age_sex_df, year):

    adm_soc_sum = pd.DataFrame(columns=['year', 'admin_unit_parent_id', 'social_group_id',
                                        'total_sum', 'men_ratio', 'women_ratio'])
    for soc in soc_list:
        for adm in adm_list:

            men_sum = soc_adm_age_sex_df.query(f'social_group_id == {soc} & admin_unit_parent_id == {adm}')['men'].sum()
            women_sum = soc_adm_age_sex_df.query(f'social_group_id == {soc} & admin_unit_parent_id == {adm}')['women'].sum()
            total_sum = soc_adm_age_sex_df.query(f'social_group_id == {soc} & admin_unit_parent_id == {adm}')['total'].sum()

            men_ratio = men_sum / total_sum
            women_ratio = women_sum / total_sum

            df_to_insert = pd.DataFrame({'year': [year], 'admin_unit_parent_id': [adm], 'social_group_id': [soc],
                                         'total_sum': [total_sum], 'men_ratio': [men_ratio],
                                         'women_ratio': [women_ratio]})

            adm_soc_sum = adm_soc_sum.append(df_to_insert, ignore_index=True)

    # print('\nadm_soc_sum\n',adm_soc_sum)
    return adm_soc_sum


# Посчитать суммарное кол-во жителей по МУН и % жителей МУН в АДМ
def calc_mun_sum(mun_list, mun_age_sex_df, adm_list, year):
    print('В процессе: расчет % жителей МУН в АДМ')

    # Посчитать общее число во всех соц.группах для МУН
    mun_allages_sum = pd.DataFrame(columns=['year', 'admin_unit_parent_id', 'municipality_id',
                                           'men_sum', 'women_sum', 'total_sum'])
    for mun in mun_list:
        men_sum = mun_age_sex_df.query(f'municipality_id == {mun}')['men'].sum()
        adm = mun_age_sex_df.query(f'municipality_id == {mun}')['admin_unit_parent_id'].values[0]
        women_sum = mun_age_sex_df.query(f'municipality_id == {mun}')['women'].sum()
        total_sum = mun_age_sex_df.query(f'municipality_id == {mun}')['total'].sum()

        df_to_insert = pd.DataFrame({'year': [year], 'admin_unit_parent_id': [adm], 'municipality_id': [mun],
                                     'men_sum': [men_sum], 'women_sum': [women_sum], 'total_sum': [total_sum]})

        mun_allages_sum = mun_allages_sum.append(df_to_insert, ignore_index=True)

    # print('\nmun_allages_sum:\n',mun_allages_sum)

    # Посчитать общее число во всех соц.группах для АДМ
    adm_allages_sum = pd.DataFrame(columns=['year', 'admin_unit_parent_id', 'men_adm_sum',
                                            'women_adm_sum', 'total_adm_sum'])
    for adm in adm_list:
        men_adm_sum = mun_allages_sum.query(f'admin_unit_parent_id == {adm}')['men_sum'].sum()
        women_adm_sum = mun_allages_sum.query(f'admin_unit_parent_id == {adm}')['women_sum'].sum()
        total_adm_sum = mun_allages_sum.query(f'admin_unit_parent_id == {adm}')['total_sum'].sum()

        df_to_insert = pd.DataFrame({'year': [year], 'admin_unit_parent_id': [adm],
                                     'men_adm_sum': [men_adm_sum], 'women_adm_sum': [women_adm_sum],
                                     'total_adm_sum': [total_adm_sum]})

        adm_allages_sum = adm_allages_sum.append(df_to_insert, ignore_index=True)

    # print('\nadm_allages_sum:\n', adm_allages_sum)

    # Найти процент суммы по соц.группам в МУН от общего числа в АДМ
    mun_allages_percent = pd.DataFrame(columns=['year', 'admin_unit_parent_id', 'municipality_id',
                                                'mun_in_adm_total_percent', 'men_mun_ratio', 'women_mun_ratio'])
    for mun in mun_list:
        adm = mun_allages_sum.query(f'municipality_id == {mun}')['admin_unit_parent_id'].values[0]

        men_ratio = mun_allages_sum.query(f'municipality_id == {mun}')['men_sum'].values[0] / \
                    mun_allages_sum.query(f'municipality_id == {mun}')['total_sum'].values[0]

        women_ratio = mun_allages_sum.query(f'municipality_id == {mun}')['women_sum'].values[0] / \
                      mun_allages_sum.query(f'municipality_id == {mun}')['total_sum'].values[0]

        total_percent = mun_allages_sum.query(f'municipality_id == {mun}')['total_sum'].values[0] / \
                        adm_allages_sum.query(f'admin_unit_parent_id == {adm}')['total_adm_sum'].values[0]

        df_to_insert = pd.DataFrame({'year': [year], 'admin_unit_parent_id': [adm], 'municipality_id': [mun],
                                     'mun_in_adm_total_percent': [total_percent],
                                     'men_mun_ratio': [men_ratio], 'women_mun_ratio': [women_ratio]
                                     })

        mun_allages_percent = mun_allages_percent.append(df_to_insert, ignore_index=True)

    # print('\nmun_allages_percent\n', mun_allages_percent)

    return mun_allages_percent


# Посчитать суммарное кол-во жителей в МУН по соц.группам
# и сохранить локально
def calc_mun_soc_sum(adm_list, soc_list, mun_allages_percent, adm_soc_sum, year, path) -> None:

    print('В процессе: расчет соц.групп по МУН')

    mun_soc_allages_sum = pd.DataFrame(columns=['year', 'admin_unit_parent_id', 'municipality_id', 'social_group_id',
                                                'total_mun_soc_sum', 'men_mun_soc_sum', 'women_mun_soc_sum'])
    for adm in adm_list:
        mun_list = mun_allages_percent.query(f'admin_unit_parent_id == {adm}')['municipality_id'].values

        for soc in soc_list:
            total_adm_soc_sum = adm_soc_sum.query(
                f'admin_unit_parent_id == {adm} & social_group_id == {soc}')['total_sum'].values[0]
            men_adm_soc_ratio = adm_soc_sum.query(
                f'admin_unit_parent_id == {adm} & social_group_id == {soc}')['men_ratio'].values[0]
            women_adm_soc_ratio = adm_soc_sum.query(
                f'admin_unit_parent_id == {adm} & social_group_id == {soc}')['women_ratio'].values[0]

            for mun in mun_list:
                mun_in_adm_total_percent = mun_allages_percent.query(
                    f'municipality_id == {mun}')['mun_in_adm_total_percent'].values[0]
                # men_mun_ratio = mun_allages_percent.query(f'municipality_id == {mun}')['men_mun_ratio'].values[0]
                # women_mun_ratio = mun_allages_percent.query(f'municipality_id == {mun}')['men_mun_ratio'].values[0]

                total_mun_soc_sum = total_adm_soc_sum * mun_in_adm_total_percent
                men_mun_soc_sum = (total_adm_soc_sum * men_adm_soc_ratio) * mun_in_adm_total_percent
                women_mun_soc_sum = (total_adm_soc_sum * women_adm_soc_ratio) * mun_in_adm_total_percent

                df_to_insert = pd.DataFrame({'year': [year], 'admin_unit_parent_id': [adm], 'municipality_id': [mun],
                                             'social_group_id': [soc], 'total_mun_soc_sum': [total_mun_soc_sum],
                                             'men_mun_soc_sum': [men_mun_soc_sum],
                                             'women_mun_soc_sum': [women_mun_soc_sum]})

                mun_soc_allages_sum = mun_soc_allages_sum.append(df_to_insert,
                                                                 ignore_index=True).sort_values(by='social_group_id')

    # Сбалансированное округление по соц.группам
    total_list_tmp = []
    men_list_tmp = []
    women_list_tmp = []

    for soc in soc_list:
        df_slice = mun_soc_allages_sum.query(f'social_group_id == {soc}')

        total = iteround.saferound(df_slice['total_mun_soc_sum'], 0)
        men = iteround.saferound(df_slice['men_mun_soc_sum'], 0)
        women = iteround.saferound(df_slice['women_mun_soc_sum'], 0)

        total_list_tmp += total
        men_list_tmp += men
        women_list_tmp += women

    mun_soc_allages_sum['total_mun_soc_sum'] = total_list_tmp
    mun_soc_allages_sum['men_mun_soc_sum'] = men_list_tmp
    mun_soc_allages_sum['women_mun_soc_sum'] = women_list_tmp

    mun_soc_allages_sum = mun_soc_allages_sum.astype(int)

    # path = '/home/gk/code/tmppycharm/ifmo_1/script/data/'
    mun_soc_allages_sum.to_csv(f'{path}mun_soc_allages_sum.csv', index=False, header=True)


def main(args, year=2023, city_id=1, path='', set_population=0):
    adm_total_df, mun_total_df, adm_age_sex_df, mun_age_sex_df, soc_adm_age_sex_df, _ = get_data.main(args)

    pd.set_option('display.max_rows', 10)
    pd.set_option('display.max_columns', 20)

    if year > 2019:

        coef_changes, year_ratio, change_coef = changes_coef.main(year, path)

        def update_total_population(df):
            new_population = df['population'] * change_coef
            new_population = iteround.saferound(new_population, 0)
            df['population'] = new_population

            return df

        # Пересчитать адм по полу и возрастам
        def update_age_sex_population(df, year):
            df['total'] = df['men'] + df['women']

            new_total_age_list = list()
            for age in range(0, 101):
                age_slice = df.query(f'age == {age}')
                total_age_value = age_slice['total'] * coef_changes[age]
                new_total_age_list += list(total_age_value)

            df['new_total'] = new_total_age_list
            df['new_total'] = iteround.saferound(df['new_total'], 0)

            new_men = list(df['men'] / df['total'] * df['new_total'])
            new_men = [0.0 if pd.isna(x) else x for x in new_men]
            df['men'] = new_men
            df['men'] = iteround.saferound(df['men'], 0)

            new_women = list(df['women'] / df['total'] * df['new_total'])
            new_women = [0.0 if pd.isna(x) else x for x in new_women]
            df['women'] = new_women
            df['women'] = iteround.saferound(df['women'], 0)

            df['year'] = year
            df.drop('total', axis=1, inplace=True)
            df = df.rename(columns={'new_total': 'total'})

            if set_population:
                df['total_percent'] = df['total'] / df['total'].sum()
                df['women_percent'] = df['women'] / df['total'].sum()
                df['men_percent'] = df['men'] / df['total'].sum()

                df['total'] = iteround.saferound(list(df['total_percent'] * set_population), 0)
                df['women'] = iteround.saferound(list(df['women_percent'] * set_population), 0)
                df['men'] = iteround.saferound(list(df['men_percent'] * set_population), 0)
                df.drop(['total_percent', 'women_percent', 'men_percent'], axis=1, inplace=True)

            return df

        # Пересчитать численность по АДМ (суммарно)
        adm_total_df = update_total_population(adm_total_df)

        # Пересчитать по мун (суммарно)
        mun_total_df = update_total_population(mun_total_df)

        adm_age_sex_df = update_age_sex_population(adm_age_sex_df, year)
        mun_age_sex_df = update_age_sex_population(mun_age_sex_df, year)
        soc_adm_age_sex_df = update_age_sex_population(soc_adm_age_sex_df, year)

        print(f'Выполнено: пересчет населения на {year} год')

    adm_age_sex_df['total'] = adm_age_sex_df['men'] + adm_age_sex_df['women']
    mun_age_sex_df['total'] = mun_age_sex_df['men'] + mun_age_sex_df['women']
    soc_adm_age_sex_df['total'] = soc_adm_age_sex_df['men'] + soc_adm_age_sex_df['women']

    adm_age_sex_df['men_percent'] = adm_age_sex_df['men'] / adm_age_sex_df['total']
    mun_age_sex_df['men_percent'] = mun_age_sex_df['men'] / mun_age_sex_df['total']
    soc_adm_age_sex_df['men_percent'] = soc_adm_age_sex_df['men'] / soc_adm_age_sex_df['total']

    adm_age_sex_df['women_percent'] = adm_age_sex_df['women'] / adm_age_sex_df['total']
    mun_age_sex_df['women_percent'] = mun_age_sex_df['women'] / mun_age_sex_df['total']
    soc_adm_age_sex_df['women_percent'] = soc_adm_age_sex_df['women'] / soc_adm_age_sex_df['total']

    adm_total_df['population_percent'] = adm_total_df['population'] / \
                                         adm_total_df['population'].sum()
    mun_total_df['population_percent'] = mun_total_df['population'] / \
                                         mun_total_df['population'].sum()
    mun_total_df = mun_total_df.rename(columns={"id": "municipality_id"})

    soc_adm_age_sex_df = soc_adm_age_sex_df.rename(columns={"administrative_unit_id": "admin_unit_parent_id"})

    mun_list = set(mun_age_sex_df['municipality_id'])
    adm_list = set(adm_age_sex_df['administrative_unit_id'])
    soc_list = set(soc_adm_age_sex_df['social_group_id'])

    calc_percent(adm_age_sex_df, adm_list, mun_age_sex_df, mun_list, path)

    # Прочитать CSV и добавить колонку с АДМ_id
    # path = '/home/gk/code/tmppycharm/ifmo_1/script/data/'
    # adm_age_sex_df = pd.read_csv(f'{path}adm_age_sex_df.csv')
    mun_age_sex_df = pd.read_csv(f'{path}mun_age_sex_df.csv')
    mun_age_sex_df = pd.merge(mun_age_sex_df, mun_total_df[['municipality_id', 'admin_unit_parent_id']],
                              on='municipality_id')

    # Изменить порядок столбцов
    col = mun_age_sex_df.pop("admin_unit_parent_id")
    mun_age_sex_df.insert(1, col.name, col)

    calc_mun_soc_age(mun_age_sex_df, soc_adm_age_sex_df, path)
    adm_soc_sum = calc_adm_soc_sum(soc_list, adm_list, soc_adm_age_sex_df, year)
    mun_allages_percent = calc_mun_sum(mun_list, mun_age_sex_df, adm_list, year)
    calc_mun_soc_sum(adm_list, soc_list, mun_allages_percent, adm_soc_sum, year, path)

    print('Выполнено: посчитана статистика по населению')


if __name__ == '__main__':
    pass
