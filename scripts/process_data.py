# 2

import iteround
import pandas as pd
from scripts import changes_coef
from scripts import read_data


# Посчитать % коэф. жителей в возрасте и МУН
def calc_percent(adm_age_sex_df, adm_list, mun_age_sex_df, mun_list, path):
    print('В процессе: расчет кол-ва жителей по возрастам')

    for age in range(0, 101):
        for sex in ['men', 'women', 'total']:

            # Расчет для АДМ

            # По возрасту среди всех адм
            adm_age_sex_slice = adm_age_sex_df[adm_age_sex_df['age'] == age][sex]
            adm_age_sex_sum = adm_age_sex_df[adm_age_sex_df['age'] == age][sex].sum()

            try:
                adm_age_sex_df.loc[adm_age_sex_df['age'] == age, f'{sex}_age_adm_percent'] = \
                    adm_age_sex_slice / adm_age_sex_sum
            except KeyError as e:
                adm_age_sex_df[f'{sex}_age_adm_percent'] = adm_age_sex_slice / adm_age_sex_sum

            # По адм среди всех возрастов
            for adm_id in adm_list:
                adm_age_sex_mun_id_slice = adm_age_sex_df.query(f"administrative_unit_id == {adm_id}")[sex]
                adm_age_sex_mun_id_sum = adm_age_sex_df.query(f"administrative_unit_id == {adm_id}")[sex].sum()

                try:
                    adm_age_sex_df.loc[adm_age_sex_df['administrative_unit_id', f'{sex}_age_adm_percent'] == adm_id] = \
                        adm_age_sex_mun_id_slice / adm_age_sex_mun_id_sum
                except KeyError as e:
                    adm_age_sex_df[f'{sex}_age_adm_percent'] = adm_age_sex_mun_id_slice / adm_age_sex_mun_id_sum

            # Расчет для МО

            # # По возрасту среди всех мун

            for adm in adm_list:
                mun_age_sex_slice = mun_age_sex_df.loc[(mun_age_sex_df['age'] == age) & (mun_age_sex_df['admin_unit_parent_id'] == adm)][sex]
                mun_age_sex_sum = mun_age_sex_df.loc[(mun_age_sex_df['age'] == age) & (mun_age_sex_df['admin_unit_parent_id'] == adm)][sex].sum()

                try:
                    mun_age_sex_df.loc[(mun_age_sex_df['age'] == age) & (mun_age_sex_df['admin_unit_parent_id'] == adm), f'{sex}_age_allmun_percent'] = \
                        mun_age_sex_slice / mun_age_sex_sum
                except KeyError as e:
                    mun_age_sex_df[f'{sex}_age_allmun_percent'] = mun_age_sex_slice / mun_age_sex_sum

            # mun_age_sex_slice = mun_age_sex_df.loc[mun_age_sex_df['age'] == age][sex]
            # mun_age_sex_sum = mun_age_sex_df.loc[mun_age_sex_df['age'] == age][sex].sum()
            #
            # try:
            #     mun_age_sex_df.loc[mun_age_sex_df['age'] == age, f'{sex}_age_allmun_percent'] = \
            #         mun_age_sex_slice / mun_age_sex_sum
            # except KeyError as e:
            #     mun_age_sex_df[f'{sex}_age_allmun_percent'] = mun_age_sex_slice / mun_age_sex_sum

            # По мун среди всех возрастов
            for mun_id in mun_list:
                mun_sex_mun_id_slice = mun_age_sex_df.query(f"municipality_id == {mun_id}")[sex]
                mun_sex_mun_id_sum = mun_age_sex_df.query(f"municipality_id == {mun_id}")[sex].sum()

                try:
                    mun_age_sex_df.loc[mun_age_sex_df['municipality_id'] == mun_id, f'{sex}_mun_allages_percent'] = \
                        mun_sex_mun_id_slice / mun_sex_mun_id_sum
                except KeyError as e:
                    mun_age_sex_df[f'{sex}_mun_allages_percent'] = mun_sex_mun_id_slice / mun_sex_mun_id_sum
                    # print(f'Exception: {e}')

    return mun_age_sex_df, adm_age_sex_df


# Посчитать население по соц.группам по возрасту для МУН
# и сохранить локально
def calc_mun_soc_age(mun_age_sex_df, soc_adm_age_sex_df, path):
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

    # print('Выполнено: расчет кол-ва жителей по возрастам')

    return mun_soc


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

            adm_soc_sum = pd.concat([adm_soc_sum, df_to_insert], ignore_index=True)

    return adm_soc_sum


# Посчитать суммарное кол-во жителей по МУН и % жителей МУН в АДМ
def calc_mun_sum(mun_list, mun_age_sex_df, adm_list, year):
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

        mun_allages_sum = pd.concat([mun_allages_sum, df_to_insert], ignore_index=True)

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

        adm_allages_sum = pd.concat([adm_allages_sum, df_to_insert], ignore_index=True)

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

        mun_allages_percent = pd.concat([mun_allages_percent, df_to_insert], ignore_index=True)

    return mun_allages_percent


# Посчитать суммарное кол-во жителей в МУН по соц.группам
def calc_mun_soc_sum(adm_list, soc_list, mun_allages_percent, adm_soc_sum, year):
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

                total_mun_soc_sum = total_adm_soc_sum * mun_in_adm_total_percent
                men_mun_soc_sum = (total_adm_soc_sum * men_adm_soc_ratio) * mun_in_adm_total_percent
                women_mun_soc_sum = (total_adm_soc_sum * women_adm_soc_ratio) * mun_in_adm_total_percent

                df_to_insert = pd.DataFrame({'year': [year], 'admin_unit_parent_id': [adm], 'municipality_id': [mun],
                                             'social_group_id': [soc], 'total_mun_soc_sum': [total_mun_soc_sum],
                                             'men_mun_soc_sum': [men_mun_soc_sum],
                                             'women_mun_soc_sum': [women_mun_soc_sum]})

                mun_soc_allages_sum = pd.concat([mun_soc_allages_sum, df_to_insert],
                                                                 ignore_index=True).sort_values(by='social_group_id')

    # Сбалансированное округление по соц.группам
    for soc in soc_list:
        df_slice = mun_soc_allages_sum.query(f'social_group_id == {soc}')

        total = iteround.saferound(df_slice['total_mun_soc_sum'], 0)
        men = iteround.saferound(df_slice['men_mun_soc_sum'], 0)
        women = iteround.saferound(df_slice['women_mun_soc_sum'], 0)

        mun_soc_allages_sum.loc[mun_soc_allages_sum['social_group_id'] == soc, 'total_mun_soc_sum'] = total
        mun_soc_allages_sum.loc[mun_soc_allages_sum['social_group_id'] == soc, 'men_mun_soc_sum'] = men
        mun_soc_allages_sum.loc[mun_soc_allages_sum['social_group_id'] == soc, 'women_mun_soc_sum'] = women

    mun_soc_allages_sum = mun_soc_allages_sum.astype(int)

    return mun_soc_allages_sum


def main(args, changes_forecast_df, city_forecast_years_age_ratio_df, city_population_forecast_df,
         year=2023, path='', set_population=0):
    adm_total_df, mun_total_df, adm_age_sex_df, mun_age_sex_df, soc_adm_age_sex_df, _ = read_data.main(args)

    pd.set_option('display.max_rows', 10)
    pd.set_option('display.max_columns', 20)

    if year > 2019:
        print(f'В процессе: пересчет населения на {year} год')
        coef_changes, year_ratio, change_coef = changes_coef.main(changes_forecast_df, city_forecast_years_age_ratio_df,
                                                                  city_population_forecast_df, year, path)

        def update_total_population(df):
            new_population = df['population'] * change_coef
            new_population = iteround.saferound(new_population, 0)
            df['population'] = new_population

            return df

        # Пересчитать адм по полу и возрастам
        def update_population_year(df, year):
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

            return df

        def set_population_num(df):

            if 'social_group_id' in df.columns:
                df.sort_values(by=['age', 'administrative_unit_id', 'social_group_id'])
                adm_age_sex_df.sort_values(by=['age', 'administrative_unit_id'])

                soc_list = set(df.social_group_id)

                for soc in soc_list:
                    df.loc[df['social_group_id'] == soc, 'men'] = df.query(f'social_group_id == {soc}')['men'] \
                                                                  / adm_age_sex_df['total'].sum() * set_population

                    df.loc[df['social_group_id'] == soc, 'women'] = df.query(f'social_group_id == {soc}')['women'] \
                                                                    / adm_age_sex_df['total'].sum() * set_population

                    df.loc[df['social_group_id'] == soc, 'total'] = df.query(f'social_group_id == {soc}')['men'] + df.query(f'social_group_id == {soc}')['women']

                    df.loc[df['social_group_id'] == soc, 'total'] = iteround.saferound(list(df.query(f'social_group_id == {soc}')['total']), 0)
                    df.loc[df['social_group_id'] == soc, 'women'] = iteround.saferound(list(df.query(f'social_group_id == {soc}')['women']), 0)
                    df.loc[df['social_group_id'] == soc, 'men'] = iteround.saferound(list(df.query(f'social_group_id == {soc}')['men']), 0)

            else:
                df['women'] = df['women'] / df['total'].sum() * set_population
                df['men'] = df['men'] / df['total'].sum() * set_population

                df['women'] = iteround.saferound(list(df['women']), 0)
                df['men'] = iteround.saferound(list(df['men']), 0)

                df['total'] = df['women'] + df['men']

            return df

        # Пересчитать численность (суммарно)
        adm_total_df = update_total_population(adm_total_df)
        mun_total_df = update_total_population(mun_total_df)

        adm_age_sex_df = update_population_year(adm_age_sex_df, year)
        mun_age_sex_df = update_population_year(mun_age_sex_df, year)
        soc_adm_age_sex_df = update_population_year(soc_adm_age_sex_df, year)

        if set_population:
            soc_adm_age_sex_df = set_population_num(soc_adm_age_sex_df)
            adm_age_sex_df = set_population_num(adm_age_sex_df)
            mun_age_sex_df = set_population_num(mun_age_sex_df)
    #
    # adm_age_sex_df['total'] = adm_age_sex_df['men'] + adm_age_sex_df['women']
    # mun_age_sex_df['total'] = mun_age_sex_df['men'] + mun_age_sex_df['women']
    # soc_adm_age_sex_df['total'] = soc_adm_age_sex_df['men'] + soc_adm_age_sex_df['women']
    #
    # adm_age_sex_df['men_percent'] = adm_age_sex_df['men'] / adm_age_sex_df['total']
    # mun_age_sex_df['men_percent'] = mun_age_sex_df['men'] / mun_age_sex_df['total']
    # soc_adm_age_sex_df['men_percent'] = soc_adm_age_sex_df['men'] / soc_adm_age_sex_df['total']
    #
    # adm_age_sex_df['women_percent'] = adm_age_sex_df['women'] / adm_age_sex_df['total']
    # mun_age_sex_df['women_percent'] = mun_age_sex_df['women'] / mun_age_sex_df['total']
    # soc_adm_age_sex_df['women_percent'] = soc_adm_age_sex_df['women'] / soc_adm_age_sex_df['total']
    #
    # adm_total_df['population_percent'] = adm_total_df['population'] / adm_total_df['population'].sum()
    # mun_total_df['population_percent'] = mun_total_df['population'] / mun_total_df['population'].sum()

    mun_total_df.rename(columns={"id": "municipality_id"}, inplace=True)

    soc_adm_age_sex_df.rename(columns={"administrative_unit_id": "admin_unit_parent_id"}, inplace=True)

    mun_list = set(mun_age_sex_df['municipality_id'])
    adm_list = set(adm_age_sex_df['administrative_unit_id'])
    soc_list = set(soc_adm_age_sex_df['social_group_id'])

    mun_age_sex_df = pd.merge(mun_age_sex_df, mun_total_df[['municipality_id', 'admin_unit_parent_id']],
                              on='municipality_id')

    # Изменить порядок столбцов
    col = mun_age_sex_df.pop("admin_unit_parent_id")
    mun_age_sex_df.insert(1, col.name, col)

    mun_age_sex_df, adm_age_sex_df = calc_percent(adm_age_sex_df, adm_list, mun_age_sex_df, mun_list, path)

    print('В процессе: расчет соц.групп по возрастам')
    mun_soc = calc_mun_soc_age(mun_age_sex_df, soc_adm_age_sex_df, path)

    print('В процессе: расчет соц.групп суммарно по АДМ')
    adm_soc_sum = calc_adm_soc_sum(soc_list, adm_list, soc_adm_age_sex_df, year)

    print('В процессе: расчет % жителей МУН в АДМ')
    mun_allages_percent = calc_mun_sum(mun_list, mun_age_sex_df, adm_list, year)

    print('В процессе: расчет соц.групп по МУН')
    mun_soc_allages_sum = calc_mun_soc_sum(adm_list, soc_list, mun_allages_percent, adm_soc_sum, year)

    return mun_soc, mun_age_sex_df, adm_age_sex_df, mun_soc_allages_sum


if __name__ == '__main__':
    pass
