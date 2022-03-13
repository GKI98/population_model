# 3

import pandas as pd
import get_data


'''
В houses нет муниципалитета №101 !!!
'''


# Посчитать макс. и вероятное кол-во жителей в домике
def forecast_house_population(args):
    houses_df = get_data.main(args)[5]
    max_sq_liv = 16
    houses_df['max_population'] = (houses_df['living_area'] / max_sq_liv).astype(int)

    def vch_calc(row):
        a_omch = 0.3  # коэффициент для ожидаемой максимальной численности жителей (ОМЧ)
        a_ich = 0.7  # коэффициент для известной численности жителей (ИЧ)

        if row['failure'] is True:
            val = row['resident_number']

        elif (row['resident_number'] == 0) and (row['failure'] is False):
            val = row['max_population']

        elif row['resident_number'] > row['max_population']:
            val = row['max_population']
        else:
            val = a_omch * row['max_population'] + a_ich * row['resident_number']
        return val

    houses_df['prob_population'] = houses_df.apply(vch_calc, axis=1).round().astype(int)

    return houses_df


# Сбалансировать вероятное кол-во жителей в домике
# и сохранить локально
def balance_houses_population(houses_df_upd, path) -> None:
    mun_age_sex_df = pd.read_csv(f'{path}/mun_age_sex_df.csv')

    mun_list = set(houses_df_upd['municipality_id'])
    # Минимальное значение, до которого может сокращаться населения в доме при балансировке, кол-во человек
    balancing_min = 5
    # Точность балансировки, кол-во человек
    accuracy = 5
    counter = 0
    df_mkd_balanced_mo = pd.DataFrame()
    sex = 'total'
    for mun in mun_list:
        citizens_mo_reg_bal = mun_age_sex_df.query(f'municipality_id == {mun}')[sex].sum()

        # Выбрать дома, относящиеся к выбранному МО
        df_mkd_mo = houses_df_upd.query(f'municipality_id == {mun}')

        # Сделать вероятные количества жителей в домах отправной точкой для расчета сбалансированных значений
        df_mkd_mo.assign(citizens_reg_bal = df_mkd_mo.prob_population)
        citizens_mo_bal = df_mkd_mo['citizens_reg_bal'].sum()

        # Шаг балансировки
        print('Посчитано:', counter)
        print('Начало балансировки для ',mun)

        # Шаг балансировки
        i = 0

        # Если количество жителей в МО после балансировки по району БОЛЬШЕ,
        # чем расчитанное вероятное количество жителей для этого МО,
        # то разница должна быть распределена между неаварийными домами МО
        if citizens_mo_reg_bal > citizens_mo_bal:
            while citizens_mo_reg_bal > citizens_mo_bal:
                df_mkd_mo_not_f = df_mkd_mo[df_mkd_mo['failure'] == 0]
                # Находим индекс неаварийного дома с максимальной разницей между ОМЧ и ВЧ
                the_house = (df_mkd_mo_not_f['max_population'] - df_mkd_mo_not_f['citizens_reg_bal']).idxmax()
                # Прибавляем жителей к "сбалансированной численности" этого дома
                df_mkd_mo.at[the_house, 'citizens_reg_bal'] = df_mkd_mo.at[the_house, 'citizens_reg_bal'] + accuracy
                # Ищем новое значение сбалансированной численности для МО
                citizens_mo_bal = df_mkd_mo['citizens_reg_bal'].sum()
                i = i + 1

        # Если количество жителей в МО после балансировки по району МЕНЬШЕ, чем расчитанное вероятное количество жителей для этого МО,
        # то разница должна быть вычтена из количества жителей домов, причем аварийные дома также участвуют в балансировке
        elif citizens_mo_reg_bal < citizens_mo_bal:
            while citizens_mo_reg_bal < citizens_mo_bal:
                df_mkd_mo_not_f = df_mkd_mo[df_mkd_mo['citizens_reg_bal'] > balancing_min]

                try:
                    the_house = (df_mkd_mo_not_f['max_population'] - df_mkd_mo_not_f['citizens_reg_bal']).idxmin()
                    # Вычитаем жителей из "сбалансированной численности" этого дома
                    df_mkd_mo.at[the_house, 'citizens_reg_bal'] = df_mkd_mo.at[
                                                                      the_house, 'citizens_reg_bal'] - accuracy
                except ValueError as e:
                    print('Численность по МУН меньше, чем минимальное распределение по домикам в этом МУН')
                    print('Необходимо уменьшить минимальную численность населения для каждого домика')
                    raise e

                # Ищем новое значение сбалансированной численности для МО
                citizens_mo_bal = df_mkd_mo['citizens_reg_bal'].sum()
                i = i + 1

        df_mkd_balanced_mo = pd.concat([df_mkd_balanced_mo, df_mkd_mo])
        counter += 1
        print('Конец балансировки для ', mun, ' \n')
        print('Выполнено шагов: ', i, '\n')

    df_mkd_balanced_mo.to_csv(f'{path}/houses_bal.csv')


def main(args, path=''):
    houses_df_upd = forecast_house_population(args)
    balance_houses_population(houses_df_upd, path)


if __name__ == '__main__':
    pass
