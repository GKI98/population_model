from scripts import get_data
import pandas as pd


'''
В houses нет муниципалитета №101 !!!
'''


# Посчитать макс. и вероятное кол-во жителей в домике
def forecast_house_population(args):
    houses_df = get_data.main(args)[5]
    max_sq_liv = 9
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



# -------------------------------------------------------------------------------------------------
    # Этап 3 - Балансировка численности населения в домах для всех МО

    # Датафрейм для хранения результатов балансировки населения по МО
df_mkd_balanced_mo = pd.DataFrame()
for index, row in df_mo.iterrows():
    # Определить количество жителей в МО по данным предыдущей балансировки
    population_mo_reg_bal = row['population_balanced']
    print(f'Количество жителей в МО {row["mo_name"]} после балансировки внутри района: {population_mo_reg_bal}')
    # Выбрать дома, относящиеся к выбранному МО
    df_mkd_mo = df_mkd[df_mkd['mo_id'] == index].copy()
    if df_mkd_mo.shape[0] == 0:
        print(f'МО "{row["mo_name"]} не содержит домов, пропускается')
        continue
    # Определить вероятную численность населения в МО как сумму population_probably домов МО
    population_mo_vch = df_mkd_mo['population_probably'].sum()
    print(f'Вероятное количество жителей в МО "{row["mo_name"]}": {population_mo_vch}')
    # Сделать вероятные количества жителей в домах отправной точкой для расчета сбалансированных значений
    df_mkd_mo['population_balanced'] = df_mkd_mo['population_probably'].copy()
    print(f'Начало балансировки для МО "{row["mo_name"]}"')
    # df_mkd_mo_print = df_mkd_mo[['mo_name', 'population_max', 'population_probably']].copy()
    # print(df_mkd_mo_print)
    # print()
    # Шаг балансировки
    i = 0
    # Если количество жителей в МО после балансировки по району БОЛЬШЕ, чем расчитанное вероятное количество жителей для этого МО,
    # то разница должна быть распределена между неаварийными домами МО
    if population_mo_reg_bal > df_mkd_mo['population_balanced'].sum():
        df_mkd_mo_not_f = df_mkd_mo[~df_mkd_mo['failing']].copy()
        df_mkd_mo_not_f['rule'] = df_mkd_mo_not_f['population_max'] - df_mkd_mo_not_f['population_balanced']
        while population_mo_reg_bal > df_mkd_mo['population_balanced'].sum():
            # Находим индекс неаварийного дома с максимальной разницей между ОМЧ и ВЧ
            try:
                house_id = df_mkd_mo_not_f['rule'].idxmax()
            except ValueError as ex:
                print(repr(ex))
                break
            # Прибавляем жителей к "сбалансированной численности" этого дома
            df_mkd_mo.loc[house_id, 'population_balanced'] = df_mkd_mo.loc[house_id, 'population_balanced'] + accuracy
            df_mkd_mo_not_f.loc[house_id, 'rule'] = sqrt(df_mkd_mo.loc[house_id, 'population_max']) - sqrt(df_mkd_mo.loc[house_id, 'population_balanced'])
            # Ищем новое значение сбалансированной численности для МО
            i += 1
    # Если количество жителей в МО после балансировки по району МЕНЬШЕ, чем расчитанное вероятное количество жителей для этого МО,
    # то разница должна быть вычтена из количества жителей домов, причем аварийные дома также участвуют в балансировке
    elif population_mo_reg_bal < df_mkd_mo['population_balanced'].sum():
        df_mkd_mo_not_f = df_mkd_mo[df_mkd_mo['population_balanced'] > 0].copy()
        df_mkd_mo_not_f['rule'] = df_mkd_mo_not_f['population_max'].apply(sqrt) - df_mkd_mo_not_f['population_balanced'].apply(sqrt)
        try:
            while population_mo_reg_bal < df_mkd_mo['population_balanced'].sum():
                # Находим индекс неаварийного дома с минимальной разницей между ОМЧ и ВЧ
                try:
                    house_id = df_mkd_mo_not_f[df_mkd_mo_not_f['population_balanced'] > accuracy]['rule'].idxmin()
                except ValueError:
                    break
                # Вычитаем жителей из "сбалансированной численности" этого дома
                df_mkd_mo.loc[house_id, 'population_balanced'] -= accuracy
                df_mkd_mo_not_f.loc[house_id, 'population_balanced'] -= accuracy
                df_mkd_mo_not_f.loc[house_id, 'rule'] = sqrt(df_mkd_mo_not_f.loc[house_id, 'population_max']) - sqrt(df_mkd_mo_not_f.loc[house_id, 'population_balanced'])
                # Ищем новое значение сбалансированной численности для МО
                i += 1
        except Exception:
            print(population_mo_reg_bal, df_mkd_mo['population_balanced'].sum())
            print(df_mkd_mo_not_f.loc[house_id])
            print(df_mkd_mo.loc[house_id])
            print(df_mkd_mo)
            raise
    print(f'Конец балансировки для {row["mo_name"]}, {i} шагов\n')
    # print('Население по домам:')
    # df_mkd_mo_print = df_mkd_mo[['mo_name', 'population_max', 'population_probably', 'population_balanced']].copy()
    # print(df_mkd_mo_print)
    # print()
    # Сохранить результаты балансировки по МО в итоговую таблицу
    df_mkd_balanced_mo = df_mkd_balanced_mo.append(df_mkd_mo)
# Сохранить результаты балансировки в исходный массив данных по МКД
df_mkd = df_mkd_balanced_mo.copy()
print('Проверка:')
print(f'Население по всем МО после балансировки по районам: {df_mo["population_balanced"].sum()}')
print(f'Сбалансированное население по всем МО: {df_mkd["population_balanced"].sum()}')