import os
import time

scenarios = ['neg','mod','pos']
cities = [1]

for city in cities:
    if city == 5:
        print(f'\n year: 2030, city_id: {city}, scenario: neg \n')
        os.system(f'python3 cli.py --year=2030 --scenario=neg --city-id={city} -save=db')
        time.sleep(5)
        continue
    else:
        for year in range(2022, 2031, 1):
            for scenario in scenarios:
                print(f'\n year: {year}, city_id: {city}, scenario: {scenario} \n')
                os.system(f'python3 cli.py --year={year} --scenario={scenario} --city-id={city} -save=db')
                time.sleep(5)
