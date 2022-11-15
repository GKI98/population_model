import os
import time

scenarios = ['mod','pos','neg']
cities = [2,5,6, 10]

for city in cities:
    for year in range(2022, 2031, 1):
        for scenario in scenarios:
            print(f'\n year: {year}, city_id: {city}, scenario: {scenario} \n')
            os.system(f'python3 cli.py --year={year} --scenario={scenario} --city-id={city} -save=db')
            time.sleep(5)
