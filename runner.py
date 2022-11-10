import os
import time

scenarios = ['mod','pos','neg']
cities = [2,5]

for city in cities:
    for year in range(2022, 2031, 1):
        for scenario in scenarios:
            print(f'\n year: {year}, scenario: {scenario} \n')
            os.system(f'python3 cli.py --year={year} --scenario={scenario} --city-id={city}')
            time.sleep(5)
