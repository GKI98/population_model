import os

scenarios = ['pos','mod','neg']

for year in range(2029, 2031, 1):
    for scenario in scenarios:
        print(f'\n year: {year}, scenario: {scenario} \n')
        os.system(f'python3 cli.py --year={year} --scenario={scenario}')
