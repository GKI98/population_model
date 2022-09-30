import subprocess
import sys
from typing import Literal
import os

import click
from loguru import logger

scenarios_ok = ['pos', 'mod', 'neg']

@click.command('cli.py runner')
@click.option('--year_begin', '-b', envvar='YEAR_BEGIN', type=int, default=2022, show_default=True,
        show_envvar=True, help='Beginning year for calculations')
@click.option('--year_end', '-e', envvar='YEAR_END', type=int, default=2030, show_default=True,
        show_envvar=True, help='Last included in year for calculations')
@click.option('--scenario', '-s', 'scenarios', envvar='SCENARIOS', multiple=True, type=click.Choice(('pos', 'mod', 'neg')),
        default=['pos', 'mod', 'neg'], show_default=True, show_envvar=True, help='Add scenario for calculations')
@click.option('--base_population_year', '-y', envvar='BASE_POPULATION_YEAR', required=True, type=int, show_envvar=True,
        help='Base year from which calculations are performed. Must be in input files/tables')
@click.option('--additional_commands', '-a', envvar='ADDITONAL_COMMANDS', type=str, default='', show_default=True, show_envvar=True,
        help='Additonal commands to cli.py after setting the year and scenario')
def main(year_begin: int, year_end: int, scenarios: list[Literal['pos', 'mod', 'neg']], base_population_year: int, additional_commands: str):
    ''''Run cli.py with given range of years'''
    for year in range(year_begin, year_end + 1):
        for scenario in scenarios:
            command = f'{sys.executable} balance_social_stats.py --year={year} --scenario={scenario}' \
                    f' --base_population_year {base_population_year} --local_output_file' \
                    f' {os.path.join("output", str(year), f"{scenario}.csv")} {additional_commands}'
            logger.success(f'Running calculations for {year} year, {scenario} scenario: {command}')
            subprocess.call(command, shell=True)

if __name__ == '__main__':
    main()
