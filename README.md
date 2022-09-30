# Population balancing utility

## Requirements for input data:  

No matter if you use database or files input, migration coefficients and population changes will be needed as a separate csv files:

- Changes population file format includes column "year" and 101 columns named with integer numbers from 0 to 100. Every row sets total
  population number of a given age for a given year
- Migration coefficient file format includes columns named with interers - years of statistic, and one row with migration coefficients
  for a given year. It may contain one more index column in the begignning if index is string.

### Database case:

The following tables must be present in the database:

**For houses:**
- buildings
- functional_objects
- physical_objects

**For the population:**
- administrative_units
- municipalities
- age_sex_administrative_units
- age_sex_municipalities
- age_sex_social_administrative_units

### Local files case:

The local_input_path directory must include a list of files:
- outer_territories_age_sex.csv
- outer_territories_total.csv
- houses.csv
- inner_territories_age_sex.csv
- inner_territories_total.csv
- outer_territories_soc_age_sex.csv

## The data processing algorithm:

1. **Forecast of population change by age until 2030 based on data from previous years.**
    1. Data is incomplete and with gaps, so data gaps and omissions are also restored.
    2. For a given forecast year (>base_year), the population data is taken from the forecast for that year.
    3. When the total number is given, the forecast data is adjusted to the given total number.
    4. Population data by age are adjusted according to the projected number of population.

2. **Calculation of data on social groups by age for municipalities.**
    1. Based on the % of people in the municipality relative to the administrative district and the % of people in the social group in the administrative unit we get an estimate of the number of social groups in the municipality.

3. **Calculation of the population by house.**
    1. Calculation of the maximum and probable number of people in houses relative to the population in the city (by county and municipality).
        1. Used parameters:  
            - Minimum number of square meters per person: 9;  
            - Balance accuracy: 1.
    2. Calculation of social groups by house (total by age).
    3. Calculation of social groups by house by age.

## Parameters:  

Options:
-  -H, --db_addr TEXT              --- Postgres DBMS address  [env var: DB_ADDR; default: localhost]
-  -P, --db_port INTEGER           --- Postgres DBMS port number  [env var: DB_PORT; default: 5432]
-  -D, --db_name TEXT              --- Postgres database name  [env var: DB_NAME; default: city_db_final]
-  -U, --db_user TEXT              --- Postgres user  [env var: DB_USER; default: postgres]
-  -W, --db_pass TEXT              --- Postgres user password  [env var: DB_PASS; default: postgres]
-  -y, --year INTEGER              --- Year for calculations  [env var: YEAR; required]
-  -c, --city_id INTEGER           --- City_id for calculations  [env var: CITY_ID]
-  -p, --set_population INTEGER    --- Set city summary population for the year [env var: SET_POPULATION; default: (non- set)]
-  -s, --scenario [pos|mod|neg]    --- Set the scenario for calculations  [env var: SCENARIO; default: mod]
-  -i, --local_input_path PATH     --- Input files from the given folder (must contain files "adm_age_sex_df", "adm_total_df", "houses_df", "mun_age_sex_df", "mun_total_df", "soc_adm_age_sex_df") instead of the database. This option overrides database [env var: LOCAL_INPUT_PATH; default: (non-set)]
-  -o, --local_output_file PATH    --- Output balanced results to the given file in csv format (file must not exist). This options DOES NOT disable saving to the database UNTIL --local_input_path was set [env var: LOCAL_OUTPUT_FILE; default: (non-set)]
-  -ndb, --no_save_to_db           --- Disable saving to the database (--local_output_file must be set)
-  -dp, --population_changes_path PATH --- Path to population changes document  [env var: POPULATION_CHANGES_PATH; default: input_data/changes_population.csv]
-  -dm, --migration_coefficients_path PATH --- Path to migration coefficients document [env var: MIGRATION_COEFFICIENTS_PATH; default: input_data/coef_migrations.csv]
-  -b, --base_population_year INTEGER --- Base year from which calculations are performed. Must be in input files/tables [env var: BASE_POPULATION_YEAR; required]