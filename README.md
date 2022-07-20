# Requirements for input data:  

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

The files from scripts/input_data are used for the population change forecast there is information about the population in the city (so far only St. Petersburg) by age.

All the population data for 2019 year.  

# The data processing algorithm:

**1. Forecast of population change by age until 2030 based on data from previous years.**
- **1.1.** Data is incomplete and with gaps, so data gaps and omissions are also restored.
- **1.2.** For a given forecast year (>2019), the population data is taken from the forecast for that year.
- **1.3.** When the total number is given, the forecast data is adjusted to the given total number.
- **1.4.** Population data by age are adjusted according to the projected number of population.

**2. Calculation of data on social groups by age for municipalities.**
- **2.1.** Based on the % of people in the municipality relative to the administrative district and the % of people in the social group in the                        administrative district we get an estimate of the number of social groups in the municipality.

**3. Calculation of the population by house.**
- **3.1.** Calculation of the maximum and probable number of people in houses relative to the population in the city (by county and municipality).
- **3.1.1.** Used parameters:  
             - Minimum number of square meters per person: 9;  
             - Balance accuracy: 1.
- **3.2.** Calculation of social groups by house (total by age).
- **3.3.** Calculation of social groups by house by age.

###### Parameters:  

`--year` - the year for which you want to forecast the population change. Default: 2022.  
`--city-id` - The id of the city for which you want to make the calculation. Default: 1 (St. Petersburg).  
`--set-population` - Set the total value of the population in the city (e.g. 10,000,000). By default it is not set.  
