# Data Pipeline for Emergency Rental Assistance

This module provides a [Luigi](https://luigi.readthedocs.io/en/stable/)-based pipeline to load geospatial and census data for determining households that, based on factual-proxy information, qualify for  [Emergency Rental Assistance](https://home.treasury.gov/policy-issues/coronavirus/assistance-for-state-local-and-tribal-governments/emergency-rental-assistance-program). [Per the U.S. Department of the Treasury's FAQs](https://home.treasury.gov/policy-issues/coronavirus/assistance-for-state-local-and-tribal-governments/emergency-rental-assistance-program/faqs#4):

> Fact-specific proxy – A grantee may rely on a written attestation from the applicant as to household income if the grantee also uses any reasonable fact-specific proxy for household income, such as reliance on data regarding average incomes in the household’s geographic area. 

This pipeline implements the example proxy from the U.S. Department of the Treasury's [Guidelines for fact-specific proxies](https://home.treasury.gov/policy-issues/coronavirus/assistance-for-state-local-and-tribal-governments/emergency-rental-assistance-program/service-design/fact-specific-proxies).

## Data Sources

1. [Census's TIGER shape files](https://www.census.gov/geographies/mapping-files/time-series/geo/tiger-line-file.html)
2. [Department of Transportation's National Address Database (NAD)](https://www.transportation.gov/gis/national-address-database/national-address-database-0)
3. [Housing and Urban Development's (HUD) Income Limits](https://www.huduser.gov/portal/datasets/il.html)
4. [Housing and Urgan Development's Fair Market Rent Geodata](https://hudgis-hud.opendata.arcgis.com/datasets/HUD::fair-market-rents/about)
5. [Census's American Community Survey Tables](https://www.census.gov/programs-surveys/acs)

Currently, we use TIGER shapefiles for 2019, the NAD version 7, HUD income-limits for 2021, and ACS data from the 2019 5-year estimates. 


## Building and using this code

The root directory of this repo contains a `docker-compose.yml` that should get you everything you need:

1. A postgres database with the postgis extensions (including the tiger geocoder)
2. A python 3.9 container with [poetry](https://python-poetry.org) installed, along with the GIS tools the pipeline uses (e.g., gdal and postgis's shp file converters). Poetry manages the rest of dependencies, like [luigi](https://github.com/spotify/luigi) for orchestrating pulling all the geodata and [plumbum](https://plumbum.readthedocs.io/en/latest/) for running CLI utils on random GIS data.

Note that you're going to want a generous amount of free-space on your disk (more than 500gb) if you want to use data from the whole country.

## Generating a state's outputs

Assuming the state is in the NAD, you can run the following command to generate proxy data for every address in the state:

`docker-compose run arp_pipeline /app/create_outputs STATE_USPS_CODE`

`STATE_USPS_CODE` will be a two-letter code like MD or OH or WA. This will kick-off a mountain of ETL-tasks, which you can monitor using luigi's dashboard at `http://localhost:8082/static/visualiser/index.html` thanks to the magic of docker.

If the state is not in the NAD, you'll have some work to do --- while in concept you can generate every address for a state using the TIGER data, that's not implemented yet, so consider it an execise to for the reader.

Also note that it is state-centric; if you need to generate data for a smaller jurisdiction like a county, you'll want to either generate the state and then filter the output probably. This too is an execrise for the reader, but hopefully a simple one.

## Overview of the system

While you'll need to work through the dependencies in the luigi steps to know exactly what's happening with the steps, almost any output generation will follow the same overall steps. Some of these steps are reused by later states, so the overview below assumes a first run. Also note that luigi can run some of these steps in parallel, but because writing is linear, the discussion below is linear:

1. Download, unzip, and load the national-level TIGER shapefiles (`tiger_national.LoadStateData`). These define the various GEOIDs and polygons/geometries of states, which is then used to download county data.
2. Download, unzip, and load the county-level TIGER shapefiles (`tiger_national.LoadCountyData`). These define geoides for all the counties in the US, along with their polygons. These are used later for downloading more specific geomtric and feature data.
3. Download, unzip, and load the National Address Database (`nad.LoadNADData`), which gives us maining address data and geolocations for every address in the file.
4. Download, unzip, and load HUD's fair-market geodata (`hud.LoadHUDFMRGeos`), which give the polygons for the fair market rent areas that are referenced by the HUD income limits.
5. Download, unzip, and load HUD's income-limits data (`hud.LoadHUDData`), which tells you what 80% of AMI is in a given Fair Market area.
6. Download, unzip, and load TIGER Shapefiles for State/County Subdivisions and Tracts, along with feature-names, address-data, and edge data that powers the built-in tiger geocoders (basically everything in `tiger_state`, but you can follow the path back from `tiger_state.LoadStateFeatures`). This is how you get the polygons for census tracts. Some of the other data are probably not strictly necessary, but I expect to need them when generating address data for states that are not in the NAD.
7. Download, clean, and load the census data (`census.LoadTractLevelACSData`). The list of census variables can be parameterized via `census.LoadTractLevelACSData.acs_variables`; the simplest thing to do is add variables to the defaults.
8. Generate lookup tables used to simplify the query for the final outputs. These are models defined by `arp_pipeline` in the `models` modules. We generate the following:
  1. A table that links the income limits to the fair market rents (in postgres, this is `lookups.hud_fmr_geo_lookup`, defined in `models.hud_fmr_lookup`): `build_lookups.CreatHUDCBSALookups`
  2. A table that links addresses to specific income-limits data (in postgres, this is `lookups.address_hud_income_limit_STATE_USPS_CODE`, so there's one per state): `build_lookups.creadHUDAddressLookups`
  3. A table that links addresses to census tracts (in postgres, this is `lookups.address_tract_STATE_USPS_CODE`, so there's one per state): `build_lookups.CreateTractLookups`
9. Create the core fact table for the state. This is slightly fancy --- we use postgres interitance to build state-specific fact tables that aggregate up to an national table. The national table is `output.address_income_fact` in postgres (`models.output.base_income_fact_table`), and then each state will have a child table with a name like `output.address_income_fact_STATE_USPS_CODE`created by (`models.output.get_address_income_fact_for_state`). The table is created by `build_outputs.CreateAddressIncomeFact` and the core logic is in the query in `build_outputs.CreateAddressIncomeFact.run`.
10. Building on the table created by `CreateAddressIncomeFact`, create a postgres dump for the state (`build_outputs.CreateStateAddressIncomePGDump`) and a paquet file for the state data (`build_outputs.CreateAddressIncomeParquet`). This parquet file is the input for later outputs.
11. Create any further outputs based on the parquet generated in step 10. Right now there's just an example step to create a CSV of data for the state.


## What has not been done

1. The data need careful QA. You'll want to sample addresses, run them through some alternative geocoder (like the census one), then do your own lookups against the census tables and income limits data to verify that the output tables are correct.
2. We don't really know what he final outputs should be --- the goal so far was to build a pipeline where the final outputs could be adjusted based on grantee needs, and that's about where we are.
