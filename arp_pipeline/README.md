# Data Pipeline for Emergency Rental Assistance

This module provides a [Luigi](https://luigi.readthedocs.io/en/stable/)-based pipeline to load geospatial and census data for determining households that, based on factual-proxy information, qualify for  [Emergency Rental Assistance](https://home.treasury.gov/policy-issues/coronavirus/assistance-for-state-local-and-tribal-governments/emergency-rental-assistance-program). [Per the department of treasury's FAQs](https://home.treasury.gov/policy-issues/coronavirus/assistance-for-state-local-and-tribal-governments/emergency-rental-assistance-program/faqs#4):

> Fact-specific proxy – A grantee may rely on a written attestation from the applicant as to household income if the grantee also uses any reasonable fact-specific proxy for household income, such as reliance on data regarding average incomes in the household’s geographic area. 

## Data Sources

1. [Census's TIGER shape files](https://www.census.gov/geographies/mapping-files/time-series/geo/tiger-line-file.html)
2. [Department of Transportation's National Address Database](https://www.transportation.gov/gis/national-address-database/national-address-database-0)
3. [Housing and Urban Development's Income Limits](https://www.huduser.gov/portal/datasets/il.html)
4. [Census's American Community Survey Tables](https://www.census.gov/programs-surveys/acs)

## Overview of the system

Currently, the system downloads data from the above sources and loads them into a postgres database (with postgis extensions). The goal is to then take this source data and turn it into per-state artifacts.
