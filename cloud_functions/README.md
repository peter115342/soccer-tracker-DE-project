# ETL Cloud Functions

This directory contains the cloud functions used for the ETL (Extract, Transform, Load) processes in the soccer-tracker-DE-project.

## Table of Contents

- [Architecture](#architecture)
- [Functions](#functions)
  - [Data Validation](#data-validation)
  - [League Data](#league-data)
  - [Match Data](#match-data)
  - [Reddit Data](#reddit-data)
  - [Serving Layer](#serving-layer)
  - [Standings Data](#standings-data)
  - [Summary Generation](#summary-generation)
  - [Weather Data](#weather-data)

## Architecture
![image](https://github.com/user-attachments/assets/c56574f2-de16-43c9-ae37-bab141897d6a)

## Functions

### Data Validation

This module contains functions for validating the incoming data to ensure it meets the required schema and quality standards.

Path: `cloud_functions/data_validation`

### League Data

This module contains functions for handling league data, including extraction, transformation, and loading into the data warehouse.

Path: `cloud_functions/league_data`

### Match Data

This module contains functions for handling match data, including extraction, transformation, and loading into the data warehouse.

Path: `cloud_functions/match_data`

### Reddit Data

This module contains functions for extracting and processing data from Reddit related to soccer.

Path: `cloud_functions/reddit_data`

### Serving Layer

This module contains functions for serving the processed data for various applications and services.

Path: `cloud_functions/serving_layer`

### Standings Data

This module contains functions for handling standings data, including extraction, transformation, and loading into the data warehouse.

Path: `cloud_functions/standings_data`

### Summary Generation

This module contains functions for generating summaries and reports from the processed data.

Path: `cloud_functions/summary_generation`

### Weather Data

This module contains functions for handling weather data, which may be relevant to soccer matches.

Path: `cloud_functions/weather_data`

