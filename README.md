# Football Statistics Tracker

An automated pipeline to collect, store, and analyze football match statistics from the top 5 European leagues using API-FOOTBALL and Google Cloud Platform services.

## Table of Contents

- [Introduction](#introduction)
- [Features](#features)
- [Architecture](#architecture)
- [Installation](#installation)
- [Usage](#usage)
- [Project Structure](#project-structure)
- [Contributing](#contributing)
- [License](#license)

## Introduction

This project fetches daily match fixtures and statistics from football-data.org, stores the data in Google BigQuery, and provides tools for data analysis and visualization.

## Features

- Automated data fetching using Google Cloud Functions.
- Data storage and querying with Google BigQuery.
- Scalable and cost-effective cloud-based solution.
- Modular codebase for easy maintenance and extension.

## Architecture

![Architecture Diagram](docs/architecture_diagram.png)

## Installation

[Provide installation instructions as previously detailed.]

## Usage

- **Deploy the Cloud Function**: Run `scripts/deploy_function.sh`.
- **Set up BigQuery**: Run `scripts/setup_bigquery.sh`.
- **View Data**: Access your BigQuery console to view and query the data.

## Project Structure

[Include the project structure as previously detailed.]



## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
