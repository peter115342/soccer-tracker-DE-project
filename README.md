<p align="center">
  <img src="https://github.com/user-attachments/assets/ce31cf4c-a438-4f31-b4c1-24620b96c5f9" width="225" alt="Football Statistics Tracker Logo">
</p>

<div style="background-color: #FFEDD5; padding: 10px; border-radius: 5px; margin-bottom: 20px;">
  <strong>⚠️ LEARNING PROJECT:</strong> This is a personal learning project in the field of data engineering. I understand the architecture might not be the most optimal as this project. I made this to practise and learn. Feedback and suggestions are highly welcomed!
</div>


# Football Statistics Tracker 📊⚽

![Python](https://img.shields.io/badge/Python-3.12-green.svg)
![GCP](https://img.shields.io/badge/GCP-Powered-blue.svg)
![Terraform](https://img.shields.io/badge/IaC-Terraform-purple.svg)
![CI/CD](https://img.shields.io/badge/CI%2FCD-passing-success.svg)
![Svelte](https://img.shields.io/badge/Svelte-orange?logo=svelte)
![BigQuery](https://img.shields.io/badge/BigQuery-blue?logo=google-cloud)
![Firestore](https://img.shields.io/badge/Firestore-yellow?logo=firebase)
![Repo Size](https://img.shields.io/github/repo-size/peter115342/soccer-tracker-DE-project)


An end-to-end data engineering pipeline that collects, processes, and analyzes football match results, standings data, weather data, Reddit data and summarizes matchdays using Gemini from the top 5 European leagues. Used data sources include [football-data.org](https://www.football-data.org/) API, [Open-Meteo](https://open-meteo.com/) API, and [PRAW](https://praw.readthedocs.io/en/stable/) (Reddit API), [Maps](https://developers.google.com/maps)...

## Introduction

This project demonstrates a complete data pipeline for football (soccer) results, from data extraction to visualization. It implements some data engineering practices including data lakes, transformation layers, and Infrastructure as Code (IaC) with Terraform.

## Features

- **Automated Data Collection**: Scheduled data fetching from multiple APIs using Google Cloud Functions
- **Multi-layer Data Architecture**: Raw data stored in GCS, processed data in BigQuery, and user-facing data in Firestore
- **Weather Integration**: Match statistics with weather data at match time
- **Social Media (Reddit) Data**: Reddit comments for fan sentiment
- **Infrastructure as Code**: Cloud Functions and Pub/Sub subscriptions and topics defined and deployed with Terraform

## Architecture

![Architecture Diagram](https://github.com/user-attachments/assets/f60432f9-40fb-4467-83d2-969d3826b372)

The pipeline follows the following architecture:

1. **Data Ingestion**: Cloud Functions trigger on schedule to fetch data
2. **Storage Layers**: Raw data(json) → External BQ tables (Parquet) → Processed Data in BQ → Firestore  
4. **Validation**: Very simple validation and Data qaulity with Dataplex
5. **Summarization**: Creation of short summaries in Markdown with Gemini 2.0 Flash
6. **Visualization**: [Web app](https://dataeurotop5football.win/) for insights 

## Data Sources

- **Football-data.org**: Match data, team data, and standings
- **Open-Meteo API**: Historical weather data
- **Reddit (via PRAW)**: Fan comments and sentiment
- **Maps SDK**: Location of stadiums

## Technology Stack

| Category | Technologies |
|----------|--------------|
| Cloud Platform | Google Cloud Platform (GCP) |
| Infrastructure as Code | Terraform |
| Programming Languages | Python, TypeScript (Svelte) |
| Data Storage | Cloud Storage, BigQuery, Firestore |
| Data Quality | Dataplex |
| Data Transformation | Dataform |
| Serverless Computing | Cloud Functions |
| Event-Driven Architecture | Pub/Sub |
| API Consumption | Football-data.org, Open-Meteo, Reddit API, Google Maps |
| CI/CD | GitHub Actions |
| Package Management | uv, pyproject.toml |
| Code Quality | Ruff, Bandit, Mypy |
| Testins | pytest |
| Web Framework | Svelte, ShadCN UI Components |
| Hosting | Firebase App Hosting |
| LLM | Google Gemini 2.0 Flash |


## Project Structure

```
soccer-tracker-DE-project/
├── README.md
├── .gitignore
├── pyproject.toml
├── Github/workflows/                  # CI/CD in Github Actions
│   ├── cd.yml
│   └── ci.yml
├── terraform/                         # IaC definitions
│   ├── main.tf
│   ├── variables.tf
│   ├── pubsub.tf
│   └── cloud_functions.tf
├── cloud_functions/
│   ├── league_data/                   # League and Teams data extraction and load
│   ├── discord_utils/                 # Package for sending Discord notifications using webhooks
│   ├── match_data/                    # Match data extraction and load
│   ├── weather_data/                  # Weather data extraction and load
│   ├── reddit_data/                   # Reddit data extraction and load
│   ├── standings_data/                # Standings data extraction and load for each matchday
│   ├── data_validation/               # Data validation using Dataplex
│   ├── serving_layer/                 # Load data to firestore
│   └── generate_summaries/            # Generate match summaries with Gemini
├── soccer_tracker_ui/                 # Svelte web app in Firebase
│   ├── src/
│   │   ├── lib/                       # Reusable components
│   │   │   ├── components/            # UI components from [shadcn](https://next.shadcn-svelte.com/)
│   │   │   ├── firebase.ts            # Firebase/Firestore connection
│   │   │   └── stores/                # Svelte stores for state management
│   │   ├── routes/                    # Page components
│   ├── package.json                   # Dependencies and scripts
│   ├── svelte.config.js               # Svelte configuration
│   ├── vite.config.js                 # Vite bundler config
└── tests/                             # Test suite for Cloud Functions with Pytest
```

## Additional Documentation

- [Tests README](https://github.com/peter115342/soccer-tracker-DE-project/blob/main/tests/README.md)
- [Cloud Functions README](https://github.com/peter115342/soccer-tracker-DE-project/blob/main/cloud_functions/README.md)
- [Terraform README](https://github.com/peter115342/soccer-tracker-DE-project/blob/main/terraform/README.md)
- [GH Actions README](https://github.com/peter115342/soccer-tracker-DE-project/blob/main/.github/workflows/README.md)


## [Web app](https://dataeurotop5football.win/)

The project includes a Svelte web app for visualizing match results, weather data, and match summaries. 

App includes:

- Match Results
- Match summaries using an LLM (Gemini 2.0 Flash)
- Weather data during matches
- Comments from Reddit

![image](https://github.com/user-attachments/assets/92d2b39d-1e6d-4f4b-bd23-0d0adde4f9dc)

![image](https://github.com/user-attachments/assets/d66691a7-adb1-41a8-9fdd-c65e56c36c00)

<div style="background-color: #FFEDD5; padding: 10px; border-radius: 5px; margin-bottom: 20px;">
  <strong>⚠️ DISCLAIMER:</strong> I know this data probably does not have much real value as it is not real-time and the statistics are not that deep ( I wanted to stay within free tiers of APIs).
</div>

-----------------------------------
I got the idea to make this project from this [repo](https://github.com/digitalghost-dev/premier-league) by [digitalghost-dev](https://github.com/digitalghost-dev)
