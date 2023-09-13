# Spark-Final-Project
# Airbnb Boston Data Analysis

This project aims to analyze the Boston Airbnb dataset using PySpark. The dataset contains three main dataframes: listings_df, calendar_df, and reviews_df. Each dataframe provides valuable information about Airbnb listings, their availability, and guest reviews in Boston.

## Prerequisites

Before running the PySpark code, ensure that you have the following prerequisites installed:

- Apache Spark
- PySpark
- Jupyter Notebook (optional)
- postgresql
- vscode

You can install PySpark and set up your environment using the official [PySpark documentation](https://spark.apache.org/docs/latest/api/python/getting_started/index.html).


## Data Description

The dataset consists of the following dataframes:

1. listings_df: Contains information about Airbnb listings in Boston.
2. calendar_df: Provides details about the availability and pricing of listings over time.
3. reviews_df: Contains guest reviews for listings.
## Questions and Tasks

### Task 1 - Customer Oriented Problem

*Question 1:* Get Distinct Street names and it's best listing based on analysis of price, reviews and booking. Give the budget oriented results and luxury oriented results

### Task 2 - Lister Oriented Problem

*Question 2:* Analyze the impact of host response time on guest satisfaction. Correlate faster response time with higher review scores. Determine if host with high turnover consistently provide quicker response.

## Running the Code

To run the PySpark code and explore the analysis, you can use the provided Jupyter Notebook files in the notebooks/ directory. Make sure to adjust the file paths according to your local setup.

## Data Source

The Boston Airbnb dataset used in this project is sourced from [Boston Airbnb](https://www.kaggle.com/datasets/airbnb/boston?select=calendar.csv).

## Acknowledgments

- Airbnb for providing the dataset.
- Apache Spark and PySpark community for powerful data analysis tools.
