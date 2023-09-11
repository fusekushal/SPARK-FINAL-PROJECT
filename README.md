# Spark-Final-Project
# Airbnb Boston Data Analysis

This project aims to analyze the Boston Airbnb dataset using PySpark. The dataset contains three main dataframes: listings_df, calendar_df, and reviews_df. Each dataframe provides valuable information about Airbnb listings, their availability, and guest reviews in Boston.

## Prerequisites

Before running the PySpark code, ensure that you have the following prerequisites installed:

- Apache Spark
- PySpark
- Jupyter Notebook (optional)
- postgresql
- jdbc driver

You can install PySpark and set up your environment using the official [PySpark documentation](https://spark.apache.org/docs/latest/api/python/getting_started/index.html).


## Data Description

The dataset consists of the following dataframes:

1. listings_df: Contains information about Airbnb listings in Boston.
2. calendar_df: Provides details about the availability and pricing of listings over time.
3. reviews_df: Contains guest reviews for listings.
## Questions and Tasks

### Task 1 

*Question 1:* To create a User-Defined Function (UDF) that converts extracted months from integers to their corresponding month names.

### Task 2 

*Question 2:* Busiest Booking Month: Using the date column in the calendar_df, determine which month in Boston had the highest number of bookings. We can use aggregation functions like groupBy and count.

### Task 3 

*Question 3:* Calculate the number of days since the last booking for each listing in Boston. Use a Window Function to find listings that have been vacant for the longest period.

## Running the Code

To run the PySpark code and explore the analysis, you can use the provided Jupyter Notebook files in the notebooks/ directory. Make sure to adjust the file paths according to your local setup.

## Data Source

The Boston Airbnb dataset used in this project is sourced from [Boston Airbnb](https://www.kaggle.com/datasets/airbnb/boston?select=calendar.csv).

## Acknowledgments

- Airbnb for providing the dataset.
- Apache Spark and PySpark community for powerful data analysis tools.
