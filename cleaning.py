from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col, regexp_replace
from pyspark.sql.types import IntegerType

# Initialize a Spark session
spark = SparkSession.builder.appName("DataTransformation")\
    .config('spark.driver.extraClassPath','/usr/lib/jvm/java-11-openjdk-amd64/lib/postgresql-42.5.0.jar')\
        .getOrCreate()

listing_df_raw = spark.read.csv('raw_data/listings.tsv', header=True, inferSchema=True, sep="\t")
reviews_df_raw = spark.read.csv('raw_data/reviews.tsv', header=True, inferSchema=True, sep="\t")
calendar_df_raw = spark.read.csv('raw_data/calendar.tsv', header=True, inferSchema=True,sep="\t")

# Transform the data

# LISTINGS DATA

# Drop the 'summary' column
listing_df_raw = listing_df_raw.drop('summary')
listing_df_raw = listing_df_raw.drop('description')
listing_df_raw = listing_df_raw.drop('host_about')


# Convert 'host_is_superhost' to boolean
listing_df_raw = listing_df_raw.withColumn('host_is_superhost', when(col('host_is_superhost') == 't', True).otherwise(False))

# Drop 'country' and 'market' columns
listing_df_raw = listing_df_raw.drop('country', 'market')

# Drop rows with null values in the 'space' column
listing_df_raw = listing_df_raw.na.drop(subset=['space'])

# Drop rows with null values in the 'property_type' column
listing_df_raw = listing_df_raw.na.drop(subset=['property_type'])

# Remove "$" and convert to integer
listing_df_raw = listing_df_raw.withColumn("price", regexp_replace(col("price"), "[^0-9]", "").cast(IntegerType()))

# Replace commas in all columns using a loop
for column in listing_df_raw.columns:
    listing_df_raw = listing_df_raw.withColumn(column, regexp_replace(col(column), ',', ''))

# CALENDAR DATA

# Convert 'available' to boolean
calendar_df_raw = calendar_df_raw.withColumn('available', when(col('available') == 't', True).otherwise(False))

# Remove "$" and convert to integer
calendar_df_raw = calendar_df_raw.withColumn("price", regexp_replace(col("price"), "[^0-9]", "").cast(IntegerType()))

#Save the data
listing_df_raw.coalesce(4).write.parquet('cleaned_data/clean_listing_parquet', mode="overwrite", compression="snappy")
calendar_df_raw.coalesce(4).write.parquet('cleaned_data/clean_calendar_parquet', mode="overwrite", compression="snappy")
reviews_df_raw.coalesce(4).write.parquet('cleaned_data/clean_reviews_parquet', mode="overwrite", compression="snappy")

listing_df_raw.write.format('jdbc').options(url='jdbc:postgresql://localhost:5432/Final',driver = 'org.postgresql.Driver', dbtable = 'listing', user='postgres',password='kushal2psg').mode('overwrite').save()
calendar_df_raw.write.format('jdbc').options(url='jdbc:postgresql://localhost:5432/Final',driver = 'org.postgresql.Driver', dbtable = 'calendar', user='postgres',password='kushal2psg').mode('overwrite').save()
reviews_df_raw.write.format('jdbc').options(url='jdbc:postgresql://localhost:5432/Final',driver = 'org.postgresql.Driver', dbtable = 'reviews', user='postgres',password='kushal2psg').mode('overwrite').save()

spark.stop()