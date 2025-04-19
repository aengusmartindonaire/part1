#!/usr/bin/python
import sys
from pyspark.sql import SparkSession

def extract_hour(violation_time):
    """Extracts the hour from the violation time string.""" 
    if violation_time is None or not violation_time.strip():
        return None
    hour_part = violation_time[:2]
    try:
        hour = int(hour_part)
    except ValueError:
        return None
    if violation_time[-1].upper() == 'P' and hour < 12:
        hour += 12
    elif violation_time[-1].upper() == 'A' and hour == 12:
        hour = 0
    return hour

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: spark.py <file_path> <parallelism_parameter>")
        sys.exit(1)

    file_path = sys.argv[1]
    parallelism_parameter = sys.argv[2]

    spark = SparkSession.builder \
        .appName("MostFrequentTicketHour") \
        .config("spark.default.parallelism", parallelism_parameter) \
        .getOrCreate()

    # Set log level to WARN to reduce output verbosity
    spark.sparkContext.setLogLevel("WARN")

    # Read CSV data into RDD
    rdd = spark.sparkContext.textFile(file_path)

    # Gets the first row (header), so we can exclude it later.
    header = rdd.first()  

    # Removes the header and split rows by commas
    rows = rdd.filter(lambda line: line != header).map(lambda line: line.split(","))

    # Extract hour from violation time (20th column)
    hours = rows.map(lambda cols: extract_hour(cols[19])).filter(lambda x: x is not None)

    # Counts how often each hour appears.
    hour_counts = hours.countByValue()

    # Finds the hour with the highest count on dictionary
    most_ticket_hour = max(hour_counts, key=hour_counts.get)

    if most_ticket_hour:
        print(f"The most frequent hour for tickets is: {most_ticket_hour} with {hour_counts[most_ticket_hour]} tickets.")
    else:
        print("No data available or unable to parse data correctly.")

    # Stops the spark session
    spark.stop()
