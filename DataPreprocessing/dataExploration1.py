from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,StringType, DoubleType, DateType
from pyspark.sql.functions import udf, col
from datetime import datetime

spark = SparkSession \
        .builder \
        .appName("Indian Comapnies") \
        .getOrCreate()

spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

# winutilsPath = 'E:/studies/winutils-master/hadoop-2.7.1'

# datasetPath = 'C:/Users/SkJain/Documents/BigDataWorkspace2/registered_companies.csv'
datasetPath = '/Users/ravigaurav/My Data/Personal Projects/Datasets/registered_companies.csv'

# Schema for the dataset
schema = StructType([
        StructField("CORPORATE_IDENTIFICATION_NUMBER",StringType(),True),
        StructField("COMPANY_NAME", StringType(), True),
        StructField("COMPANY_STATUS", StringType(), True),
        StructField("COMPANY_CLASS", StringType(), True),
        StructField("COMPANY_CATEGORY", StringType(), True),
        StructField("COMPANY_SUB_CATEGORY", StringType(), True),
        StructField("DATE_OF_REGISTRATION", StringType(), True),
        StructField("REGISTERED_STATE", StringType(), True),
        StructField("AUTHORIZED_CAP", DoubleType(), True),
        StructField("PAIDUP_CAPITAL", DoubleType(), True),
        StructField("INDUSTRIAL_CLASS", DoubleType(), True),
        StructField("PRINCIPAL_BUSINESS_ACTIVITY_AS_PER_CIN", StringType(), True),
        StructField("REGISTERED_OFFICE_ADDRESS", StringType(), True),
        StructField("REGISTRAR_OF_COMPANIES", StringType(), True),
        StructField("EMAIL_ADDR", StringType(), True),
        StructField("LATEST_YEAR_ANNUAL_RETURN", StringType(), True),
        StructField("LATEST_YEAR_FINANCIAL_STATEMENT", StringType(), True)
])

# read the dataset
companies_data_raw = spark.read.option("header", True).schema(schema).csv(datasetPath)

# Removing all the companies where date of registration is null
# Total number of companies = 1992170
# Number of companies with non-null DOR = 1989645
comp_data1 = companies_data_raw.filter('DATE_OF_REGISTRATION is not NULL')

# Removing all the companies where company class is null
# Number of companies with non-null COMPANY_CLASS = 1985416
comp_data2 = comp_data1.filter('COMPANY_CLASS is not NULL')

# Removing all the companies with null REGISTERED_OFFICE_ADDRESS
# Number of companies with non-null REGISTERED_OFFICE_ADDRESS = 1971833
comp_data3 = comp_data2.filter('REGISTERED_OFFICE_ADDRESS is not NULL')

# UDF to convert date column from StringType to DateType
dateUDF =  udf (lambda x: datetime.strptime(x, '%d-%m-%Y'), DateType())

# Removing the null values from all the date columns and then
# Converting date columns to DateType from StringType
comp_data4 = comp_data3.filter(comp_data3["LATEST_YEAR_ANNUAL_RETURN"].rlike("[0-9]{2}-[0-9]{2}-[0-9]{4}")) \
        .filter(comp_data3["LATEST_YEAR_FINANCIAL_STATEMENT"].rlike("[0-9]{2}-[0-9]{2}-[0-9]{4}")) \
        .withColumn("DATE_OF_REGISTRATION_NEW", dateUDF(col("DATE_OF_REGISTRATION"))) \
        .withColumn("LATEST_YEAR_ANNUAL_RETURN_NEW", dateUDF(col("LATEST_YEAR_ANNUAL_RETURN"))) \
        .withColumn("LATEST_YEAR_FINANCIAL_STATEMENT_NEW", dateUDF(col("LATEST_YEAR_FINANCIAL_STATEMENT"))) \
        .drop("DATE_OF_REGISTRATION") \
        .drop("LATEST_YEAR_ANNUAL_RETURN") \
        .drop("LATEST_YEAR_FINANCIAL_STATEMENT")


comp_data4.show()
spark.stop()