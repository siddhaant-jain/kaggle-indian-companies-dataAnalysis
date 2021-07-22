import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession \
        .builder \
        .appName("Indian Comapnies") \
        .getOrCreate()


# winutilsPath = 'E:/studies/winutils-master/hadoop-2.7.1'

# datasetPath = 'C:/Users/SkJain/Documents/BigDataWorkspace2/registered_companies.csv'
datasetPath = '/Users/ravigaurav/My Data/Personal Projects/Datasets/registered_companies.csv'

# read the dataset
companies_data_raw = spark.read.option("header", True).csv(datasetPath)
# companies_data_raw.show(truncate=False)

# list of columns in the dataset
colms_list = companies_data_raw.columns
for coln in colms_list:
        print(coln)
        
# list of columns with the datatypes
for col_types in companies_data_raw.dtypes:
        print(col_types)


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

spark.stop()