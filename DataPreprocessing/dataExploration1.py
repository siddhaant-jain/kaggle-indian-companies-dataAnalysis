import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession \
        .builder \
        .appName("Indian Comapnies") \
        .getOrCreate()


winutilsPath = 'E:/studies/winutils-master/hadoop-2.7.1'
datasetPath = 'C:/Users/SkJain/Documents/BigDataWorkspace2/registered_companies.csv'

#read the dataset
companies_data_raw = spark.read.option("header", True).csv(datasetPath)
# companies_data_raw.show(truncate=False)

#list of columns in the dataset
colms_list = companies_data_raw.columns
for coln in colms_list:
        print(coln)
        
#list of columns with the datatypes
for col_types in companies_data_raw.dtypes:
        print(col_types)

spark.stop()