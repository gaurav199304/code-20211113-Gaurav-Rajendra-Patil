# code-20211113-Gaurav-Rajendra-Patil
BMI Calculator


Given the following json data into file data.txt



[
{"Gender": "Male","HeightCm": 175, "WeigthKg": 75},
{"Gender": "Male","HeightCm": 171, "WeigthKg": 96},
{"Gender": "Male","HeightCm": 161, "WeigthKg": 85},
{"Gender": "Male","HeightCm": 180, "WeigthKg": 77},
{"Gender": "Female","HeightCm": 166, "WeigthKg": 62},
{"Gender": "Female","HeightCm": 150, "WeigthKg": 70},
{"Gender": "Female","HeightCm": 167, "WeigthKg": 82}
]


To read the data from file and perform the operations on data I am goining to use pyspark.

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from pyspark.sql.types import StructType, StructField, StringType,IntegerType
from pyspark.sql.functions import round, col
from pyspark.sql.functions import udf, log

#create a spark session
spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()


def getCatg(R):
            if(R <= 18.4 ):
                return "Underweight"
            elif(R>=18.5 and R<=24.9):
                return "Normal weight"
            elif(R>=25 and R<=29.9):
                return "Overweight"
            elif(R>=30 and R<=34.9):
                return "Moderately obese"
            elif(R>=35 and R<=39.9):
                return "Severely obese"
            elif(R>=40):
                return "Very severely obese"

def getRisk(R):
            if(R <= 18.4 ):
                return "Malnutrition risk"
            elif(R>=18.5 and R<=24.9):
                return "Low risk"
            elif(R>=25 and R<=29.9):
                return "Enhanced risk"
            elif(R>=30 and R<=34.9):
                return "Medium risk"
            elif(R>=35 and R<=39.9):
                return "High risk"
            elif(R>=40):
                return "Very High risk"

fun = udf(getCatg)
fun1 = udf(getRisk)
# reading data from file and create a dataframe
df = spark.read.option("multiline","true").json("/home/gaurav/data.txt")
#calculate BMI using fromula and creating a new column
df1= df.withColumn('BMI',(df.WeigthKg)/(df.HeightCm**2)*10000)
df2=df1.select("*", round(col('BMI'),2)).drop('BMI').withColumnRenamed("round(BMI, 2)", "BMI")
#add two columns for BMI category and health risk by calling functions for both
res = df2.withColumn("BMICategory", fun(df2.BMI)).withColumn("Health Risk", fun1(df2.BMI))
res.show()

res.createOrReplaceTempView("BMITable")
# to count total number of overwieght people from the table
spark.sql(" select count(*) from BMITable where BMICategory='Overweight' ").show()







Documentation-


To run the code use spark submit command.

spark-submit bmical.py
