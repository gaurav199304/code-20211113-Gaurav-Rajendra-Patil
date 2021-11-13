import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from pyspark.sql.types import StructType, StructField, StringType,IntegerType
from pyspark.sql.functions import round, col
from pyspark.sql.functions import udf, log

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
df = spark.read.option("multiline","true").json("/home/gaurav/data.txt")
df1= df.withColumn('BMI',(df.WeigthKg)/(df.HeightCm**2)*10000)
df2=df1.select("*", round(col('BMI'),2)).drop('BMI').withColumnRenamed("round(BMI, 2)", "BMI")
res = df2.withColumn("BMICategory", fun(df2.BMI)).withColumn("Health Risk", fun1(df2.BMI))
res.show()
res.createOrReplaceTempView("BMITable")
spark.sql(" select count(*) from BMITable where BMICategory='Overweight' ").show()
