from pyspark import SparkConf, SparkContext
from pyspark.sql import HiveContext
from pyspark.sql.functions import count, avg
from subprocess import call
import os 

dir_path = os.path.dirname(os.path.realpath(__file__))

fileName = dir_path + '/chronicdata.csv'

conf = (SparkConf()
         .setMaster("local")
         .setAppName("MSD Test")
         .set("spark.executor.memory", "4g"))
sc = SparkContext(conf = conf)

call(['wget','https://chronicdata.cdc.gov/views/735e-byxc/rows.csv?accessType=DOWNLOAD','-O',fileName])

call(['hive', '-f',dir_path+"/hive_setup.hql"])

call(['hive', '-e',"LOAD DATA LOCAL INPATH '"+fileName+"' OVERWRITE INTO TABLE chronicdata"])

hc = HiveContext(sc)

data = hc.sql("select * from chronicdata")
header = data.take(1)
cleanData = data.filter("YearStart != 'YearStart'")

b = cleanData.groupBy("question","yearstart").agg(avg("Data_Value").alias("avgDataValue"))

b.write.mode('overwrite').saveAsTable('question1')
b.write.mode('overwrite').json('/root/question1')

nextQuestion= cleanData.filter("gender = 'Female'")
c = nextQuestion.groupBy("question","yearstart").agg(avg("Data_Value").alias("avgDataValue"))

c.write.mode('overwrite').saveAsTable('question2')
c.write.mode('overwrite').json('/root/question2')
