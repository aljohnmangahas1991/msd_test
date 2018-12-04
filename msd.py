from pyspark import SparkConf, SparkContext
from pyspark.sql import HiveContext
from pyspark.sql.functions import count, avg
from subprocess import call
import os 
import itertools

dir_path = os.path.dirname(os.path.realpath(__file__))

def setUpEnvironment():
    call(['hive', '-f',dir_path+"/hive_setup.hql"])

def loadDatatoEnvironment(file):
    call(['wget','https://chronicdata.cdc.gov/views/735e-byxc/rows.csv?accessType=DOWNLOAD','-O',file])
    call(['hive', '-e',"LOAD DATA LOCAL INPATH '"+fileName+"' OVERWRITE INTO TABLE chronicdata"])

def fetchData(context, query):
    return context.sql(query)

def filterData(data, filter_condition):
    return data.filter(filter_condition)

def groupData(data, grouped_cols):
    return data.groupBy(grouped_cols)

def aggregateData(data, func, col, alias):
    return data.agg(func(col).alias(alias))

def writeToHive(data, mode, hive_table):
    data.write.mode(mode).saveAsTable(hive_table)


fileName = dir_path + '/chronicdata.csv'

conf = (SparkConf()
         .setMaster("local")
         .setAppName("MSD Test")
         .set("spark.executor.memory", "4g"))
sc = SparkContext(conf = conf)

hc = HiveContext(sc)

setUpEnvironment()
loadDatatoEnvironment(fileName)

data = fetchData(hc, "select * from chronicdata")


rawData =data.rdd.mapPartitionsWithIndex(
    lambda idx, it: itertools.islice(it, 1, None) if idx == 0 else it 
)

rawData = rawData.toDF()

cleanData = filterData(rawData , "trim(age_months)!=''")

questionA = aggregateData(groupData(cleanData, ["question","yearstart"]),avg, "Data_Value", "avgDataValue")

writeToHive(questionA, 'overwrite', 'question1')

cleanData = filterData(rawData , "gender = 'Female'")
questionB = aggregateData(groupData(cleanData, ["question","yearstart"]),avg, "Data_Value", "avgDataValue")

writeToHive(questionB, 'overwrite', 'question2')