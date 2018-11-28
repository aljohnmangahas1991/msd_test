use default;
drop table chronicdata;
CREATE TABLE IF NOT EXISTS chronicdata(
YearStart STRING,
YearEnd STRING,
LocationAbbr STRING,
LocationDesc STRING,
Datasource STRING,
Class STRING,
Topic STRING,
Question STRING,
Data_Value_Unit STRING,
Data_Value_Type STRING,
Data_Value STRING,
Data_Value_Alt STRING,
Data_Value_Footnote_Symbol STRING,
Data_Value_Footnote STRING,
Low_Confidence_Limit STRING,
High_Confidence_Limit  STRING,
Sample_Size STRING,
Total STRING,
Age_months STRING,
Gender STRING,
RaceorEthnicity STRING,
GeoLocation STRING,
ClassID STRING,
TopicID STRING,
QuestionID STRING,
DataValueTypeID STRING,
LocationID STRING,
StratificationCategory1 STRING,
Stratification1 STRING,
StratificationCategoryId1 STRING,
StratificationID1 STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = ",",
   "quoteChar"     = "\""
)  
TBLPROPERTIES('serialization.null.format'='', "skip.header.line.count"="1");