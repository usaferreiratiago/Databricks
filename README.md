databricks-logoProject_1(Python)


 # Import Libraries 
2
 
3
from pyspark.sql import SparkSession
4
from pyspark.sql.functions import *
5
from pyspark.sql.types import *
Command took 0.07 seconds -- by usaferreiratiago@gmail.com at 8/24/2022, 1:03:08 PM on Databricks
Cmd 2
1
import numpy as np
2
import pandas as pd
3
import seaborn as sns
4
import matplotlib.pyplot as plt
5
#import findspark as fd
Command took 0.03 seconds -- by usaferreiratiago@gmail.com at 8/24/2022, 1:03:12 PM on Databricks
Cmd 3
1
dbutils.fs.ls("/FileStore/tables/Bronze/")
2
​
Out[236]: [FileInfo(path='dbfs:/FileStore/tables/Bronze/HumanResources_Department.csv', name='HumanResources_Department.csv', size=1136, modificationTime=1661338419000),
 FileInfo(path='dbfs:/FileStore/tables/Bronze/HumanResources_Employee.csv', name='HumanResources_Employee.csv', size=49935, modificationTime=1661338419000),
 FileInfo(path='dbfs:/FileStore/tables/Bronze/HumanResources_EmployeeDepartmentHistory.csv', name='HumanResources_EmployeeDepartmentHistory.csv', size=14550, modificationTime=1661338420000),
 FileInfo(path='dbfs:/FileStore/tables/Bronze/HumanResources_EmployeePayHistory.csv', name='HumanResources_EmployeePayHistory.csv', size=19343, modificationTime=1661338420000),
 FileInfo(path='dbfs:/FileStore/tables/Bronze/HumanResources_JobCandidate.csv', name='HumanResources_JobCandidate.csv', size=64287, modificationTime=1661338421000),
 FileInfo(path='dbfs:/FileStore/tables/Bronze/HumanResources_Shift.csv', name='HumanResources_Shift.csv', size=249, modificationTime=1661338421000),
 FileInfo(path='dbfs:/FileStore/tables/Bronze/Person_Address.csv', name='Person_Address.csv', size=3082183, modificationTime=1661338437000),
 FileInfo(path='dbfs:/FileStore/tables/Bronze/Person_AddressType.csv', name='Person_AddressType.csv', size=478, modificationTime=1661338422000),
 FileInfo(path='dbfs:/FileStore/tables/Bronze/Person_BusinessEntity.csv', name='Person_BusinessEntity.csv', size=1401772, modificationTime=1661338430000),
 FileInfo(path='dbfs:/FileStore/tables/Bronze/Person_BusinessEntityAddress.csv', name='Person_BusinessEntityAddress.csv', size=1478939, modificationTime=1661338438000),
 FileInfo(path='dbfs:/FileStore/tables/Bronze/Person_BusinessEntityContact.csv', name='Person_BusinessEntityContact.csv', size=67492, modificationTime=1661338438000),
 FileInfo(path='dbfs:/FileStore/tables/Bronze/Person_ContactType.csv', name='Person_ContactType.csv', size=1002, modificationTime=1661338439000),
 FileInfo(path='dbfs:/FileStore/tables/Bronze/Person_CountryRegion.csv', name='Person_CountryRegion.csv', size=9352, modificationTime=1661338439000),
 FileInfo(path='dbfs:/FileStore/tables/Bronze/Person_EPhoneNumberType.csv', name='Person_EPhoneNumberType.csv', size=136, modificationTime=1661338440000),
 FileInfo(path='dbfs:/FileStore/tables/Bronze/Person_EmailAddress.csv', name='Person_EmailAddress.csv', size=2027800, modificationTime=1661338450000),
 FileInfo(path='dbfs:/FileStore/tables/Bronze/Person_Password.csv', name='Person_Password.csv', size=2426705, modificationTime=1661338453000),
 FileInfo(path='dbfs:/FileStore/tables/Bronze/Person_Person.csv', name='Person_Person.csv', size=13646947, modificationTime=1661338522000),
 FileInfo(path='dbfs:/FileStore/tables/Bronze/Person_PersonPhone.csv', name='Person_PersonPhone.csv', size=973138, modificationTime=1661338459000),
 FileInfo(path='dbfs:/FileStore/tables/Bronze/Person_StateProvince.csv', name='Person_StateProvince.csv', size=16212, modificationTime=1661338460000),
 FileInfo(path='dbfs:/FileStore/tables/Bronze/Production_BillOfMaterials.csv', name='Production_BillOfMaterials.csv', size=217415, modificationTime=1661338463000),
 FileInfo(path='dbfs:/FileStore/tables/Bronze/Production_Culture.csv', name='Production_Culture.csv', size=391, modificationTime=1661338464000)]
Command took 0.17 seconds -- by usaferreiratiago@gmail.com at 8/24/2022, 1:03:24 PM on Databricks
Cmd 4
1
# Create SparkSession
2
 
3
spark = (
4
    SparkSession.builder
5
    .master('local')
6
    .appName('Project_01')
7
    .getOrCreate()
8
)
Command took 0.10 seconds -- by usaferreiratiago@gmail.com at 8/24/2022, 1:03:31 PM on Databricks
Cmd 5
df = spark.read.format("csv").option("infer Schema" , True) .option("header", True ) .option("sep",","). load("/FileStore/tables/Bronze/HumanResources_Department.csv")
#display(df)
#print(df.count())
df.show()
1
df = spark.read.format("csv").option("infer Schema" , True) .option("header", True ) .option("sep",","). load("/FileStore/tables/Bronze/HumanResources_Department.csv")
2
#display(df)
3
#print(df.count())
4
df.show()
(2) Spark Jobs
df:pyspark.sql.dataframe.DataFrame = [DepartmentID: string, Name: string ... 2 more fields]
+------------+--------------------+--------------------+--------------------+
|DepartmentID|                Name|           GroupName|        ModifiedDate|
+------------+--------------------+--------------------+--------------------+
|           1|         Engineering|Research and Deve...|2008-04-30 00:00:...|
|           2|         Tool Design|Research and Deve...|2008-04-30 00:00:...|
|           3|               Sales| Sales and Marketing|2008-04-30 00:00:...|
|           4|           Marketing| Sales and Marketing|2008-04-30 00:00:...|
|           5|          Purchasing|Inventory Management|2008-04-30 00:00:...|
|           6|Research and Deve...|Research and Deve...|2008-04-30 00:00:...|
|           7|          Production|       Manufacturing|2008-04-30 00:00:...|
|           8|  Production Control|       Manufacturing|2008-04-30 00:00:...|
|           9|     Human Resources|Executive General...|2008-04-30 00:00:...|
|          10|             Finance|Executive General...|2008-04-30 00:00:...|
|          11|Information Services|Executive General...|2008-04-30 00:00:...|
|          12|    Document Control|   Quality Assurance|2008-04-30 00:00:...|
|          13|   Quality Assurance|   Quality Assurance|2008-04-30 00:00:...|
|          14|Facilities and Ma...|Executive General...|2008-04-30 00:00:...|
|          15|Shipping and Rece...|Inventory Management|2008-04-30 00:00:...|
|          16|           Executive|Executive General...|2008-04-30 00:00:...|
+------------+--------------------+--------------------+--------------------+

Command took 0.92 seconds -- by usaferreiratiago@gmail.com at 8/24/2022, 1:03:41 PM on Databricks
Cmd 6
1
# Check Schema
2
df.printSchema()
root
 |-- DepartmentID: string (nullable = true)
 |-- Name: string (nullable = true)
 |-- GroupName: string (nullable = true)
 |-- ModifiedDate: string (nullable = true)

Command took 0.07 seconds -- by usaferreiratiago@gmail.com at 8/24/2022, 1:04:04 PM on Databricks
Cmd 7
1
# Checking Datas null -- Pandas has limitations #Don't use - Only try
2
 
3
df.toPandas().isna().sum()
(1) Spark Jobs
Out[240]: DepartmentID    0
Name            0
GroupName       0
ModifiedDate    0
dtype: int64
Command took 0.39 seconds -- by usaferreiratiago@gmail.com at 8/24/2022, 1:04:10 PM on Databricks
Cmd 8
1
# Searching for Nulls
2
 
3
for column in df.columns:
4
    print(column,df.filter(df[column].isNull()).count())
(8) Spark Jobs
DepartmentID 0
Name 0
GroupName 0
ModifiedDate 0
Command took 1.21 seconds -- by usaferreiratiago@gmail.com at 8/24/2022, 1:04:13 PM on Databricks
Cmd 9
1
# Rename Columns
2
# Always you need to put ## [ df = ]## to Save
3
 
4
df = df.withColumnRenamed('Modified Date','ModifiedDate')
5
df.show(truncate = False)
(1) Spark Jobs
df:pyspark.sql.dataframe.DataFrame = [DepartmentID: string, Name: string ... 2 more fields]
+------------+--------------------------+------------------------------------+-----------------------+
|DepartmentID|Name                      |GroupName                           |ModifiedDate           |
+------------+--------------------------+------------------------------------+-----------------------+
|1           |Engineering               |Research and Development            |2008-04-30 00:00:00.000|
|2           |Tool Design               |Research and Development            |2008-04-30 00:00:00.000|
|3           |Sales                     |Sales and Marketing                 |2008-04-30 00:00:00.000|
|4           |Marketing                 |Sales and Marketing                 |2008-04-30 00:00:00.000|
|5           |Purchasing                |Inventory Management                |2008-04-30 00:00:00.000|
|6           |Research and Development  |Research and Development            |2008-04-30 00:00:00.000|
|7           |Production                |Manufacturing                       |2008-04-30 00:00:00.000|
|8           |Production Control        |Manufacturing                       |2008-04-30 00:00:00.000|
|9           |Human Resources           |Executive General and Administration|2008-04-30 00:00:00.000|
|10          |Finance                   |Executive General and Administration|2008-04-30 00:00:00.000|
|11          |Information Services      |Executive General and Administration|2008-04-30 00:00:00.000|
|12          |Document Control          |Quality Assurance                   |2008-04-30 00:00:00.000|
|13          |Quality Assurance         |Quality Assurance                   |2008-04-30 00:00:00.000|
|14          |Facilities and Maintenance|Executive General and Administration|2008-04-30 00:00:00.000|
|15          |Shipping and Receiving    |Inventory Management                |2008-04-30 00:00:00.000|
|16          |Executive                 |Executive General and Administration|2008-04-30 00:00:00.000|
+------------+--------------------------+------------------------------------+-----------------------+

Command took 0.23 seconds -- by usaferreiratiago@gmail.com at 8/24/2022, 1:04:15 PM on Databricks
Cmd 10
1
# Check all Columns
2
 
3
df.columns
Out[243]: ['DepartmentID', 'Name', 'GroupName', 'ModifiedDate']
Command took 0.03 seconds -- by usaferreiratiago@gmail.com at 8/24/2022, 1:04:19 PM on Databricks
Cmd 11
1
# Select Columns
2
 
3
df.select(col('GroupName'),col('Name')).show(truncate = False)
(1) Spark Jobs
+------------------------------------+--------------------------+
|GroupName                           |Name                      |
+------------------------------------+--------------------------+
|Research and Development            |Engineering               |
|Research and Development            |Tool Design               |
|Sales and Marketing                 |Sales                     |
|Sales and Marketing                 |Marketing                 |
|Inventory Management                |Purchasing                |
|Research and Development            |Research and Development  |
|Manufacturing                       |Production                |
|Manufacturing                       |Production Control        |
|Executive General and Administration|Human Resources           |
|Executive General and Administration|Finance                   |
|Executive General and Administration|Information Services      |
|Quality Assurance                   |Document Control          |
|Quality Assurance                   |Quality Assurance         |
|Executive General and Administration|Facilities and Maintenance|
|Inventory Management                |Shipping and Receiving    |
|Executive General and Administration|Executive                 |
+------------------------------------+--------------------------+

Command took 0.31 seconds -- by usaferreiratiago@gmail.com at 8/24/2022, 1:04:22 PM on Databricks
Cmd 12
1
# Create Alias
2
# Always you need to put ## [ df = ]## to Save
3
 
4
df.select(col('Name').alias('Names')).show(truncate = False)
(1) Spark Jobs
+--------------------------+
|Names                     |
+--------------------------+
|Engineering               |
|Tool Design               |
|Sales                     |
|Marketing                 |
|Purchasing                |
|Research and Development  |
|Production                |
|Production Control        |
|Human Resources           |
|Finance                   |
|Information Services      |
|Document Control          |
|Quality Assurance         |
|Facilities and Maintenance|
|Shipping and Receiving    |
|Executive                 |
+--------------------------+

Command took 0.31 seconds -- by usaferreiratiago@gmail.com at 8/24/2022, 1:04:25 PM on Databricks
Cmd 13
1
# using Split - only to know about it, if see on another code
2
# Always you need to put ## [ df = ]## to Save
3
 
4
df.select('DepartmentID Name GroupName ModifiedDate'.split()).show(truncate = False)
(1) Spark Jobs
+------------+--------------------------+------------------------------------+-----------------------+
|DepartmentID|Name                      |GroupName                           |ModifiedDate           |
+------------+--------------------------+------------------------------------+-----------------------+
|1           |Engineering               |Research and Development            |2008-04-30 00:00:00.000|
|2           |Tool Design               |Research and Development            |2008-04-30 00:00:00.000|
|3           |Sales                     |Sales and Marketing                 |2008-04-30 00:00:00.000|
|4           |Marketing                 |Sales and Marketing                 |2008-04-30 00:00:00.000|
|5           |Purchasing                |Inventory Management                |2008-04-30 00:00:00.000|
|6           |Research and Development  |Research and Development            |2008-04-30 00:00:00.000|
|7           |Production                |Manufacturing                       |2008-04-30 00:00:00.000|
|8           |Production Control        |Manufacturing                       |2008-04-30 00:00:00.000|
|9           |Human Resources           |Executive General and Administration|2008-04-30 00:00:00.000|
|10          |Finance                   |Executive General and Administration|2008-04-30 00:00:00.000|
|11          |Information Services      |Executive General and Administration|2008-04-30 00:00:00.000|
|12          |Document Control          |Quality Assurance                   |2008-04-30 00:00:00.000|
|13          |Quality Assurance         |Quality Assurance                   |2008-04-30 00:00:00.000|
|14          |Facilities and Maintenance|Executive General and Administration|2008-04-30 00:00:00.000|
|15          |Shipping and Receiving    |Inventory Management                |2008-04-30 00:00:00.000|
|16          |Executive                 |Executive General and Administration|2008-04-30 00:00:00.000|
+------------+--------------------------+------------------------------------+-----------------------+

Command took 0.21 seconds -- by usaferreiratiago@gmail.com at 8/24/2022, 1:04:29 PM on Databricks
Cmd 14
1
# Showing Columns as you want to see 
2
 
3
df.select('Name','GroupName').show(truncate = False)
(1) Spark Jobs
+--------------------------+------------------------------------+
|Name                      |GroupName                           |
+--------------------------+------------------------------------+
|Engineering               |Research and Development            |
|Tool Design               |Research and Development            |
|Sales                     |Sales and Marketing                 |
|Marketing                 |Sales and Marketing                 |
|Purchasing                |Inventory Management                |
|Research and Development  |Research and Development            |
|Production                |Manufacturing                       |
|Production Control        |Manufacturing                       |
|Human Resources           |Executive General and Administration|
|Finance                   |Executive General and Administration|
|Information Services      |Executive General and Administration|
|Document Control          |Quality Assurance                   |
|Quality Assurance         |Quality Assurance                   |
|Facilities and Maintenance|Executive General and Administration|
|Shipping and Receiving    |Inventory Management                |
|Executive                 |Executive General and Administration|
+--------------------------+------------------------------------+

Command took 0.23 seconds -- by usaferreiratiago@gmail.com at 8/24/2022, 1:04:33 PM on Databricks
Cmd 15
1
# Filtring df  --Showing only the specific column and specific filtern and putting distinct function to not duplicate information
2
 
3
df.select(col('GroupName')).filter(col('GroupName') == "Inventory Management").distinct().show(truncate = False)
(2) Spark Jobs
+--------------------+
|GroupName           |
+--------------------+
|Inventory Management|
+--------------------+

Command took 0.58 seconds -- by usaferreiratiago@gmail.com at 8/24/2022, 1:04:36 PM on Databricks
Cmd 16
1
#Showing all df
2
 
3
df.show(truncate = False)
(1) Spark Jobs
+------------+--------------------------+------------------------------------+-----------------------+
|DepartmentID|Name                      |GroupName                           |ModifiedDate           |
+------------+--------------------------+------------------------------------+-----------------------+
|1           |Engineering               |Research and Development            |2008-04-30 00:00:00.000|
|2           |Tool Design               |Research and Development            |2008-04-30 00:00:00.000|
|3           |Sales                     |Sales and Marketing                 |2008-04-30 00:00:00.000|
|4           |Marketing                 |Sales and Marketing                 |2008-04-30 00:00:00.000|
|5           |Purchasing                |Inventory Management                |2008-04-30 00:00:00.000|
|6           |Research and Development  |Research and Development            |2008-04-30 00:00:00.000|
|7           |Production                |Manufacturing                       |2008-04-30 00:00:00.000|
|8           |Production Control        |Manufacturing                       |2008-04-30 00:00:00.000|
|9           |Human Resources           |Executive General and Administration|2008-04-30 00:00:00.000|
|10          |Finance                   |Executive General and Administration|2008-04-30 00:00:00.000|
|11          |Information Services      |Executive General and Administration|2008-04-30 00:00:00.000|
|12          |Document Control          |Quality Assurance                   |2008-04-30 00:00:00.000|
|13          |Quality Assurance         |Quality Assurance                   |2008-04-30 00:00:00.000|
|14          |Facilities and Maintenance|Executive General and Administration|2008-04-30 00:00:00.000|
|15          |Shipping and Receiving    |Inventory Management                |2008-04-30 00:00:00.000|
|16          |Executive                 |Executive General and Administration|2008-04-30 00:00:00.000|
+------------+--------------------------+------------------------------------+-----------------------+

Command took 0.26 seconds -- by usaferreiratiago@gmail.com at 8/24/2022, 1:04:38 PM on Databricks
Cmd 17
1
# Filtring df with more conditions and specific columns (AND / &)
2
 
3
df.select('Name','ModifiedDate').filter((col('Name') == "Finance")).show(truncate = False)
(1) Spark Jobs
+-------+-----------------------+
|Name   |ModifiedDate           |
+-------+-----------------------+
|Finance|2008-04-30 00:00:00.000|
+-------+-----------------------+

Command took 0.30 seconds -- by usaferreiratiago@gmail.com at 8/24/2022, 1:04:42 PM on Databricks
Cmd 18
1
# Filtring df with specific columns and more conditions  (AND / &)
2
 
3
df.select('DepartmentID','Name').filter((col('Name') == "Finance") & (col('DepartmentID') == 10)).show(truncate = False) 
(1) Spark Jobs
+------------+-------+
|DepartmentID|Name   |
+------------+-------+
|10          |Finance|
+------------+-------+

Command took 0.43 seconds -- by usaferreiratiago@gmail.com at 8/24/2022, 1:04:45 PM on Databricks
Cmd 19
1
# Filtring df with specific columns and more conditions  (AND / &)
2
 
3
df.select('DepartmentId','Name').filter((col('DepartmentID') == 2)).show(truncate = False)
(1) Spark Jobs
+------------+-----------+
|DepartmentId|Name       |
+------------+-----------+
|2           |Tool Design|
+------------+-----------+

Command took 0.33 seconds -- by usaferreiratiago@gmail.com at 8/24/2022, 1:04:47 PM on Databricks
Cmd 20
1
# Filtring df with specific columns and more conditions  (AND / &)
2
 
3
df.select('DepartmentId','Name').filter((col('DepartmentID') != 12)).show(truncate = False)
(1) Spark Jobs
+------------+--------------------------+
|DepartmentId|Name                      |
+------------+--------------------------+
|1           |Engineering               |
|2           |Tool Design               |
|3           |Sales                     |
|4           |Marketing                 |
|5           |Purchasing                |
|6           |Research and Development  |
|7           |Production                |
|8           |Production Control        |
|9           |Human Resources           |
|10          |Finance                   |
|11          |Information Services      |
|13          |Quality Assurance         |
|14          |Facilities and Maintenance|
|15          |Shipping and Receiving    |
|16          |Executive                 |
+------------+--------------------------+

Command took 0.38 seconds -- by usaferreiratiago@gmail.com at 8/24/2022, 1:04:53 PM on Databricks
Cmd 21
1
# Filtring df with specific columns and more conditions  (AND / &)
2
## df.filter('Name = "Finance"').filter(col('DepartmentId') == 1).show()
3
 
4
df.select('DepartmentId','Name').filter((col('DepartmentID') >= 3)).show(truncate = False)
(1) Spark Jobs
+------------+--------------------------+
|DepartmentId|Name                      |
+------------+--------------------------+
|3           |Sales                     |
|4           |Marketing                 |
|5           |Purchasing                |
|6           |Research and Development  |
|7           |Production                |
|8           |Production Control        |
|9           |Human Resources           |
|10          |Finance                   |
|11          |Information Services      |
|12          |Document Control          |
|13          |Quality Assurance         |
|14          |Facilities and Maintenance|
|15          |Shipping and Receiving    |
|16          |Executive                 |
+------------+--------------------------+

Command took 0.35 seconds -- by usaferreiratiago@gmail.com at 8/24/2022, 1:04:56 PM on Databricks
Cmd 22
1
# Filtring df with specific columns and more conditions  (AND / &)
2
 
3
df.select('DepartmentId','Name').filter((col('DepartmentID') <= 16)).show(truncate = False)
(1) Spark Jobs
+------------+--------------------------+
|DepartmentId|Name                      |
+------------+--------------------------+
|1           |Engineering               |
|2           |Tool Design               |
|3           |Sales                     |
|4           |Marketing                 |
|5           |Purchasing                |
|6           |Research and Development  |
|7           |Production                |
|8           |Production Control        |
|9           |Human Resources           |
|10          |Finance                   |
|11          |Information Services      |
|12          |Document Control          |
|13          |Quality Assurance         |
|14          |Facilities and Maintenance|
|15          |Shipping and Receiving    |
|16          |Executive                 |
+------------+--------------------------+

Command took 0.27 seconds -- by usaferreiratiago@gmail.com at 8/24/2022, 1:04:59 PM on Databricks
Cmd 23
1
# Filtring df with more conditions (OR / |)
2
 
3
df.filter('DepartmentID = "1"').show(truncate = False)
(1) Spark Jobs
+------------+-----------+------------------------+-----------------------+
|DepartmentID|Name       |GroupName               |ModifiedDate           |
+------------+-----------+------------------------+-----------------------+
|1           |Engineering|Research and Development|2008-04-30 00:00:00.000|
+------------+-----------+------------------------+-----------------------+

Command took 0.38 seconds -- by usaferreiratiago@gmail.com at 8/24/2022, 1:05:03 PM on Databricks
Cmd 24
1
# Filtring df with more conditions (OR / |)
2
 
3
df.filter((col('Name') == 'Finance') | (col('Name') == 'Sales') | (col('DepartmentID') == 12)).show(truncate = False)
(1) Spark Jobs
+------------+----------------+------------------------------------+-----------------------+
|DepartmentID|Name            |GroupName                           |ModifiedDate           |
+------------+----------------+------------------------------------+-----------------------+
|3           |Sales           |Sales and Marketing                 |2008-04-30 00:00:00.000|
|10          |Finance         |Executive General and Administration|2008-04-30 00:00:00.000|
|12          |Document Control|Quality Assurance                   |2008-04-30 00:00:00.000|
+------------+----------------+------------------------------------+-----------------------+

Command took 0.32 seconds -- by usaferreiratiago@gmail.com at 8/24/2022, 1:05:06 PM on Databricks
Cmd 25
1
# # Filtring df with more conditions (OR / |)
2
 
3
df.filter(col('GroupName') == 'Quality Assurance').show(truncate = False)
(1) Spark Jobs
+------------+-----------------+-----------------+-----------------------+
|DepartmentID|Name             |GroupName        |ModifiedDate           |
+------------+-----------------+-----------------+-----------------------+
|12          |Document Control |Quality Assurance|2008-04-30 00:00:00.000|
|13          |Quality Assurance|Quality Assurance|2008-04-30 00:00:00.000|
+------------+-----------------+-----------------+-----------------------+

Command took 0.26 seconds -- by usaferreiratiago@gmail.com at 8/24/2022, 1:05:11 PM on Databricks
Cmd 26
1
# # Filtring df combining & and | # And e OR #
2
 
3
df.filter((col('GroupName') == "Quality Assurance")  | (col('Name') == "Sales") | (col('DepartmentID') == 10)).show(truncate = False)
(1) Spark Jobs
+------------+-----------------+------------------------------------+-----------------------+
|DepartmentID|Name             |GroupName                           |ModifiedDate           |
+------------+-----------------+------------------------------------+-----------------------+
|3           |Sales            |Sales and Marketing                 |2008-04-30 00:00:00.000|
|10          |Finance          |Executive General and Administration|2008-04-30 00:00:00.000|
|12          |Document Control |Quality Assurance                   |2008-04-30 00:00:00.000|
|13          |Quality Assurance|Quality Assurance                   |2008-04-30 00:00:00.000|
+------------+-----------------+------------------------------------+-----------------------+

Command took 0.34 seconds -- by usaferreiratiago@gmail.com at 8/24/2022, 1:05:14 PM on Databricks
Cmd 27
1
# Concatenate Columns without space
2
 
3
df.withColumn("DepartmentID + GroupName", concat('DepartmentID','GroupName')).show()
(1) Spark Jobs
+------------+--------------------+--------------------+--------------------+------------------------+
|DepartmentID|                Name|           GroupName|        ModifiedDate|DepartmentID + GroupName|
+------------+--------------------+--------------------+--------------------+------------------------+
|           1|         Engineering|Research and Deve...|2008-04-30 00:00:...|    1Research and Dev...|
|           2|         Tool Design|Research and Deve...|2008-04-30 00:00:...|    2Research and Dev...|
|           3|               Sales| Sales and Marketing|2008-04-30 00:00:...|    3Sales and Marketing|
|           4|           Marketing| Sales and Marketing|2008-04-30 00:00:...|    4Sales and Marketing|
|           5|          Purchasing|Inventory Management|2008-04-30 00:00:...|    5Inventory Manage...|
|           6|Research and Deve...|Research and Deve...|2008-04-30 00:00:...|    6Research and Dev...|
|           7|          Production|       Manufacturing|2008-04-30 00:00:...|          7Manufacturing|
|           8|  Production Control|       Manufacturing|2008-04-30 00:00:...|          8Manufacturing|
|           9|     Human Resources|Executive General...|2008-04-30 00:00:...|    9Executive Genera...|
|          10|             Finance|Executive General...|2008-04-30 00:00:...|    10Executive Gener...|
|          11|Information Services|Executive General...|2008-04-30 00:00:...|    11Executive Gener...|
|          12|    Document Control|   Quality Assurance|2008-04-30 00:00:...|     12Quality Assurance|
|          13|   Quality Assurance|   Quality Assurance|2008-04-30 00:00:...|     13Quality Assurance|
|          14|Facilities and Ma...|Executive General...|2008-04-30 00:00:...|    14Executive Gener...|
|          15|Shipping and Rece...|Inventory Management|2008-04-30 00:00:...|    15Inventory Manag...|
|          16|           Executive|Executive General...|2008-04-30 00:00:...|    16Executive Gener...|
+------------+--------------------+--------------------+--------------------+------------------------+

Command took 0.26 seconds -- by usaferreiratiago@gmail.com at 8/24/2022, 1:05:18 PM on Databricks
Cmd 28
1
# Concatenate Columns with space
2
 
3
df.withColumn("DepartmentID + GroupName", concat_ws(' ', 'DepartmentID','GroupName')).show()
(1) Spark Jobs
+------------+--------------------+--------------------+--------------------+------------------------+
|DepartmentID|                Name|           GroupName|        ModifiedDate|DepartmentID + GroupName|
+------------+--------------------+--------------------+--------------------+------------------------+
|           1|         Engineering|Research and Deve...|2008-04-30 00:00:...|    1 Research and De...|
|           2|         Tool Design|Research and Deve...|2008-04-30 00:00:...|    2 Research and De...|
|           3|               Sales| Sales and Marketing|2008-04-30 00:00:...|    3 Sales and Marke...|
|           4|           Marketing| Sales and Marketing|2008-04-30 00:00:...|    4 Sales and Marke...|
|           5|          Purchasing|Inventory Management|2008-04-30 00:00:...|    5 Inventory Manag...|
|           6|Research and Deve...|Research and Deve...|2008-04-30 00:00:...|    6 Research and De...|
|           7|          Production|       Manufacturing|2008-04-30 00:00:...|         7 Manufacturing|
|           8|  Production Control|       Manufacturing|2008-04-30 00:00:...|         8 Manufacturing|
|           9|     Human Resources|Executive General...|2008-04-30 00:00:...|    9 Executive Gener...|
|          10|             Finance|Executive General...|2008-04-30 00:00:...|    10 Executive Gene...|
|          11|Information Services|Executive General...|2008-04-30 00:00:...|    11 Executive Gene...|
|          12|    Document Control|   Quality Assurance|2008-04-30 00:00:...|    12 Quality Assurance|
|          13|   Quality Assurance|   Quality Assurance|2008-04-30 00:00:...|    13 Quality Assurance|
|          14|Facilities and Ma...|Executive General...|2008-04-30 00:00:...|    14 Executive Gene...|
|          15|Shipping and Rece...|Inventory Management|2008-04-30 00:00:...|    15 Inventory Mana...|
|          16|           Executive|Executive General...|2008-04-30 00:00:...|    16 Executive Gene...|
+------------+--------------------+--------------------+--------------------+------------------------+

Command took 0.29 seconds -- by usaferreiratiago@gmail.com at 8/24/2022, 1:05:20 PM on Databricks
Cmd 29
1
# Alter type of Column
2
 
3
df.show(truncate = False)
(1) Spark Jobs
+------------+--------------------------+------------------------------------+-----------------------+
|DepartmentID|Name                      |GroupName                           |ModifiedDate           |
+------------+--------------------------+------------------------------------+-----------------------+
|1           |Engineering               |Research and Development            |2008-04-30 00:00:00.000|
|2           |Tool Design               |Research and Development            |2008-04-30 00:00:00.000|
|3           |Sales                     |Sales and Marketing                 |2008-04-30 00:00:00.000|
|4           |Marketing                 |Sales and Marketing                 |2008-04-30 00:00:00.000|
|5           |Purchasing                |Inventory Management                |2008-04-30 00:00:00.000|
|6           |Research and Development  |Research and Development            |2008-04-30 00:00:00.000|
|7           |Production                |Manufacturing                       |2008-04-30 00:00:00.000|
|8           |Production Control        |Manufacturing                       |2008-04-30 00:00:00.000|
|9           |Human Resources           |Executive General and Administration|2008-04-30 00:00:00.000|
|10          |Finance                   |Executive General and Administration|2008-04-30 00:00:00.000|
|11          |Information Services      |Executive General and Administration|2008-04-30 00:00:00.000|
|12          |Document Control          |Quality Assurance                   |2008-04-30 00:00:00.000|
|13          |Quality Assurance         |Quality Assurance                   |2008-04-30 00:00:00.000|
|14          |Facilities and Maintenance|Executive General and Administration|2008-04-30 00:00:00.000|
|15          |Shipping and Receiving    |Inventory Management                |2008-04-30 00:00:00.000|
|16          |Executive                 |Executive General and Administration|2008-04-30 00:00:00.000|
+------------+--------------------------+------------------------------------+-----------------------+

Command took 0.19 seconds -- by usaferreiratiago@gmail.com at 8/24/2022, 1:05:23 PM on Databricks
Cmd 30
1
# Alter type of Column
2
 
3
df.printSchema()
root
 |-- DepartmentID: string (nullable = true)
 |-- Name: string (nullable = true)
 |-- GroupName: string (nullable = true)
 |-- ModifiedDate: string (nullable = true)

Command took 0.04 seconds -- by usaferreiratiago@gmail.com at 8/24/2022, 1:05:26 PM on Databricks
Cmd 31
1
# Alter type of Metada of the Column
2
 
3
#df.withColumn('ModifiedDate', col('ModifiedDate').cast(IntegerType())).show(truncate=False)
4
 
5
df.show()
(1) Spark Jobs
+------------+--------------------+--------------------+--------------------+
|DepartmentID|                Name|           GroupName|        ModifiedDate|
+------------+--------------------+--------------------+--------------------+
|           1|         Engineering|Research and Deve...|2008-04-30 00:00:...|
|           2|         Tool Design|Research and Deve...|2008-04-30 00:00:...|
|           3|               Sales| Sales and Marketing|2008-04-30 00:00:...|
|           4|           Marketing| Sales and Marketing|2008-04-30 00:00:...|
|           5|          Purchasing|Inventory Management|2008-04-30 00:00:...|
|           6|Research and Deve...|Research and Deve...|2008-04-30 00:00:...|
|           7|          Production|       Manufacturing|2008-04-30 00:00:...|
|           8|  Production Control|       Manufacturing|2008-04-30 00:00:...|
|           9|     Human Resources|Executive General...|2008-04-30 00:00:...|
|          10|             Finance|Executive General...|2008-04-30 00:00:...|
|          11|Information Services|Executive General...|2008-04-30 00:00:...|
|          12|    Document Control|   Quality Assurance|2008-04-30 00:00:...|
|          13|   Quality Assurance|   Quality Assurance|2008-04-30 00:00:...|
|          14|Facilities and Ma...|Executive General...|2008-04-30 00:00:...|
|          15|Shipping and Rece...|Inventory Management|2008-04-30 00:00:...|
|          16|           Executive|Executive General...|2008-04-30 00:00:...|
+------------+--------------------+--------------------+--------------------+

Command took 0.28 seconds -- by usaferreiratiago@gmail.com at 8/24/2022, 1:05:31 PM on Databricks
Cmd 32
1
# Alter type of Column
2
# Didn't change yet because we didn't put variable "" df = df.withColumn('ModifiedDate', col('ModifiedDate').cast(IntegerType())).show(truncate=False)"  bfore the execution of code,
3
# only when we put this everything will change
4
 
5
df.printSchema()
root
 |-- DepartmentID: string (nullable = true)
 |-- Name: string (nullable = true)
 |-- GroupName: string (nullable = true)
 |-- ModifiedDate: string (nullable = true)

Command took 0.09 seconds -- by usaferreiratiago@gmail.com at 8/24/2022, 1:05:34 PM on Databricks
Cmd 33
1
#The below example returns the difference between two dates using datediff().
2
 
3
df.select(col('ModifiedDate'),
4
         datediff(current_timestamp(), col('ModifiedDate')).alias('difference between two dates')).show(truncate=False)
(1) Spark Jobs
+-----------------------+----------------------------+
|ModifiedDate           |difference between two dates|
+-----------------------+----------------------------+
|2008-04-30 00:00:00.000|5229                        |
|2008-04-30 00:00:00.000|5229                        |
|2008-04-30 00:00:00.000|5229                        |
|2008-04-30 00:00:00.000|5229                        |
|2008-04-30 00:00:00.000|5229                        |
|2008-04-30 00:00:00.000|5229                        |
|2008-04-30 00:00:00.000|5229                        |
|2008-04-30 00:00:00.000|5229                        |
|2008-04-30 00:00:00.000|5229                        |
|2008-04-30 00:00:00.000|5229                        |
|2008-04-30 00:00:00.000|5229                        |
|2008-04-30 00:00:00.000|5229                        |
|2008-04-30 00:00:00.000|5229                        |
|2008-04-30 00:00:00.000|5229                        |
|2008-04-30 00:00:00.000|5229                        |
|2008-04-30 00:00:00.000|5229                        |
+-----------------------+----------------------------+

Command took 0.42 seconds -- by usaferreiratiago@gmail.com at 8/24/2022, 1:05:36 PM on Databricks
Cmd 34
1
#The below example returns the months between two dates 
2
 
3
df.select(col("ModifiedDate"), 
4
    months_between(current_timestamp(),col("ModifiedDate")).alias("months_between")  
5
  ).show()
6
 
7
#round(col("score")
(1) Spark Jobs
+--------------------+--------------+
|        ModifiedDate|months_between|
+--------------------+--------------+
|2008-04-30 00:00:...|  171.82136313|
|2008-04-30 00:00:...|  171.82136313|
|2008-04-30 00:00:...|  171.82136313|
|2008-04-30 00:00:...|  171.82136313|
|2008-04-30 00:00:...|  171.82136313|
|2008-04-30 00:00:...|  171.82136313|
|2008-04-30 00:00:...|  171.82136313|
|2008-04-30 00:00:...|  171.82136313|
|2008-04-30 00:00:...|  171.82136313|
|2008-04-30 00:00:...|  171.82136313|
|2008-04-30 00:00:...|  171.82136313|
|2008-04-30 00:00:...|  171.82136313|
|2008-04-30 00:00:...|  171.82136313|
|2008-04-30 00:00:...|  171.82136313|
|2008-04-30 00:00:...|  171.82136313|
|2008-04-30 00:00:...|  171.82136313|
+--------------------+--------------+

Command took 0.44 seconds -- by usaferreiratiago@gmail.com at 8/24/2022, 1:05:39 PM on Databricks
Cmd 35
1
#Using round numbers ('Arredontar numeros')
2
 
3
df.select("*",round(col("DepartmentID")).alias("Teste")).show(truncate=False)
(1) Spark Jobs
+------------+--------------------------+------------------------------------+-----------------------+-----+
|DepartmentID|Name                      |GroupName                           |ModifiedDate           |Teste|
+------------+--------------------------+------------------------------------+-----------------------+-----+
|1           |Engineering               |Research and Development            |2008-04-30 00:00:00.000|1.0  |
|2           |Tool Design               |Research and Development            |2008-04-30 00:00:00.000|2.0  |
|3           |Sales                     |Sales and Marketing                 |2008-04-30 00:00:00.000|3.0  |
|4           |Marketing                 |Sales and Marketing                 |2008-04-30 00:00:00.000|4.0  |
|5           |Purchasing                |Inventory Management                |2008-04-30 00:00:00.000|5.0  |
|6           |Research and Development  |Research and Development            |2008-04-30 00:00:00.000|6.0  |
|7           |Production                |Manufacturing                       |2008-04-30 00:00:00.000|7.0  |
|8           |Production Control        |Manufacturing                       |2008-04-30 00:00:00.000|8.0  |
|9           |Human Resources           |Executive General and Administration|2008-04-30 00:00:00.000|9.0  |
|10          |Finance                   |Executive General and Administration|2008-04-30 00:00:00.000|10.0 |
|11          |Information Services      |Executive General and Administration|2008-04-30 00:00:00.000|11.0 |
|12          |Document Control          |Quality Assurance                   |2008-04-30 00:00:00.000|12.0 |
|13          |Quality Assurance         |Quality Assurance                   |2008-04-30 00:00:00.000|13.0 |
|14          |Facilities and Maintenance|Executive General and Administration|2008-04-30 00:00:00.000|14.0 |
|15          |Shipping and Receiving    |Inventory Management                |2008-04-30 00:00:00.000|15.0 |
|16          |Executive                 |Executive General and Administration|2008-04-30 00:00:00.000|16.0 |
+------------+--------------------------+------------------------------------+-----------------------+-----+

Command took 0.39 seconds -- by usaferreiratiago@gmail.com at 8/24/2022, 1:05:46 PM on Databricks
Cmd 36
1
## test modifying dates##
2
 
3
#(df_date
4
#.withColumn("to_date", f.to_date("input_date"))
5
 
6
df.withColumn("year",year("ModifiedDate")).show(2)
7
df.withColumn("quarter", quarter("ModifiedDate")).show(2)
8
df.withColumn("month",month("ModifiedDate")).show(2)
9
df.withColumn("week",weekofyear("ModifiedDate")).show(2)
10
df.withColumn("dayofyear",dayofyear("ModifiedDate")).show(2)
11
df.withColumn("dayofmonth ",dayofmonth("ModifiedDate")).show(2)
12
df.withColumn("dayofweek" , dayofweek("ModifiedDate")).show(2)
(7) Spark Jobs
+------------+-----------+--------------------+--------------------+----+
|DepartmentID|       Name|           GroupName|        ModifiedDate|year|
+------------+-----------+--------------------+--------------------+----+
|           1|Engineering|Research and Deve...|2008-04-30 00:00:...|2008|
|           2|Tool Design|Research and Deve...|2008-04-30 00:00:...|2008|
+------------+-----------+--------------------+--------------------+----+
only showing top 2 rows

+------------+-----------+--------------------+--------------------+-------+
|DepartmentID|       Name|           GroupName|        ModifiedDate|quarter|
+------------+-----------+--------------------+--------------------+-------+
|           1|Engineering|Research and Deve...|2008-04-30 00:00:...|      2|
|           2|Tool Design|Research and Deve...|2008-04-30 00:00:...|      2|
+------------+-----------+--------------------+--------------------+-------+
only showing top 2 rows

+------------+-----------+--------------------+--------------------+-----+
|DepartmentID|       Name|           GroupName|        ModifiedDate|month|
+------------+-----------+--------------------+--------------------+-----+
|           1|Engineering|Research and Deve...|2008-04-30 00:00:...|    4|
|           2|Tool Design|Research and Deve...|2008-04-30 00:00:...|    4|
+------------+-----------+--------------------+--------------------+-----+
only showing top 2 rows

+------------+-----------+--------------------+--------------------+----+
|DepartmentID|       Name|           GroupName|        ModifiedDate|week|
+------------+-----------+--------------------+--------------------+----+
|           1|Engineering|Research and Deve...|2008-04-30 00:00:...|  18|
|           2|Tool Design|Research and Deve...|2008-04-30 00:00:...|  18|
+------------+-----------+--------------------+--------------------+----+
only showing top 2 rows

+------------+-----------+--------------------+--------------------+---------+
|DepartmentID|       Name|           GroupName|        ModifiedDate|dayofyear|
+------------+-----------+--------------------+--------------------+---------+
|           1|Engineering|Research and Deve...|2008-04-30 00:00:...|      121|
|           2|Tool Design|Research and Deve...|2008-04-30 00:00:...|      121|
+------------+-----------+--------------------+--------------------+---------+
only showing top 2 rows

+------------+-----------+--------------------+--------------------+-----------+
|DepartmentID|       Name|           GroupName|        ModifiedDate|dayofmonth |
+------------+-----------+--------------------+--------------------+-----------+
|           1|Engineering|Research and Deve...|2008-04-30 00:00:...|         30|
|           2|Tool Design|Research and Deve...|2008-04-30 00:00:...|         30|
+------------+-----------+--------------------+--------------------+-----------+
only showing top 2 rows

+------------+-----------+--------------------+--------------------+---------+
|DepartmentID|       Name|           GroupName|        ModifiedDate|dayofweek|
+------------+-----------+--------------------+--------------------+---------+
|           1|Engineering|Research and Deve...|2008-04-30 00:00:...|        4|
|           2|Tool Design|Research and Deve...|2008-04-30 00:00:...|        4|
+------------+-----------+--------------------+--------------------+---------+
only showing top 2 rows

Command took 2.41 seconds -- by usaferreiratiago@gmail.com at 8/24/2022, 1:06:03 PM on Databricks
Cmd 37
1
# Extract Hour, Minutes and Seconds
2
 
3
 
4
#(df_date
5
#.withColumn("to_timestamp",f.to_timestamp("input_date"))
6
df.withColumn("hour", hour("ModifiedDate")).show(2)
7
df.withColumn("minute",minute("ModifiedDate")).show(2)
8
df.withColumn("second",second("ModifiedDate")).show(2)
(3) Spark Jobs
+------------+-----------+--------------------+--------------------+----+
|DepartmentID|       Name|           GroupName|        ModifiedDate|hour|
+------------+-----------+--------------------+--------------------+----+
|           1|Engineering|Research and Deve...|2008-04-30 00:00:...|   0|
|           2|Tool Design|Research and Deve...|2008-04-30 00:00:...|   0|
+------------+-----------+--------------------+--------------------+----+
only showing top 2 rows

+------------+-----------+--------------------+--------------------+------+
|DepartmentID|       Name|           GroupName|        ModifiedDate|minute|
+------------+-----------+--------------------+--------------------+------+
|           1|Engineering|Research and Deve...|2008-04-30 00:00:...|     0|
|           2|Tool Design|Research and Deve...|2008-04-30 00:00:...|     0|
+------------+-----------+--------------------+--------------------+------+
only showing top 2 rows

+------------+-----------+--------------------+--------------------+------+
|DepartmentID|       Name|           GroupName|        ModifiedDate|second|
+------------+-----------+--------------------+--------------------+------+
|           1|Engineering|Research and Deve...|2008-04-30 00:00:...|     0|
|           2|Tool Design|Research and Deve...|2008-04-30 00:00:...|     0|
+------------+-----------+--------------------+--------------------+------+
only showing top 2 rows

Command took 1.10 seconds -- by usaferreiratiago@gmail.com at 8/24/2022, 1:06:05 PM on Databricks
Cmd 38
1
#Days and Month in Words
2
 
3
df.withColumn("dayofweek" ,dayofweek("ModifiedDate")).show(2)
4
df.withColumn("dayinwords",date_format("ModifiedDate" , "EEEE")).show(2)
5
##df.withColumn("monthinwords", date_format("ModifiedDate" , "LLLL")).show(2)
(2) Spark Jobs
+------------+-----------+--------------------+--------------------+---------+
|DepartmentID|       Name|           GroupName|        ModifiedDate|dayofweek|
+------------+-----------+--------------------+--------------------+---------+
|           1|Engineering|Research and Deve...|2008-04-30 00:00:...|        4|
|           2|Tool Design|Research and Deve...|2008-04-30 00:00:...|        4|
+------------+-----------+--------------------+--------------------+---------+
only showing top 2 rows

+------------+-----------+--------------------+--------------------+----------+
|DepartmentID|       Name|           GroupName|        ModifiedDate|dayinwords|
+------------+-----------+--------------------+--------------------+----------+
|           1|Engineering|Research and Deve...|2008-04-30 00:00:...| Wednesday|
|           2|Tool Design|Research and Deve...|2008-04-30 00:00:...| Wednesday|
+------------+-----------+--------------------+--------------------+----------+
only showing top 2 rows

Command took 0.45 seconds -- by usaferreiratiago@gmail.com at 8/24/2022, 1:06:08 PM on Databricks
Cmd 39
1
#Hadling Dates
2
 
3
df.withColumn("cur_date",current_date()).show(2)
4
df.withColumn("Days",datediff(current_date(),"ModifiedDate" )).show(2) 
5
df.withColumn("dateadd" ,date_add("ModifiedDate",5)).show(2) 
6
df.withColumn("datesub" ,date_sub("ModifiedDate",5)).show(2) 
7
df.withColumn("datetrnc",date_trunc('mm' , "ModifiedDate")).show(2) 
(5) Spark Jobs
+------------+-----------+--------------------+--------------------+----------+
|DepartmentID|       Name|           GroupName|        ModifiedDate|  cur_date|
+------------+-----------+--------------------+--------------------+----------+
|           1|Engineering|Research and Deve...|2008-04-30 00:00:...|2022-08-24|
|           2|Tool Design|Research and Deve...|2008-04-30 00:00:...|2022-08-24|
+------------+-----------+--------------------+--------------------+----------+
only showing top 2 rows

+------------+-----------+--------------------+--------------------+----+
|DepartmentID|       Name|           GroupName|        ModifiedDate|Days|
+------------+-----------+--------------------+--------------------+----+
|           1|Engineering|Research and Deve...|2008-04-30 00:00:...|5229|
|           2|Tool Design|Research and Deve...|2008-04-30 00:00:...|5229|
+------------+-----------+--------------------+--------------------+----+
only showing top 2 rows

+------------+-----------+--------------------+--------------------+----------+
|DepartmentID|       Name|           GroupName|        ModifiedDate|   dateadd|
+------------+-----------+--------------------+--------------------+----------+
|           1|Engineering|Research and Deve...|2008-04-30 00:00:...|2008-05-05|
|           2|Tool Design|Research and Deve...|2008-04-30 00:00:...|2008-05-05|
+------------+-----------+--------------------+--------------------+----------+
only showing top 2 rows

+------------+-----------+--------------------+--------------------+----------+
|DepartmentID|       Name|           GroupName|        ModifiedDate|   datesub|
+------------+-----------+--------------------+--------------------+----------+
|           1|Engineering|Research and Deve...|2008-04-30 00:00:...|2008-04-25|
|           2|Tool Design|Research and Deve...|2008-04-30 00:00:...|2008-04-25|
+------------+-----------+--------------------+--------------------+----------+
only showing top 2 rows

+------------+-----------+--------------------+--------------------+-------------------+
|DepartmentID|       Name|           GroupName|        ModifiedDate|           datetrnc|
+------------+-----------+--------------------+--------------------+-------------------+
|           1|Engineering|Research and Deve...|2008-04-30 00:00:...|2008-04-01 00:00:00|
|           2|Tool Design|Research and Deve...|2008-04-30 00:00:...|2008-04-01 00:00:00|
+------------+-----------+--------------------+--------------------+-------------------+
only showing top 2 rows

Command took 1.55 seconds -- by usaferreiratiago@gmail.com at 8/24/2022, 1:06:10 PM on Databricks
Cmd 40
1
#Using Distinct #
2
 
3
df.select(col('GroupName')).distinct().show(truncate=False)
(2) Spark Jobs
+------------------------------------+
|GroupName                           |
+------------------------------------+
|Executive General and Administration|
|Sales and Marketing                 |
|Research and Development            |
|Quality Assurance                   |
|Manufacturing                       |
|Inventory Management                |
+------------------------------------+

Command took 0.53 seconds -- by usaferreiratiago@gmail.com at 8/24/2022, 1:06:13 PM on Databricks
Cmd 41
1
# Using Collect - show all the rows#
2
 
3
df.select(col('GroupName')).distinct().collect()
(2) Spark Jobs
Out[274]: [Row(GroupName='Executive General and Administration'),
 Row(GroupName='Sales and Marketing'),
 Row(GroupName='Research and Development'),
 Row(GroupName='Quality Assurance'),
 Row(GroupName='Manufacturing'),
 Row(GroupName='Inventory Management')]
Command took 0.46 seconds -- by usaferreiratiago@gmail.com at 8/24/2022, 1:06:16 PM on Databricks
Cmd 42
1
list = df.select(col('GroupName')).collect()
(1) Spark Jobs
Command took 0.39 seconds -- by usaferreiratiago@gmail.com at 8/24/2022, 1:06:23 PM on Databricks
Cmd 43
1
type(list[0][0])
Out[276]: str
Command took 0.10 seconds -- by usaferreiratiago@gmail.com at 8/24/2022, 1:06:25 PM on Databricks
Cmd 44
1
list[5][0]
Out[277]: 'Research and Development'
Command took 0.12 seconds -- by usaferreiratiago@gmail.com at 8/24/2022, 1:06:27 PM on Databricks
Cmd 45
1
list[0][0]
Out[278]: 'Research and Development'
Command took 0.10 seconds -- by usaferreiratiago@gmail.com at 8/24/2022, 1:06:28 PM on Databricks
Cmd 46
1
# Generating a list GroupName = []
2
 
3
for GroupName in list:
4
    GroupName.asDict(GroupName[0])
5
GroupName
Out[279]: Row(GroupName='Executive General and Administration')
Command took 0.05 seconds -- by usaferreiratiago@gmail.com at 8/24/2022, 1:06:33 PM on Databricks
Cmd 47
1
## Working with When () / Otherwise()##
2
 
3
##df.withColumn('Correct', when(col('GroupName') == "Manufacturing", lit("OK"))).otherwise("NOT")
4
 
5
df.withColumn("Correct", when(col("GroupName") == "Manufacturing" , lit("OK")).otherwise("")).distinct().show(truncate=False)
(2) Spark Jobs
+------------+--------------------------+------------------------------------+-----------------------+-------+
|DepartmentID|Name                      |GroupName                           |ModifiedDate           |Correct|
+------------+--------------------------+------------------------------------+-----------------------+-------+
|12          |Document Control          |Quality Assurance                   |2008-04-30 00:00:00.000|       |
|9           |Human Resources           |Executive General and Administration|2008-04-30 00:00:00.000|       |
|6           |Research and Development  |Research and Development            |2008-04-30 00:00:00.000|       |
|3           |Sales                     |Sales and Marketing                 |2008-04-30 00:00:00.000|       |
|2           |Tool Design               |Research and Development            |2008-04-30 00:00:00.000|       |
|8           |Production Control        |Manufacturing                       |2008-04-30 00:00:00.000|OK     |
|15          |Shipping and Receiving    |Inventory Management                |2008-04-30 00:00:00.000|       |
|16          |Executive                 |Executive General and Administration|2008-04-30 00:00:00.000|       |
|10          |Finance                   |Executive General and Administration|2008-04-30 00:00:00.000|       |
|14          |Facilities and Maintenance|Executive General and Administration|2008-04-30 00:00:00.000|       |
|4           |Marketing                 |Sales and Marketing                 |2008-04-30 00:00:00.000|       |
|7           |Production                |Manufacturing                       |2008-04-30 00:00:00.000|OK     |
|11          |Information Services      |Executive General and Administration|2008-04-30 00:00:00.000|       |
|13          |Quality Assurance         |Quality Assurance                   |2008-04-30 00:00:00.000|       |
|1           |Engineering               |Research and Development            |2008-04-30 00:00:00.000|       |
|5           |Purchasing                |Inventory Management                |2008-04-30 00:00:00.000|       |
+------------+--------------------------+------------------------------------+-----------------------+-------+

Command took 0.58 seconds -- by usaferreiratiago@gmail.com at 8/24/2022, 1:06:35 PM on Databricks
Cmd 48
1
## Working with When () / Otherwise()##
2
 
3
##df.withColumn('Correct', when(col('GroupName') == "Manufacturing", lit("OK"))).otherwise("NOT")
4
 
5
df.withColumn("Correct", when(col("GroupName").isin("GroupName"),'Correct')
6
             
7
 
8
.otherwise("Ok")).distinct().show(truncate=False)
(2) Spark Jobs
+------------+--------------------------+------------------------------------+-----------------------+-------+
|DepartmentID|Name                      |GroupName                           |ModifiedDate           |Correct|
+------------+--------------------------+------------------------------------+-----------------------+-------+
|6           |Research and Development  |Research and Development            |2008-04-30 00:00:00.000|Ok     |
|7           |Production                |Manufacturing                       |2008-04-30 00:00:00.000|Ok     |
|14          |Facilities and Maintenance|Executive General and Administration|2008-04-30 00:00:00.000|Ok     |
|15          |Shipping and Receiving    |Inventory Management                |2008-04-30 00:00:00.000|Ok     |
|10          |Finance                   |Executive General and Administration|2008-04-30 00:00:00.000|Ok     |
|11          |Information Services      |Executive General and Administration|2008-04-30 00:00:00.000|Ok     |
|16          |Executive                 |Executive General and Administration|2008-04-30 00:00:00.000|Ok     |
|4           |Marketing                 |Sales and Marketing                 |2008-04-30 00:00:00.000|Ok     |
|8           |Production Control        |Manufacturing                       |2008-04-30 00:00:00.000|Ok     |
|13          |Quality Assurance         |Quality Assurance                   |2008-04-30 00:00:00.000|Ok     |
|12          |Document Control          |Quality Assurance                   |2008-04-30 00:00:00.000|Ok     |
|3           |Sales                     |Sales and Marketing                 |2008-04-30 00:00:00.000|Ok     |
|5           |Purchasing                |Inventory Management                |2008-04-30 00:00:00.000|Ok     |
|2           |Tool Design               |Research and Development            |2008-04-30 00:00:00.000|Ok     |
|1           |Engineering               |Research and Development            |2008-04-30 00:00:00.000|Ok     |
|9           |Human Resources           |Executive General and Administration|2008-04-30 00:00:00.000|Ok     |
+------------+--------------------------+------------------------------------+-----------------------+-------+

Command took 0.38 seconds -- by usaferreiratiago@gmail.com at 8/24/2022, 1:06:36 PM on Databricks
Cmd 49
1
## Working with OrderBy desc
2
 
3
df.orderBy(col("GroupName").desc()).show(truncate=False)
(1) Spark Jobs
+------------+--------------------------+------------------------------------+-----------------------+
|DepartmentID|Name                      |GroupName                           |ModifiedDate           |
+------------+--------------------------+------------------------------------+-----------------------+
|3           |Sales                     |Sales and Marketing                 |2008-04-30 00:00:00.000|
|4           |Marketing                 |Sales and Marketing                 |2008-04-30 00:00:00.000|
|2           |Tool Design               |Research and Development            |2008-04-30 00:00:00.000|
|6           |Research and Development  |Research and Development            |2008-04-30 00:00:00.000|
|1           |Engineering               |Research and Development            |2008-04-30 00:00:00.000|
|12          |Document Control          |Quality Assurance                   |2008-04-30 00:00:00.000|
|13          |Quality Assurance         |Quality Assurance                   |2008-04-30 00:00:00.000|
|7           |Production                |Manufacturing                       |2008-04-30 00:00:00.000|
|8           |Production Control        |Manufacturing                       |2008-04-30 00:00:00.000|
|15          |Shipping and Receiving    |Inventory Management                |2008-04-30 00:00:00.000|
|5           |Purchasing                |Inventory Management                |2008-04-30 00:00:00.000|
|9           |Human Resources           |Executive General and Administration|2008-04-30 00:00:00.000|
|16          |Executive                 |Executive General and Administration|2008-04-30 00:00:00.000|
|10          |Finance                   |Executive General and Administration|2008-04-30 00:00:00.000|
|11          |Information Services      |Executive General and Administration|2008-04-30 00:00:00.000|
|14          |Facilities and Maintenance|Executive General and Administration|2008-04-30 00:00:00.000|
+------------+--------------------------+------------------------------------+-----------------------+

Command took 0.25 seconds -- by usaferreiratiago@gmail.com at 8/24/2022, 1:06:39 PM on Databricks
Cmd 50
1
## Working with OrderBy asc
2
 
3
df.orderBy(col("GroupName").asc()).show(truncate=False)
(1) Spark Jobs
+------------+--------------------------+------------------------------------+-----------------------+
|DepartmentID|Name                      |GroupName                           |ModifiedDate           |
+------------+--------------------------+------------------------------------+-----------------------+
|10          |Finance                   |Executive General and Administration|2008-04-30 00:00:00.000|
|16          |Executive                 |Executive General and Administration|2008-04-30 00:00:00.000|
|11          |Information Services      |Executive General and Administration|2008-04-30 00:00:00.000|
|9           |Human Resources           |Executive General and Administration|2008-04-30 00:00:00.000|
|14          |Facilities and Maintenance|Executive General and Administration|2008-04-30 00:00:00.000|
|5           |Purchasing                |Inventory Management                |2008-04-30 00:00:00.000|
|15          |Shipping and Receiving    |Inventory Management                |2008-04-30 00:00:00.000|
|7           |Production                |Manufacturing                       |2008-04-30 00:00:00.000|
|8           |Production Control        |Manufacturing                       |2008-04-30 00:00:00.000|
|13          |Quality Assurance         |Quality Assurance                   |2008-04-30 00:00:00.000|
|12          |Document Control          |Quality Assurance                   |2008-04-30 00:00:00.000|
|1           |Engineering               |Research and Development            |2008-04-30 00:00:00.000|
|2           |Tool Design               |Research and Development            |2008-04-30 00:00:00.000|
|6           |Research and Development  |Research and Development            |2008-04-30 00:00:00.000|
|3           |Sales                     |Sales and Marketing                 |2008-04-30 00:00:00.000|
|4           |Marketing                 |Sales and Marketing                 |2008-04-30 00:00:00.000|
+------------+--------------------------+------------------------------------+-----------------------+

Command took 0.44 seconds -- by usaferreiratiago@gmail.com at 8/24/2022, 1:06:43 PM on Databricks
Cmd 51
1
## Working with OrderBy, Distinct asc
2
 
3
df.orderBy(col("GroupName").asc()).distinct().show(truncate=False)
(2) Spark Jobs
+------------+--------------------------+------------------------------------+-----------------------+
|DepartmentID|Name                      |GroupName                           |ModifiedDate           |
+------------+--------------------------+------------------------------------+-----------------------+
|7           |Production                |Manufacturing                       |2008-04-30 00:00:00.000|
|15          |Shipping and Receiving    |Inventory Management                |2008-04-30 00:00:00.000|
|13          |Quality Assurance         |Quality Assurance                   |2008-04-30 00:00:00.000|
|3           |Sales                     |Sales and Marketing                 |2008-04-30 00:00:00.000|
|9           |Human Resources           |Executive General and Administration|2008-04-30 00:00:00.000|
|2           |Tool Design               |Research and Development            |2008-04-30 00:00:00.000|
|8           |Production Control        |Manufacturing                       |2008-04-30 00:00:00.000|
|10          |Finance                   |Executive General and Administration|2008-04-30 00:00:00.000|
|14          |Facilities and Maintenance|Executive General and Administration|2008-04-30 00:00:00.000|
|16          |Executive                 |Executive General and Administration|2008-04-30 00:00:00.000|
|5           |Purchasing                |Inventory Management                |2008-04-30 00:00:00.000|
|4           |Marketing                 |Sales and Marketing                 |2008-04-30 00:00:00.000|
|6           |Research and Development  |Research and Development            |2008-04-30 00:00:00.000|
|12          |Document Control          |Quality Assurance                   |2008-04-30 00:00:00.000|
|1           |Engineering               |Research and Development            |2008-04-30 00:00:00.000|
|11          |Information Services      |Executive General and Administration|2008-04-30 00:00:00.000|
+------------+--------------------------+------------------------------------+-----------------------+

Command took 0.57 seconds -- by usaferreiratiago@gmail.com at 8/24/2022, 1:06:46 PM on Databricks
Cmd 52
1
## Working with GroupBy, Count, Distinct and asc
2
 
3
df.groupBy("GroupName").count().distinct().show(truncate=False)
(2) Spark Jobs
+------------------------------------+-----+
|GroupName                           |count|
+------------------------------------+-----+
|Executive General and Administration|5    |
|Sales and Marketing                 |2    |
|Research and Development            |3    |
|Quality Assurance                   |2    |
|Manufacturing                       |2    |
|Inventory Management                |2    |
+------------------------------------+-----+

Command took 0.62 seconds -- by usaferreiratiago@gmail.com at 8/24/2022, 1:06:48 PM on Databricks
Cmd 53
3
1
df2 = spark.read.format("csv").option("infer Schema" , True) .option("header", True ) .option("sep",","). load("/FileStore/tables/Bronze/HumanResources_Employee.csv")
2
#display(df)
3
#print(df.count())
4
df2.show(3)
(2) Spark Jobs
df2:pyspark.sql.dataframe.DataFrame = [BusinessEntityID: string, NationalIDNumber: string ... 14 more fields]
+----------------+----------------+--------------------+----------------+-----------------+--------------------+----------+-------------+------+----------+------------+-------------+--------------+-----------+--------------------+--------------------+
|BusinessEntityID|NationalIDNumber|             LoginID|OrganizationNode|OrganizationLevel|            JobTitle| BirthDate|MaritalStatus|Gender|  HireDate|SalariedFlag|VacationHours|SickLeaveHours|CurrentFlag|             rowguid|        ModifiedDate|
+----------------+----------------+--------------------+----------------+-----------------+--------------------+----------+-------------+------+----------+------------+-------------+--------------+-----------+--------------------+--------------------+
|               1|       295847284|adventure-works\ken0|            NULL|             NULL|Chief Executive O...|1969-01-29|            S|     M|2009-01-14|           1|           99|            69|          1|F01251E5-96A3-448...|2014-06-30 00:00:...|
|               2|       245797967|adventure-works\t...|            0x58|                1|Vice President of...|1971-08-01|            S|     F|2008-01-31|           1|            1|            20|          1|45E8F437-670D-440...|2014-06-30 00:00:...|
|               3|       509647174|adventure-works\r...|          0x5AC0|                2| Engineering Manager|1974-11-12|            M|     M|2007-11-11|           1|            2|            21|          1|9BBBFB2C-EFBB-421...|2014-06-30 00:00:...|
+----------------+----------------+--------------------+----------------+-----------------+--------------------+----------+-------------+------+----------+------------+-------------+--------------+-----------+--------------------+--------------------+
only showing top 3 rows

Command took 0.81 seconds -- by usaferreiratiago@gmail.com at 8/24/2022, 1:07:36 PM on Databricks
Cmd 54
1
df2.printSchema()
root
 |-- BusinessEntityID: string (nullable = true)
 |-- NationalIDNumber: string (nullable = true)
 |-- LoginID: string (nullable = true)
 |-- OrganizationNode: string (nullable = true)
 |-- OrganizationLevel: string (nullable = true)
 |-- JobTitle: string (nullable = true)
 |-- BirthDate: string (nullable = true)
 |-- MaritalStatus: string (nullable = true)
 |-- Gender: string (nullable = true)
 |-- HireDate: string (nullable = true)
 |-- SalariedFlag: string (nullable = true)
 |-- VacationHours: string (nullable = true)
 |-- SickLeaveHours: string (nullable = true)
 |-- CurrentFlag: string (nullable = true)
 |-- rowguid: string (nullable = true)
 |-- ModifiedDate: string (nullable = true)

Command took 0.08 seconds -- by usaferreiratiago@gmail.com at 8/24/2022, 1:07:41 PM on Databricks
Cmd 55
1
# Searching for Nulls
2
 
3
for column in df2.columns:
4
    print(column,df2.filter(df2[column].isNull()).count())
(32) Spark Jobs
BusinessEntityID 0
NationalIDNumber 0
LoginID 0
OrganizationNode 0
OrganizationLevel 0
JobTitle 0
BirthDate 0
MaritalStatus 0
Gender 0
HireDate 0
SalariedFlag 0
VacationHours 0
SickLeaveHours 0
CurrentFlag 0
rowguid 0
ModifiedDate 0
Command took 4.09 seconds -- by usaferreiratiago@gmail.com at 8/24/2022, 1:07:44 PM on Databricks
Cmd 56
1
# Check all Columns
2
 
3
df2.columns
Out[291]: ['BusinessEntityID',
 'NationalIDNumber',
 'LoginID',
 'OrganizationNode',
 'OrganizationLevel',
 'JobTitle',
 'BirthDate',
 'MaritalStatus',
 'Gender',
 'HireDate',
 'SalariedFlag',
 'VacationHours',
 'SickLeaveHours',
 'CurrentFlag',
 'rowguid',
 'ModifiedDate']
Command took 0.09 seconds -- by usaferreiratiago@gmail.com at 8/24/2022, 1:07:46 PM on Databricks
Cmd 57
1
# using Split - only to know about it, if see on another code
2
# Always you need to put ## [ df = ]## to Save
3
 
4
df2.select('BusinessEntityID LoginID OrganizationNode OrganizationLevel JobTitle '.split()).show(truncate=False)
(1) Spark Jobs
+----------------+------------------------+----------------+-----------------+---------------------------------+
|BusinessEntityID|LoginID                 |OrganizationNode|OrganizationLevel|JobTitle                         |
+----------------+------------------------+----------------+-----------------+---------------------------------+
|1               |adventure-works\ken0    |NULL            |NULL             |Chief Executive Officer          |
|2               |adventure-works\terri0  |0x58            |1                |Vice President of Engineering    |
|3               |adventure-works\roberto0|0x5AC0          |2                |Engineering Manager              |
|4               |adventure-works\rob0    |0x5AD6          |3                |Senior Tool Designer             |
|5               |adventure-works\gail0   |0x5ADA          |3                |Design Engineer                  |
|6               |adventure-works\jossef0 |0x5ADE          |3                |Design Engineer                  |
|7               |adventure-works\dylan0  |0x5AE1          |3                |Research and Development Manager |
|8               |adventure-works\diane1  |0x5AE158        |4                |Research and Development Engineer|
|9               |adventure-works\gigi0   |0x5AE168        |4                |Research and Development Engineer|
|10              |adventure-works\michael6|0x5AE178        |4                |Research and Development Manager |
|11              |adventure-works\ovidiu0 |0x5AE3          |3                |Senior Tool Designer             |
|12              |adventure-works\thierry0|0x5AE358        |4                |Tool Designer                    |
|13              |adventure-works\janice0 |0x5AE368        |4                |Tool Designer                    |
|14              |adventure-works\michael8|0x5AE5          |3                |Senior Design Engineer           |
|15              |adventure-works\sharon0 |0x5AE7          |3                |Design Engineer                  |
|16              |adventure-works\david0  |0x68            |1                |Marketing Manager                |
|17              |adventure-works\kevin0  |0x6AC0          |2                |Marketing Assistant              |
|18              |adventure-works\john5   |0x6B40          |2                |Marketing Specialist             |
|19              |adventure-works\mary2   |0x6BC0          |2                |Marketing Assistant              |
|20              |adventure-works\wanida0 |0x6C20          |2                |Marketing Assistant              |
+----------------+------------------------+----------------+-----------------+---------------------------------+
only showing top 20 rows

Command took 0.34 seconds -- by usaferreiratiago@gmail.com at 8/24/2022, 1:07:56 PM on Databricks
Cmd 58
1
#empDF.join(deptDF,empDF.emp_dept_id ==  deptDF.dept_id,"inner") \
2
     #.show(truncate=False)
3
 
4
 
5
#(deptDF,empDF.emp_dept_id ==  deptDF.dept_id,"inner")
6
 
7
dfinner = df.join(df2,df.DepartmentID ==  df2.BusinessEntityID,"leftouter")
8
dfinner.show(2)
(1) Spark Jobs
dfinner:pyspark.sql.dataframe.DataFrame = [DepartmentID: string, Name: string ... 18 more fields]
+------------+-----------+--------------------+--------------------+----------------+----------------+--------------------+----------------+-----------------+--------------------+----------+-------------+------+----------+------------+-------------+--------------+-----------+--------------------+--------------------+
|DepartmentID|       Name|           GroupName|        ModifiedDate|BusinessEntityID|NationalIDNumber|             LoginID|OrganizationNode|OrganizationLevel|            JobTitle| BirthDate|MaritalStatus|Gender|  HireDate|SalariedFlag|VacationHours|SickLeaveHours|CurrentFlag|             rowguid|        ModifiedDate|
+------------+-----------+--------------------+--------------------+----------------+----------------+--------------------+----------------+-----------------+--------------------+----------+-------------+------+----------+------------+-------------+--------------+-----------+--------------------+--------------------+
|           1|Engineering|Research and Deve...|2008-04-30 00:00:...|               1|       295847284|adventure-works\ken0|            NULL|             NULL|Chief Executive O...|1969-01-29|            S|     M|2009-01-14|           1|           99|            69|          1|F01251E5-96A3-448...|2014-06-30 00:00:...|
|           2|Tool Design|Research and Deve...|2008-04-30 00:00:...|               2|       245797967|adventure-works\t...|            0x58|                1|Vice President of...|1971-08-01|            S|     F|2008-01-31|           1|            1|            20|          1|45E8F437-670D-440...|2014-06-30 00:00:...|
+------------+-----------+--------------------+--------------------+----------------+----------------+--------------------+----------------+-----------------+--------------------+----------+-------------+------+----------+------------+-------------+--------------+-----------+--------------------+--------------------+
only showing top 2 rows

Command took 0.64 seconds -- by usaferreiratiago@gmail.com at 8/24/2022, 1:07:59 PM on Databricks
Cmd 59
1
#empDF.join(deptDF,empDF.emp_dept_id ==  deptDF.dept_id,"inner") \
2
     #.show(truncate=False)
3
 
4
 
5
#(deptDF,empDF.emp_dept_id ==  deptDF.dept_id,"inner")
6
 
7
dfHR = df.join(df2,df.DepartmentID ==  df2.BusinessEntityID,"leftouter")\
8
.select(col("JobTitle"))
9
dfHR.show(truncate=False)
(1) Spark Jobs
dfHR:pyspark.sql.dataframe.DataFrame = [JobTitle: string]
+---------------------------------+
|JobTitle                         |
+---------------------------------+
|Chief Executive Officer          |
|Vice President of Engineering    |
|Engineering Manager              |
|Senior Tool Designer             |
|Design Engineer                  |
|Design Engineer                  |
|Research and Development Manager |
|Research and Development Engineer|
|Research and Development Engineer|
|Research and Development Manager |
|Senior Tool Designer             |
|Tool Designer                    |
|Tool Designer                    |
|Senior Design Engineer           |
|Design Engineer                  |
|Marketing Manager                |
+---------------------------------+

Command took 0.59 seconds -- by usaferreiratiago@gmail.com at 8/24/2022, 1:08:16 PM on Databricks
Cmd 60
1
### Joins Dataframes ###
2
 
3
 
4
df.join(df2, df.DepartmentID == df2.BusinessEntityID, "inner")\
5
.select(df2.BusinessEntityID.alias("Entity"), \
6
df.DepartmentID.alias('Department'), \
7
df.Name.alias('Name')).show(truncate= False)
(1) Spark Jobs
+------+----------+--------------------------+
|Entity|Department|Name                      |
+------+----------+--------------------------+
|1     |1         |Engineering               |
|2     |2         |Tool Design               |
|3     |3         |Sales                     |
|4     |4         |Marketing                 |
|5     |5         |Purchasing                |
|6     |6         |Research and Development  |
|7     |7         |Production                |
|8     |8         |Production Control        |
|9     |9         |Human Resources           |
|10    |10        |Finance                   |
|11    |11        |Information Services      |
|12    |12        |Document Control          |
|13    |13        |Quality Assurance         |
|14    |14        |Facilities and Maintenance|
|15    |15        |Shipping and Receiving    |
|16    |16        |Executive                 |
+------+----------+--------------------------+

Command took 0.44 seconds -- by usaferreiratiago@gmail.com at 8/24/2022, 1:08:20 PM on Databricks
Cmd 61
1
df2.columns
Out[296]: ['BusinessEntityID',
 'NationalIDNumber',
 'LoginID',
 'OrganizationNode',
 'OrganizationLevel',
 'JobTitle',
 'BirthDate',
 'MaritalStatus',
 'Gender',
 'HireDate',
 'SalariedFlag',
 'VacationHours',
 'SickLeaveHours',
 'CurrentFlag',
 'rowguid',
 'ModifiedDate']
Command took 0.04 seconds -- by usaferreiratiago@gmail.com at 8/24/2022, 1:08:23 PM on Databricks
Cmd 62
1
df2.select(col("LoginID")).show(truncate=False)
(1) Spark Jobs
+------------------------+
|LoginID                 |
+------------------------+
|adventure-works\ken0    |
|adventure-works\terri0  |
|adventure-works\roberto0|
|adventure-works\rob0    |
|adventure-works\gail0   |
|adventure-works\jossef0 |
|adventure-works\dylan0  |
|adventure-works\diane1  |
|adventure-works\gigi0   |
|adventure-works\michael6|
|adventure-works\ovidiu0 |
|adventure-works\thierry0|
|adventure-works\janice0 |
|adventure-works\michael8|
|adventure-works\sharon0 |
|adventure-works\david0  |
|adventure-works\kevin0  |
|adventure-works\john5   |
|adventure-works\mary2   |
|adventure-works\wanida0 |
+------------------------+
only showing top 20 rows

Command took 0.25 seconds -- by usaferreiratiago@gmail.com at 8/24/2022, 1:08:25 PM on Databricks
Cmd 63
truncate=False
1
df3 = spark.read.format("csv").option("infer Schema" , True) .option("header", True ) .option("sep",","). load("/FileStore/tables/Bronze/HumanResources_EmployeeDepartmentHistory.csv")
2
#display(df)
3
#print(df.count())
4
df3.show(truncate=False)
(2) Spark Jobs
df3:pyspark.sql.dataframe.DataFrame = [BusinessEntityID: string, DepartmentID: string ... 4 more fields]
+----------------+------------+-------+----------+----------+-----------------------+
|BusinessEntityID|DepartmentID|ShiftID|StartDate |EndDate   |ModifiedDate           |
+----------------+------------+-------+----------+----------+-----------------------+
|1               |16          |1      |2009-01-14|NULL      |2009-01-13 00:00:00.000|
|2               |1           |1      |2008-01-31|NULL      |2008-01-30 00:00:00.000|
|3               |1           |1      |2007-11-11|NULL      |2007-11-10 00:00:00.000|
|4               |1           |1      |2007-12-05|2010-05-30|2010-05-28 00:00:00.000|
|4               |2           |1      |2010-05-31|NULL      |2010-05-30 00:00:00.000|
|5               |1           |1      |2008-01-06|NULL      |2008-01-05 00:00:00.000|
|6               |1           |1      |2008-01-24|NULL      |2008-01-23 00:00:00.000|
|7               |6           |1      |2009-02-08|NULL      |2009-02-07 00:00:00.000|
|8               |6           |1      |2008-12-29|NULL      |2008-12-28 00:00:00.000|
|9               |6           |1      |2009-01-16|NULL      |2009-01-15 00:00:00.000|
|10              |6           |1      |2009-05-03|NULL      |2009-05-02 00:00:00.000|
|11              |2           |1      |2010-12-05|NULL      |2010-12-04 00:00:00.000|
|12              |2           |1      |2007-12-11|NULL      |2007-12-10 00:00:00.000|
|13              |2           |1      |2010-12-23|NULL      |2010-12-22 00:00:00.000|
|14              |1           |1      |2010-12-30|NULL      |2010-12-29 00:00:00.000|
|15              |1           |1      |2011-01-18|NULL      |2011-01-17 00:00:00.000|
|16              |5           |1      |2007-12-20|2009-07-14|2009-07-12 00:00:00.000|
|16              |4           |1      |2009-07-15|NULL      |2009-07-14 00:00:00.000|
|17              |4           |1      |2007-01-26|NULL      |2007-01-25 00:00:00.000|
|18              |4           |1      |2011-02-07|NULL      |2011-02-06 00:00:00.000|
+----------------+------------+-------+----------+----------+-----------------------+
only showing top 20 rows

Command took 0.79 seconds -- by usaferreiratiago@gmail.com at 8/24/2022, 1:08:50 PM on Databricks
Cmd 64
1
for column in df3.columns:
2
    print(column,df3.filter(df3[column].isNull()).count())
(12) Spark Jobs
BusinessEntityID 0
DepartmentID 0
ShiftID 0
StartDate 0
EndDate 0
ModifiedDate 0
Command took 1.50 seconds -- by usaferreiratiago@gmail.com at 8/24/2022, 1:09:00 PM on Databricks
Cmd 65
1
df3.columns
Out[301]: ['BusinessEntityID',
 'DepartmentID',
 'ShiftID',
 'StartDate',
 'EndDate',
 'ModifiedDate']
Command took 0.09 seconds -- by usaferreiratiago@gmail.com at 8/24/2022, 1:09:01 PM on Databricks
Cmd 66
1
%fs ls '/'
 
path
name
size
modificationTime
1
2
3
4
5
dbfs:/ /
/
0
0
dbfs:/FileStore/
FileStore/
0
0
dbfs:/databricks-datasets/
databricks-datasets/
0
0
dbfs:/databricks-results/
databricks-results/
0
0
dbfs:/user/
user/
0
0
Showing all 5 rows.

 

Command took 1.72 seconds -- by usaferreiratiago@gmail.com at 8/24/2022, 1:09:06 PM on Databricks
Cmd 67
1
 # List the DBFS root
2
    
3
 %fs ls
4
    
5
 # Recursively remove the files under foobar
6
    
7
 %fs rm -r dbfs:/foobar
Cmd 68
1
 %fs ls
 
path
name
size
modificationTime
1
2
3
4
5
dbfs:/ /
/
0
0
dbfs:/FileStore/
FileStore/
0
0
dbfs:/databricks-datasets/
databricks-datasets/
0
0
dbfs:/databricks-results/
databricks-results/
0
0
dbfs:/user/
user/
0
0
Showing all 5 rows.

 

Command took 0.61 seconds -- by usaferreiratiago@gmail.com at 8/24/2022, 1:10:34 PM on Databricks
Cmd 69
1
# using Split - only to know about it, if see on another code
2
# Always you need to put ## [ df = ]## to Save
3
 
4
df2.select('BusinessEntityID LoginID OrganizationNode OrganizationLevel JobTitle '.split()).show(truncate=False)
(1) Spark Jobs
+----------------+------------------------+----------------+-----------------+---------------------------------+
|BusinessEntityID|LoginID                 |OrganizationNode|OrganizationLevel|JobTitle                         |
+----------------+------------------------+----------------+-----------------+---------------------------------+
|1               |adventure-works\ken0    |NULL            |NULL             |Chief Executive Officer          |
|2               |adventure-works\terri0  |0x58            |1                |Vice President of Engineering    |
|3               |adventure-works\roberto0|0x5AC0          |2                |Engineering Manager              |
|4               |adventure-works\rob0    |0x5AD6          |3                |Senior Tool Designer             |
|5               |adventure-works\gail0   |0x5ADA          |3                |Design Engineer                  |
|6               |adventure-works\jossef0 |0x5ADE          |3                |Design Engineer                  |
|7               |adventure-works\dylan0  |0x5AE1          |3                |Research and Development Manager |
|8               |adventure-works\diane1  |0x5AE158        |4                |Research and Development Engineer|
|9               |adventure-works\gigi0   |0x5AE168        |4                |Research and Development Engineer|
|10              |adventure-works\michael6|0x5AE178        |4                |Research and Development Manager |
|11              |adventure-works\ovidiu0 |0x5AE3          |3                |Senior Tool Designer             |
|12              |adventure-works\thierry0|0x5AE358        |4                |Tool Designer                    |
|13              |adventure-works\janice0 |0x5AE368        |4                |Tool Designer                    |
|14              |adventure-works\michael8|0x5AE5          |3                |Senior Design Engineer           |
|15              |adventure-works\sharon0 |0x5AE7          |3                |Design Engineer                  |
|16              |adventure-works\david0  |0x68            |1                |Marketing Manager                |
|17              |adventure-works\kevin0  |0x6AC0          |2                |Marketing Assistant              |
|18              |adventure-works\john5   |0x6B40          |2                |Marketing Specialist             |
|19              |adventure-works\mary2   |0x6BC0          |2                |Marketing Assistant              |
|20              |adventure-works\wanida0 |0x6C20          |2                |Marketing Assistant              |
+----------------+------------------------+----------------+-----------------+---------------------------------+
only showing top 20 rows

Command took 0.27 seconds -- by usaferreiratiago@gmail.com at 8/24/2022, 1:11:07 PM on Databricks
Cmd 70
1
# Create TempView
2
 
3
data_test = df2.createOrReplaceTempView("HumanResources_Employee")
Command took 0.09 seconds -- by usaferreiratiago@gmail.com at 8/24/2022, 1:11:17 PM on Databricks
Cmd 71
1
# SQL inside Pyspark
2
 
3
data_test = spark.sql("""SELECT
4
 
5
 
6
 SUBSTRING(LoginID, 17, 100) AS Login, 
7
 SUBSTRING(HireDate, 1, 10) AS HireDate,
8
 JobTitle,
9
 SUBSTRING(BirthDate, 1, 10) AS BirthDate,
10
 CASE
11
    WHEN MaritalStatus = 'S' THEN 'Single'
12
    WHEN MaritalStatus = 'M' THEN 'Married'
13
    ELSE ''
14
END AS MaritalStatus,
15
CASE
16
    WHEN Gender = 'M' THEN 'Male'
17
    WHEN Gender = 'F' THEN 'Female'
18
    ELSE ''
19
END AS Gender
20
FROM HumanResources_Employee
21
 
22
where JobTitle in ('Senior Tool Designer','Tool Designer') 
23
 
24
 
25
 
26
""").show(truncate=False)
(1) Spark Jobs
+--------+----------+--------------------+----------+-------------+------+
|Login   |HireDate  |JobTitle            |BirthDate |MaritalStatus|Gender|
+--------+----------+--------------------+----------+-------------+------+
|rob0    |2007-12-05|Senior Tool Designer|1974-12-23|Single       |Male  |
|ovidiu0 |2010-12-05|Senior Tool Designer|1978-01-17|Single       |Male  |
|thierry0|2007-12-11|Tool Designer       |1959-07-29|Married      |Male  |
|janice0 |2010-12-23|Tool Designer       |1989-05-28|Married      |Female|
+--------+----------+--------------------+----------+-------------+------+

Command took 0.53 seconds -- by usaferreiratiago@gmail.com at 8/24/2022, 1:53:28 PM on Databricks
Cmd 72
1
# Check List Catalogs
2
​
3
spark.catalog.listCatalogs()
Out[344]: []
Command took 0.27 seconds -- by usaferreiratiago@gmail.com at 8/24/2022, 1:54:58 PM on Databricks
Cmd 73
1
# Check List Databases
2
​
3
spark.catalog.listDatabases()
(2) Spark Jobs
Out[345]: [Database(name='default', catalog='spark_catalog', description='Default Hive database', locationUri='dbfs:/user/hive/warehouse')]
Command took 0.26 seconds -- by usaferreiratiago@gmail.com at 8/24/2022, 1:55:28 PM on Databricks
Cmd 74
1
# Check Files inside of Folder
2
​
3
dbutils.fs.ls("/FileStore/tables/Bronze/")
Out[346]: [FileInfo(path='dbfs:/FileStore/tables/Bronze/HumanResources_Department.csv', name='HumanResources_Department.csv', size=1136, modificationTime=1661338419000),
 FileInfo(path='dbfs:/FileStore/tables/Bronze/HumanResources_Employee.csv', name='HumanResources_Employee.csv', size=49935, modificationTime=1661338419000),
 FileInfo(path='dbfs:/FileStore/tables/Bronze/HumanResources_EmployeeDepartmentHistory.csv', name='HumanResources_EmployeeDepartmentHistory.csv', size=14550, modificationTime=1661338420000),
 FileInfo(path='dbfs:/FileStore/tables/Bronze/HumanResources_EmployeePayHistory.csv', name='HumanResources_EmployeePayHistory.csv', size=19343, modificationTime=1661338420000),
 FileInfo(path='dbfs:/FileStore/tables/Bronze/HumanResources_JobCandidate.csv', name='HumanResources_JobCandidate.csv', size=64287, modificationTime=1661338421000),
 FileInfo(path='dbfs:/FileStore/tables/Bronze/HumanResources_Shift.csv', name='HumanResources_Shift.csv', size=249, modificationTime=1661338421000),
 FileInfo(path='dbfs:/FileStore/tables/Bronze/Person_Address.csv', name='Person_Address.csv', size=3082183, modificationTime=1661338437000),
 FileInfo(path='dbfs:/FileStore/tables/Bronze/Person_AddressType.csv', name='Person_AddressType.csv', size=478, modificationTime=1661338422000),
 FileInfo(path='dbfs:/FileStore/tables/Bronze/Person_BusinessEntity.csv', name='Person_BusinessEntity.csv', size=1401772, modificationTime=1661338430000),
 FileInfo(path='dbfs:/FileStore/tables/Bronze/Person_BusinessEntityAddress.csv', name='Person_BusinessEntityAddress.csv', size=1478939, modificationTime=1661338438000),
 FileInfo(path='dbfs:/FileStore/tables/Bronze/Person_BusinessEntityContact.csv', name='Person_BusinessEntityContact.csv', size=67492, modificationTime=1661338438000),
 FileInfo(path='dbfs:/FileStore/tables/Bronze/Person_ContactType.csv', name='Person_ContactType.csv', size=1002, modificationTime=1661338439000),
 FileInfo(path='dbfs:/FileStore/tables/Bronze/Person_CountryRegion.csv', name='Person_CountryRegion.csv', size=9352, modificationTime=1661338439000),
 FileInfo(path='dbfs:/FileStore/tables/Bronze/Person_EPhoneNumberType.csv', name='Person_EPhoneNumberType.csv', size=136, modificationTime=1661338440000),
 FileInfo(path='dbfs:/FileStore/tables/Bronze/Person_EmailAddress.csv', name='Person_EmailAddress.csv', size=2027800, modificationTime=1661338450000),
 FileInfo(path='dbfs:/FileStore/tables/Bronze/Person_Password.csv', name='Person_Password.csv', size=2426705, modificationTime=1661338453000),
 FileInfo(path='dbfs:/FileStore/tables/Bronze/Person_Person.csv', name='Person_Person.csv', size=13646947, modificationTime=1661338522000),
 FileInfo(path='dbfs:/FileStore/tables/Bronze/Person_PersonPhone.csv', name='Person_PersonPhone.csv', size=973138, modificationTime=1661338459000),
 FileInfo(path='dbfs:/FileStore/tables/Bronze/Person_StateProvince.csv', name='Person_StateProvince.csv', size=16212, modificationTime=1661338460000),
 FileInfo(path='dbfs:/FileStore/tables/Bronze/Production_BillOfMaterials.csv', name='Production_BillOfMaterials.csv', size=217415, modificationTime=1661338463000),
 FileInfo(path='dbfs:/FileStore/tables/Bronze/Production_Culture.csv', name='Production_Culture.csv', size=391, modificationTime=1661338464000)]
Command took 0.25 seconds -- by usaferreiratiago@gmail.com at 8/24/2022, 1:55:36 PM on Databricks
Cmd 75
1
# Check Files inside of Folder
2
​
3
dbutils.fs.ls("/user/hive/warehouse/")
Out[312]: [FileInfo(path='dbfs:/user/hive/warehouse/data_test/', name='data_test/', size=0, modificationTime=0)]
Command took 0.23 seconds -- by usaferreiratiago@gmail.com at 8/24/2022, 1:13:42 PM on Databricks
Cmd 76
1
# Check Files inside of Folder
2
​
3
dbutils.fs.ls("/user/")
Out[313]: [FileInfo(path='dbfs:/user/hive/', name='hive/', size=0, modificationTime=0)]
Command took 0.17 seconds -- by usaferreiratiago@gmail.com at 8/24/2022, 1:13:52 PM on Databricks
Cmd 77
1
data_test = df2.createOrReplaceTempView("HumanResources_Employee")
Command took 0.07 seconds -- by usaferreiratiago@gmail.com at 8/24/2022, 1:41:39 PM on Databricks
Cmd 78
1
df2.write.saveAsTable("HumanResources_Employee")
AnalysisException: Table default.HumanResources_Employee already exists

1
Command took 0.40 seconds -- by usaferreiratiago@gmail.com at 8/24/2022, 1:55:45 PM on Databricks
Cmd 79
1
# Check List Tables
2
​
3
spark.catalog.listTables()
(2) Spark Jobs
Out[349]: [Table(name='humanresources_employee', catalog='spark_catalog', namespace=['default'], description=None, tableType='MANAGED', isTemporary=False),
 Table(name='humanresources_employee', catalog='spark_catalog', namespace=None, description=None, tableType='TEMPORARY', isTemporary=True)]
Command took 0.30 seconds -- by usaferreiratiago@gmail.com at 8/24/2022, 1:58:49 PM on Databricks
Cmd 80
1
#SHOW VIEWS FROM default LIKE 'humanresources_employee'
2
​
3
#SHOW VIEWS LIKE ''
4
​
5
DROP VIEW employeeView
6
​
7
​
Command took 0.08 seconds -- by usaferreiratiago@gmail.com at 8/24/2022, 1:37:10 PM on Databricks
Cmd 81
("humanresources_employee")
1
spark.catalog.listColumns("humanresources_employee")
(8) Spark Jobs
Out[358]: [Column(name='BusinessEntityID', description=None, dataType='string', nullable=True, isPartition=False, isBucket=False),
 Column(name='NationalIDNumber', description=None, dataType='string', nullable=True, isPartition=False, isBucket=False),
 Column(name='LoginID', description=None, dataType='string', nullable=True, isPartition=False, isBucket=False),
 Column(name='OrganizationNode', description=None, dataType='string', nullable=True, isPartition=False, isBucket=False),
 Column(name='OrganizationLevel', description=None, dataType='string', nullable=True, isPartition=False, isBucket=False),
 Column(name='JobTitle', description=None, dataType='string', nullable=True, isPartition=False, isBucket=False),
 Column(name='BirthDate', description=None, dataType='string', nullable=True, isPartition=False, isBucket=False),
 Column(name='MaritalStatus', description=None, dataType='string', nullable=True, isPartition=False, isBucket=False),
 Column(name='Gender', description=None, dataType='string', nullable=True, isPartition=False, isBucket=False),
 Column(name='HireDate', description=None, dataType='string', nullable=True, isPartition=False, isBucket=False),
 Column(name='SalariedFlag', description=None, dataType='string', nullable=True, isPartition=False, isBucket=False),
 Column(name='VacationHours', description=None, dataType='string', nullable=True, isPartition=False, isBucket=False),
 Column(name='SickLeaveHours', description=None, dataType='string', nullable=True, isPartition=False, isBucket=False),
 Column(name='CurrentFlag', description=None, dataType='string', nullable=True, isPartition=False, isBucket=False),
 Column(name='rowguid', description=None, dataType='string', nullable=True, isPartition=False, isBucket=False),
 Column(name='ModifiedDate', description=None, dataType='string', nullable=True, isPartition=False, isBucket=False)]
Command took 0.31 seconds -- by usaferreiratiago@gmail.com at 8/24/2022, 2:24:06 PM on Databricks
Cmd 82
1
spark.catalog.
Out[366]: 'spark_catalog'
Command took 0.07 seconds -- by usaferreiratiago@gmail.com at 8/24/2022, 2:29:34 PM on Databricks
Cmd 83
1
spark.catalog.listFunctions()
(8) Spark Jobs
Out[356]: [Function(name='!', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.Not', isTemporary=True),
 Function(name='%', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.Remainder', isTemporary=True),
 Function(name='&', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.BitwiseAnd', isTemporary=True),
 Function(name='*', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.Multiply', isTemporary=True),
 Function(name='+', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.Add', isTemporary=True),
 Function(name='-', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.Subtract', isTemporary=True),
 Function(name='/', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.Divide', isTemporary=True),
 Function(name='<', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.LessThan', isTemporary=True),
 Function(name='<=', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.LessThanOrEqual', isTemporary=True),
 Function(name='<=>', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.EqualNullSafe', isTemporary=True),
 Function(name='=', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.EqualTo', isTemporary=True),
 Function(name='==', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.EqualTo', isTemporary=True),
 Function(name='>', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.GreaterThan', isTemporary=True),
 Function(name='>=', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.GreaterThanOrEqual', isTemporary=True),
 Function(name='^', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.BitwiseXor', isTemporary=True),
 Function(name='abs', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.Abs', isTemporary=True),
 Function(name='acos', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.Acos', isTemporary=True),
 Function(name='acosh', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.Acosh', isTemporary=True),
 Function(name='add_months', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.AddMonths', isTemporary=True),
 Function(name='aes_decrypt', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.AesDecrypt', isTemporary=True),
 Function(name='aes_encrypt', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.AesEncrypt', isTemporary=True),
 Function(name='aggregate', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.ArrayAggregate', isTemporary=True),
 Function(name='and', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.And', isTemporary=True),
 Function(name='any', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.aggregate.BoolOr', isTemporary=True),
 Function(name='any_value', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.aggregate.AnyValue', isTemporary=True),
 Function(name='approx_count_distinct', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.aggregate.HyperLogLogPlusPlus', isTemporary=True),
 Function(name='approx_percentile', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.aggregate.ApproximatePercentile', isTemporary=True),
 Function(name='approx_top_k', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.aggregate.ApproximateFrequentItems', isTemporary=True),
 Function(name='array', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.CreateArray', isTemporary=True),
 Function(name='array_agg', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.aggregate.CollectList', isTemporary=True),
 Function(name='array_contains', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.ArrayContains', isTemporary=True),
 Function(name='array_distinct', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.ArrayDistinct', isTemporary=True),
 Function(name='array_except', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.ArrayExcept', isTemporary=True),
 Function(name='array_intersect', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.ArrayIntersect', isTemporary=True),
 Function(name='array_join', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.ArrayJoin', isTemporary=True),
 Function(name='array_max', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.ArrayMax', isTemporary=True),
 Function(name='array_min', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.ArrayMin', isTemporary=True),
 Function(name='array_position', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.ArrayPosition', isTemporary=True),
 Function(name='array_remove', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.ArrayRemove', isTemporary=True),
 Function(name='array_repeat', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.ArrayRepeat', isTemporary=True),
 Function(name='array_size', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.ArraySize', isTemporary=True),
 Function(name='array_sort', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.ArraySort', isTemporary=True),
 Function(name='array_union', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.ArrayUnion', isTemporary=True),
 Function(name='arrays_overlap', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.ArraysOverlap', isTemporary=True),
 Function(name='arrays_zip', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.ArraysZip', isTemporary=True),
 Function(name='ascii', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.Ascii', isTemporary=True),
 Function(name='asin', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.Asin', isTemporary=True),
 Function(name='asinh', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.Asinh', isTemporary=True),
 Function(name='assert_true', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.AssertTrue', isTemporary=True),
 Function(name='atan', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.Atan', isTemporary=True),
 Function(name='atan2', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.Atan2', isTemporary=True),
 Function(name='atanh', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.Atanh', isTemporary=True),
 Function(name='avg', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.aggregate.Average', isTemporary=True),
 Function(name='base64', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.Base64', isTemporary=True),
 Function(name='bigint', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.Cast', isTemporary=True),
 Function(name='bin', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.Bin', isTemporary=True),
 Function(name='binary', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.Cast', isTemporary=True),
 Function(name='bit_and', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.aggregate.BitAndAgg', isTemporary=True),
 Function(name='bit_count', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.BitwiseCount', isTemporary=True),
 Function(name='bit_get', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.BitwiseGet', isTemporary=True),
 Function(name='bit_length', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.BitLength', isTemporary=True),
 Function(name='bit_or', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.aggregate.BitOrAgg', isTemporary=True),
 Function(name='bit_reverse', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.BitwiseReverse', isTemporary=True),
 Function(name='bit_xor', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.aggregate.BitXorAgg', isTemporary=True),
 Function(name='bool_and', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.aggregate.BoolAnd', isTemporary=True),
 Function(name='bool_or', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.aggregate.BoolOr', isTemporary=True),
 Function(name='boolean', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.Cast', isTemporary=True),
 Function(name='bround', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.BRound', isTemporary=True),
 Function(name='btrim', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.StringTrimBoth', isTemporary=True),
 Function(name='cardinality', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.Size', isTemporary=True),
 Function(name='cast', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.Cast', isTemporary=True),
 Function(name='cbrt', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.Cbrt', isTemporary=True),
 Function(name='ceil', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.CeilExpressionBuilder', isTemporary=True),
 Function(name='ceiling', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.CeilExpressionBuilder', isTemporary=True),
 Function(name='char', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.Chr', isTemporary=True),
 Function(name='char_length', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.Length', isTemporary=True),
 Function(name='character_length', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.Length', isTemporary=True),
 Function(name='charindex', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.StringLocate', isTemporary=True),
 Function(name='chr', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.Chr', isTemporary=True),
 Function(name='coalesce', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.Coalesce', isTemporary=True),
 Function(name='collect_list', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.aggregate.CollectList', isTemporary=True),
 Function(name='collect_set', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.aggregate.CollectSet', isTemporary=True),
 Function(name='concat', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.Concat', isTemporary=True),
 Function(name='concat_ws', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.ConcatWs', isTemporary=True),
 Function(name='contains', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.ContainsExpressionBuilder', isTemporary=True),
 Function(name='conv', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.Conv', isTemporary=True),
 Function(name='corr', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.aggregate.Corr', isTemporary=True),
 Function(name='cos', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.Cos', isTemporary=True),
 Function(name='cosh', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.Cosh', isTemporary=True),
 Function(name='cot', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.Cot', isTemporary=True),
 Function(name='count', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.aggregate.Count', isTemporary=True),
 Function(name='count_if', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.aggregate.CountIf', isTemporary=True),
 Function(name='count_min_sketch', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.aggregate.CountMinSketchAgg', isTemporary=True),
 Function(name='covar_pop', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.aggregate.CovPopulation', isTemporary=True),
 Function(name='covar_samp', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.aggregate.CovSample', isTemporary=True),
 Function(name='crc32', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.Crc32', isTemporary=True),
 Function(name='csc', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.Csc', isTemporary=True),
 Function(name='cume_dist', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.CumeDist', isTemporary=True),
 Function(name='current_catalog', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.CurrentCatalog', isTemporary=True),
 Function(name='current_database', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.CurrentDatabase', isTemporary=True),
 Function(name='current_date', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.CurrentDate', isTemporary=True),
 Function(name='current_metastore', catalog=None, namespace=None, description=None, className='com.databricks.sql.catalyst.expressions.CurrentMetastore', isTemporary=True),
 Function(name='current_timestamp', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.CurrentTimestamp', isTemporary=True),
 Function(name='current_timezone', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.CurrentTimeZone', isTemporary=True),
 Function(name='current_user', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.CurrentUser', isTemporary=True),
 Function(name='current_version', catalog=None, namespace=None, description=None, className='com.databricks.sql.catalyst.expressions.CurrentVersion', isTemporary=True),
 Function(name='date', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.Cast', isTemporary=True),
 Function(name='date_add', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.DateAdd', isTemporary=True),
 Function(name='date_format', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.DateFormatClass', isTemporary=True),
 Function(name='date_from_unix_date', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.DateFromUnixDate', isTemporary=True),
 Function(name='date_part', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.DatePartExpressionBuilder', isTemporary=True),
 Function(name='date_sub', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.DateSub', isTemporary=True),
 Function(name='date_trunc', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.TruncTimestamp', isTemporary=True),
 Function(name='datediff', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.DateDiff', isTemporary=True),
 Function(name='day', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.DayOfMonth', isTemporary=True),
 Function(name='dayofmonth', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.DayOfMonth', isTemporary=True),
 Function(name='dayofweek', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.DayOfWeek', isTemporary=True),
 Function(name='dayofyear', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.DayOfYear', isTemporary=True),
 Function(name='decimal', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.Cast', isTemporary=True),
 Function(name='decode', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.Decode', isTemporary=True),
 Function(name='degrees', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.ToDegrees', isTemporary=True),
 Function(name='dense_rank', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.DenseRank', isTemporary=True),
 Function(name='div', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.IntegralDivide', isTemporary=True),
 Function(name='double', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.Cast', isTemporary=True),
 Function(name='e', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.EulerNumber', isTemporary=True),
 Function(name='element_at', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.ElementAt', isTemporary=True),
 Function(name='elt', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.Elt', isTemporary=True),
 Function(name='encode', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.Encode', isTemporary=True),
 Function(name='endswith', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.EndsWithExpressionBuilder', isTemporary=True),
 Function(name='equal_null', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.EqualNull', isTemporary=True),
 Function(name='every', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.aggregate.BoolAnd', isTemporary=True),
 Function(name='exists', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.ArrayExists', isTemporary=True),
 Function(name='exp', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.Exp', isTemporary=True),
 Function(name='explode', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.Explode', isTemporary=True),
 Function(name='explode_outer', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.Explode', isTemporary=True),
 Function(name='expm1', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.Expm1', isTemporary=True),
 Function(name='extract', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.Extract', isTemporary=True),
 Function(name='factorial', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.Factorial', isTemporary=True),
 Function(name='filter', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.ArrayFilter', isTemporary=True),
 Function(name='find_in_set', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.FindInSet', isTemporary=True),
 Function(name='first', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.aggregate.First', isTemporary=True),
 Function(name='first_value', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.aggregate.First', isTemporary=True),
 Function(name='flatten', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.Flatten', isTemporary=True),
 Function(name='float', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.Cast', isTemporary=True),
 Function(name='floor', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.FloorExpressionBuilder', isTemporary=True),
 Function(name='forall', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.ArrayForAll', isTemporary=True),
 Function(name='format_number', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.FormatNumber', isTemporary=True),
 Function(name='format_string', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.FormatString', isTemporary=True),
 Function(name='from_csv', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.CsvToStructs', isTemporary=True),
 Function(name='from_json', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.JsonToStructs', isTemporary=True),
 Function(name='from_unixtime', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.FromUnixTime', isTemporary=True),
 Function(name='from_utc_timestamp', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.FromUTCTimestamp', isTemporary=True),
 Function(name='get_json_object', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.GetJsonObject', isTemporary=True),
 Function(name='getbit', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.BitwiseGet', isTemporary=True),
 Function(name='getdate', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.CurrentTimestamp', isTemporary=True),
 Function(name='greatest', catalog=None, namespace=None, description=None, classNa

*** WARNING: max output size exceeded, skipping output. ***

k', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.PercentRank', isTemporary=True),
 Function(name='percentile', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.aggregate.Percentile', isTemporary=True),
 Function(name='percentile_approx', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.aggregate.ApproximatePercentile', isTemporary=True),
 Function(name='pi', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.Pi', isTemporary=True),
 Function(name='pmod', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.Pmod', isTemporary=True),
 Function(name='posexplode', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.PosExplode', isTemporary=True),
 Function(name='posexplode_outer', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.PosExplode', isTemporary=True),
 Function(name='position', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.StringLocate', isTemporary=True),
 Function(name='positive', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.UnaryPositive', isTemporary=True),
 Function(name='pow', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.Pow', isTemporary=True),
 Function(name='power', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.Pow', isTemporary=True),
 Function(name='printf', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.FormatString', isTemporary=True),
 Function(name='quarter', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.Quarter', isTemporary=True),
 Function(name='radians', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.ToRadians', isTemporary=True),
 Function(name='raise_error', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.RaiseError', isTemporary=True),
 Function(name='rand', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.Rand', isTemporary=True),
 Function(name='randn', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.Randn', isTemporary=True),
 Function(name='random', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.Rand', isTemporary=True),
 Function(name='range', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.plans.logical.Range', isTemporary=True),
 Function(name='rank', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.Rank', isTemporary=True),
 Function(name='reduce', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.ArrayAggregate', isTemporary=True),
 Function(name='reflect', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.CallMethodViaReflection', isTemporary=True),
 Function(name='regexp', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.RLike', isTemporary=True),
 Function(name='regexp_extract', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.RegExpExtract', isTemporary=True),
 Function(name='regexp_extract_all', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.RegExpExtractAll', isTemporary=True),
 Function(name='regexp_like', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.RLike', isTemporary=True),
 Function(name='regexp_replace', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.RegExpReplace', isTemporary=True),
 Function(name='regr_avgx', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.aggregate.RegrAvgX', isTemporary=True),
 Function(name='regr_avgy', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.aggregate.RegrAvgY', isTemporary=True),
 Function(name='regr_count', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.aggregate.RegrCount', isTemporary=True),
 Function(name='regr_r2', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.aggregate.RegrR2', isTemporary=True),
 Function(name='regr_slope', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.aggregate.RegrSlope', isTemporary=True),
 Function(name='regr_sxx', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.aggregate.RegrSXX', isTemporary=True),
 Function(name='regr_sxy', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.aggregate.RegrSXY', isTemporary=True),
 Function(name='regr_syy', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.aggregate.RegrSYY', isTemporary=True),
 Function(name='repeat', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.StringRepeat', isTemporary=True),
 Function(name='replace', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.StringReplace', isTemporary=True),
 Function(name='reverse', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.Reverse', isTemporary=True),
 Function(name='right', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.Right', isTemporary=True),
 Function(name='rint', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.Rint', isTemporary=True),
 Function(name='rlike', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.RLike', isTemporary=True),
 Function(name='round', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.Round', isTemporary=True),
 Function(name='row_number', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.RowNumber', isTemporary=True),
 Function(name='rpad', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.RPadExpressionBuilder', isTemporary=True),
 Function(name='rtrim', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.StringTrimRight', isTemporary=True),
 Function(name='schema_of_csv', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.SchemaOfCsv', isTemporary=True),
 Function(name='schema_of_json', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.SchemaOfJson', isTemporary=True),
 Function(name='sec', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.Sec', isTemporary=True),
 Function(name='second', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.Second', isTemporary=True),
 Function(name='sentences', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.Sentences', isTemporary=True),
 Function(name='sequence', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.Sequence', isTemporary=True),
 Function(name='session_window', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.SessionWindow', isTemporary=True),
 Function(name='sha', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.Sha1', isTemporary=True),
 Function(name='sha1', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.Sha1', isTemporary=True),
 Function(name='sha2', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.Sha2', isTemporary=True),
 Function(name='shiftleft', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.ShiftLeft', isTemporary=True),
 Function(name='shiftright', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.ShiftRight', isTemporary=True),
 Function(name='shiftrightunsigned', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.ShiftRightUnsigned', isTemporary=True),
 Function(name='shuffle', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.Shuffle', isTemporary=True),
 Function(name='sign', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.Signum', isTemporary=True),
 Function(name='signum', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.Signum', isTemporary=True),
 Function(name='sin', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.Sin', isTemporary=True),
 Function(name='sinh', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.Sinh', isTemporary=True),
 Function(name='size', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.Size', isTemporary=True),
 Function(name='skewness', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.aggregate.Skewness', isTemporary=True),
 Function(name='slice', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.Slice', isTemporary=True),
 Function(name='smallint', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.Cast', isTemporary=True),
 Function(name='some', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.aggregate.BoolOr', isTemporary=True),
 Function(name='sort_array', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.SortArray', isTemporary=True),
 Function(name='soundex', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.SoundEx', isTemporary=True),
 Function(name='space', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.StringSpace', isTemporary=True),
 Function(name='spark_partition_id', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.SparkPartitionID', isTemporary=True),
 Function(name='split', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.StringSplit', isTemporary=True),
 Function(name='split_part', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.SplitPart', isTemporary=True),
 Function(name='sqrt', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.Sqrt', isTemporary=True),
 Function(name='stack', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.Stack', isTemporary=True),
 Function(name='startswith', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.StartsWithExpressionBuilder', isTemporary=True),
 Function(name='std', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.aggregate.StddevSamp', isTemporary=True),
 Function(name='stddev', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.aggregate.StddevSamp', isTemporary=True),
 Function(name='stddev_pop', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.aggregate.StddevPop', isTemporary=True),
 Function(name='stddev_samp', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.aggregate.StddevSamp', isTemporary=True),
 Function(name='str_to_map', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.StringToMap', isTemporary=True),
 Function(name='string', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.Cast', isTemporary=True),
 Function(name='stringdecode', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.StringDecode', isTemporary=True),
 Function(name='struct', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.CreateNamedStruct', isTemporary=True),
 Function(name='substr', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.Substring', isTemporary=True),
 Function(name='substring', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.Substring', isTemporary=True),
 Function(name='substring_index', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.SubstringIndex', isTemporary=True),
 Function(name='sum', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.aggregate.Sum', isTemporary=True),
 Function(name='tan', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.Tan', isTemporary=True),
 Function(name='tanh', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.Tanh', isTemporary=True),
 Function(name='timestamp', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.Cast', isTemporary=True),
 Function(name='timestamp_micros', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.MicrosToTimestamp', isTemporary=True),
 Function(name='timestamp_millis', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.MillisToTimestamp', isTemporary=True),
 Function(name='timestamp_seconds', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.SecondsToTimestamp', isTemporary=True),
 Function(name='tinyint', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.Cast', isTemporary=True),
 Function(name='to_binary', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.ToBinary', isTemporary=True),
 Function(name='to_char', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.ToCharacter', isTemporary=True),
 Function(name='to_csv', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.StructsToCsv', isTemporary=True),
 Function(name='to_date', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.ParseToDate', isTemporary=True),
 Function(name='to_json', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.StructsToJson', isTemporary=True),
 Function(name='to_number', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.ToNumber', isTemporary=True),
 Function(name='to_timestamp', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.ParseToTimestamp', isTemporary=True),
 Function(name='to_unix_timestamp', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.ToUnixTimestamp', isTemporary=True),
 Function(name='to_utc_timestamp', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.ToUTCTimestamp', isTemporary=True),
 Function(name='transform', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.ArrayTransform', isTemporary=True),
 Function(name='transform_keys', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.TransformKeys', isTemporary=True),
 Function(name='transform_values', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.TransformValues', isTemporary=True),
 Function(name='translate', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.StringTranslate', isTemporary=True),
 Function(name='trim', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.StringTrim', isTemporary=True),
 Function(name='trunc', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.TruncDate', isTemporary=True),
 Function(name='try_add', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.TryAdd', isTemporary=True),
 Function(name='try_avg', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.aggregate.TryAverage', isTemporary=True),
 Function(name='try_divide', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.TryDivide', isTemporary=True),
 Function(name='try_element_at', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.TryElementAt', isTemporary=True),
 Function(name='try_multiply', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.TryMultiply', isTemporary=True),
 Function(name='try_subtract', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.TrySubtract', isTemporary=True),
 Function(name='try_sum', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.aggregate.TrySum', isTemporary=True),
 Function(name='try_to_binary', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.TryToBinary', isTemporary=True),
 Function(name='try_to_number', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.TryToNumber', isTemporary=True),
 Function(name='try_to_timestamp', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.TryToTimestampExpressionBuilder', isTemporary=True),
 Function(name='typeof', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.TypeOf', isTemporary=True),
 Function(name='ucase', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.Upper', isTemporary=True),
 Function(name='unbase64', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.UnBase64', isTemporary=True),
 Function(name='unhex', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.Unhex', isTemporary=True),
 Function(name='unix_date', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.UnixDate', isTemporary=True),
 Function(name='unix_micros', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.UnixMicros', isTemporary=True),
 Function(name='unix_millis', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.UnixMillis', isTemporary=True),
 Function(name='unix_seconds', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.UnixSeconds', isTemporary=True),
 Function(name='unix_timestamp', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.UnixTimestamp', isTemporary=True),
 Function(name='upper', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.Upper', isTemporary=True),
 Function(name='uuid', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.Uuid', isTemporary=True),
 Function(name='var_pop', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.aggregate.VariancePop', isTemporary=True),
 Function(name='var_samp', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.aggregate.VarianceSamp', isTemporary=True),
 Function(name='variance', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.aggregate.VarianceSamp', isTemporary=True),
 Function(name='version', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.SparkVersion', isTemporary=True),
 Function(name='weekday', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.WeekDay', isTemporary=True),
 Function(name='weekofyear', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.WeekOfYear', isTemporary=True),
 Function(name='when', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.CaseWhen', isTemporary=True),
 Function(name='width_bucket', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.WidthBucket', isTemporary=True),
 Function(name='window', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.TimeWindow', isTemporary=True),
 Function(name='xpath', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.xml.XPathList', isTemporary=True),
 Function(name='xpath_boolean', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.xml.XPathBoolean', isTemporary=True),
 Function(name='xpath_double', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.xml.XPathDouble', isTemporary=True),
 Function(name='xpath_float', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.xml.XPathFloat', isTemporary=True),
 Function(name='xpath_int', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.xml.XPathInt', isTemporary=True),
 Function(name='xpath_long', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.xml.XPathLong', isTemporary=True),
 Function(name='xpath_number', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.xml.XPathDouble', isTemporary=True),
 Function(name='xpath_short', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.xml.XPathShort', isTemporary=True),
 Function(name='xpath_string', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.xml.XPathString', isTemporary=True),
 Function(name='xxhash64', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.XxHash64', isTemporary=True),
 Function(name='year', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.Year', isTemporary=True),
 Function(name='zip_with', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.ZipWith', isTemporary=True),
 Function(name='|', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.BitwiseOr', isTemporary=True),
 Function(name='~', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.BitwiseNot', isTemporary=True)]
Command took 0.82 seconds -- by usaferreiratiago@gmail.com at 8/24/2022, 2:21:46 PM on Databricks
Cmd 84
1
# Delete Files
2
 
3
%fs rm -r dbfs:/FileStore/tables/input/Put here File to delete.csv
4
 
5
# create directory in dbfs
6
 
7
dbutils.fs.mkdirs(" /path/directoryname")
8
 
9
# create file and write data to it
10
 
11
dbutils.fs.put(" /path/filename.txt", "content")
12
 
13
# display file content
14
 
15
dbutils.fs.head("/path/filename.txt")
16
 
17
# list down content in a directory
18
 
19
dbutils.fs.ls("/path/")
20
 
21
# move files from one directory to another directory
22
 
23
dbutils.fs.mv("path1","path2")
24
 
25
# copy file from one directory to another directory
26
 
27
dbutils.fs.cp("path1", "path2")
28
 
29
# remove file and directories
30
 
31
dbutils.fs.rm("path1/file.txt")
32
dbutils.fs.rm("path1/", True)
33
 
34
# mount and unmount file system
35
 
36
dbutils.fs.mount("mountpoint")
37
dbutils.fs.unmount("mountpoint")
38
 
39
# list down mount
40
 
41
dbutils.fs.mounts()
42
 
43
# refresh mount points
44
 
45
dbutils.fs.refreshMounts()
46
 
47
# install the packages
48
 
49
dbutils.library.installPyPI("tensorflow")
50
 
51
# find current notebook path/from UI
52
 
53
dbutils.notebook.getContext.notebookPath
54
 
55
# run one notebook from another notebook
56
 
57
%run path $name="rama" $location="bangalore"
58
 
59
dbutils.notebook.run("path",600,{"name":"rama","location":"bangalore"})
60
 
61
#exit notebook execution
62
 
63
dbutils.notebook.exit("exit message")
64
 
65
# list down secret scopes Go to Settings to
66
 
67
dbutils.secrets.listScopes()
68
 
69
 
70
 
71
 
72
 
UsageError: Line magic function `%fs` not found.
UsageError: Line magic function `%fs` not found.
Command took 0.13 seconds -- by usaferreiratiago@gmail.com at 8/24/2022, 2:16:32 PM on Databricks
Cmd 85
1
dbutils.fs.ls("/FileStore/tables/Bronze")
Out[205]: [FileInfo(path='dbfs:/FileStore/tables/Bronze/HumanResources_Employee.csv', name='HumanResources_Employee.csv', size=49935, modificationTime=1661337098000)]
Command took 0.17 seconds -- by usaferreiratiago@gmail.com at 8/24/2022, 12:38:49 PM on Databricks
Cmd 86
1
#dbfs:/FileStore/tables/input
2
 
3
 
4
 
5
dbutils.fs.cp("dbfs:/FileStore/tables/input/HumanResources_Employee.csv", "dbfs:/FileStore/tables/Bronze/HumanResources_Employee.csv")
6
 
7
 
8
 
Out[195]: True
Command took 0.68 seconds -- by usaferreiratiago@gmail.com at 8/24/2022, 12:31:37 PM on Databricks
Cmd 87
1
%fs
2
 
3
ls /FileStore/tables/Bronze
 
path
name
size
modificationTime
1
2
3
4
5
6
7
dbfs:/FileStore/tables/Bronze/HumanResources_Department.csv
HumanResources_Department.csv
1136
1661338419000
dbfs:/FileStore/tables/Bronze/HumanResources_Employee.csv
HumanResources_Employee.csv
49935
1661338419000
dbfs:/FileStore/tables/Bronze/HumanResources_EmployeeDepartmentHistory.csv
HumanResources_EmployeeDepartmentHistory.csv
14550
1661338420000
dbfs:/FileStore/tables/Bronze/HumanResources_EmployeePayHistory.csv
HumanResources_EmployeePayHistory.csv
19343
1661338420000
dbfs:/FileStore/tables/Bronze/HumanResources_JobCandidate.csv
HumanResources_JobCandidate.csv
64287
1661338421000
dbfs:/FileStore/tables/Bronze/HumanResources_Shift.csv
HumanResources_Shift.csv
249
1661338421000
dbfs:/FileStore/tables/Bronze/Person_Address.csv
Person_Address.csv
3082183
1661338437000
Showing all 21 rows.

 

Command took 1.52 seconds -- by usaferreiratiago@gmail.com at 8/24/2022, 3:47:26 PM on Databricks
Cmd 88
1
df = spark.read.format("csv").option("infer Schema" , True).option("header", True ).option("sep",","). load("/FileStore/tables/Bronze/HumanResources_Employee.csv")
2
#display(df)
3
#print(df.count())
4
df.show(1)
(2) Spark Jobs
df:pyspark.sql.dataframe.DataFrame = [BusinessEntityID: string, NationalIDNumber: string ... 14 more fields]
+----------------+----------------+--------------------+----------------+-----------------+--------------------+----------+-------------+------+----------+------------+-------------+--------------+-----------+--------------------+--------------------+
|BusinessEntityID|NationalIDNumber|             LoginID|OrganizationNode|OrganizationLevel|            JobTitle| BirthDate|MaritalStatus|Gender|  HireDate|SalariedFlag|VacationHours|SickLeaveHours|CurrentFlag|             rowguid|        ModifiedDate|
+----------------+----------------+--------------------+----------------+-----------------+--------------------+----------+-------------+------+----------+------------+-------------+--------------+-----------+--------------------+--------------------+
|               1|       295847284|adventure-works\ken0|            NULL|             NULL|Chief Executive O...|1969-01-29|            S|     M|2009-01-14|           1|           99|            69|          1|F01251E5-96A3-448...|2014-06-30 00:00:...|
+----------------+----------------+--------------------+----------------+-----------------+--------------------+----------+-------------+------+----------+------------+-------------+--------------+-----------+--------------------+--------------------+
only showing top 1 row

Command took 0.86 seconds -- by usaferreiratiago@gmail.com at 8/24/2022, 3:48:48 PM on Databricks
Cmd 89
1
dbutils.fs.ls("/FileStore/tables/Bronze")
Out[368]: [FileInfo(path='dbfs:/FileStore/tables/Bronze/HumanResources_Department.csv', name='HumanResources_Department.csv', size=1136, modificationTime=1661338419000),
 FileInfo(path='dbfs:/FileStore/tables/Bronze/HumanResources_Employee.csv', name='HumanResources_Employee.csv', size=49935, modificationTime=1661338419000),
 FileInfo(path='dbfs:/FileStore/tables/Bronze/HumanResources_EmployeeDepartmentHistory.csv', name='HumanResources_EmployeeDepartmentHistory.csv', size=14550, modificationTime=1661338420000),
 FileInfo(path='dbfs:/FileStore/tables/Bronze/HumanResources_EmployeePayHistory.csv', name='HumanResources_EmployeePayHistory.csv', size=19343, modificationTime=1661338420000),
 FileInfo(path='dbfs:/FileStore/tables/Bronze/HumanResources_JobCandidate.csv', name='HumanResources_JobCandidate.csv', size=64287, modificationTime=1661338421000),
 FileInfo(path='dbfs:/FileStore/tables/Bronze/HumanResources_Shift.csv', name='HumanResources_Shift.csv', size=249, modificationTime=1661338421000),
 FileInfo(path='dbfs:/FileStore/tables/Bronze/Person_Address.csv', name='Person_Address.csv', size=3082183, modificationTime=1661338437000),
 FileInfo(path='dbfs:/FileStore/tables/Bronze/Person_AddressType.csv', name='Person_AddressType.csv', size=478, modificationTime=1661338422000),
 FileInfo(path='dbfs:/FileStore/tables/Bronze/Person_BusinessEntity.csv', name='Person_BusinessEntity.csv', size=1401772, modificationTime=1661338430000),
 FileInfo(path='dbfs:/FileStore/tables/Bronze/Person_BusinessEntityAddress.csv', name='Person_BusinessEntityAddress.csv', size=1478939, modificationTime=1661338438000),
 FileInfo(path='dbfs:/FileStore/tables/Bronze/Person_BusinessEntityContact.csv', name='Person_BusinessEntityContact.csv', size=67492, modificationTime=1661338438000),
 FileInfo(path='dbfs:/FileStore/tables/Bronze/Person_ContactType.csv', name='Person_ContactType.csv', size=1002, modificationTime=1661338439000),
 FileInfo(path='dbfs:/FileStore/tables/Bronze/Person_CountryRegion.csv', name='Person_CountryRegion.csv', size=9352, modificationTime=1661338439000),
 FileInfo(path='dbfs:/FileStore/tables/Bronze/Person_EPhoneNumberType.csv', name='Person_EPhoneNumberType.csv', size=136, modificationTime=1661338440000),
 FileInfo(path='dbfs:/FileStore/tables/Bronze/Person_EmailAddress.csv', name='Person_EmailAddress.csv', size=2027800, modificationTime=1661338450000),
 FileInfo(path='dbfs:/FileStore/tables/Bronze/Person_Password.csv', name='Person_Password.csv', size=2426705, modificationTime=1661338453000),
 FileInfo(path='dbfs:/FileStore/tables/Bronze/Person_Person.csv', name='Person_Person.csv', size=13646947, modificationTime=1661338522000),
 FileInfo(path='dbfs:/FileStore/tables/Bronze/Person_PersonPhone.csv', name='Person_PersonPhone.csv', size=973138, modificationTime=1661338459000),
 FileInfo(path='dbfs:/FileStore/tables/Bronze/Person_StateProvince.csv', name='Person_StateProvince.csv', size=16212, modificationTime=1661338460000),
 FileInfo(path='dbfs:/FileStore/tables/Bronze/Production_BillOfMaterials.csv', name='Production_BillOfMaterials.csv', size=217415, modificationTime=1661338463000),
 FileInfo(path='dbfs:/FileStore/tables/Bronze/Production_Culture.csv', name='Production_Culture.csv', size=391, modificationTime=1661338464000)]
Command took 0.24 seconds -- by usaferreiratiago@gmail.com at 8/24/2022, 3:14:03 PM on Databricks
Cmd 90
1
%sql
2
​
3
show tables
 
database
tableName
isTemporary
1
2
default
humanresources_employee
false
humanresources_employee
true
Showing all 2 rows.

 

Command took 0.07 seconds -- by usaferreiratiago@gmail.com at 8/24/2022, 3:46:25 PM on Databricks
Cmd 91
1
%sql
2
​
3
show views
 
namespace
viewName
isTemporary
1
humanresources_employee
true
Showing all 1 rows.

 

Command took 0.03 seconds -- by usaferreiratiago@gmail.com at 8/24/2022, 3:46:35 PM on Databricks
Cmd 92
humanresources_employee
1
%sql
2
​
3
DESCRIBE EXTENDED humanresources_employee
 
col_name
data_type
comment
1
2
3
4
5
6
7
BusinessEntityID
string
null
NationalIDNumber
string
null
LoginID
string
null
OrganizationNode
string
null
OrganizationLevel
string
null
JobTitle
string
null
BirthDate
string
null
Showing all 16 rows.

 

Command took 0.36 seconds -- by usaferreiratiago@gmail.com at 8/24/2022, 3:18:49 PM on Databricks
Cmd 93
1
%sql
2
​
3
show views in global_temp
 
namespace
viewName
isTemporary
1
2
global_temp
csv
true
humanresources_employee
true
Showing all 2 rows.

 

Command took 0.04 seconds -- by usaferreiratiago@gmail.com at 8/24/2022, 3:46:56 PM on Databricks
Cmd 94
1
%sql
2
​
3
select * from default.humanresources_employee
(1) Spark Jobs
 
BusinessEntityID
NationalIDNumber
LoginID
OrganizationNode
OrganizationLevel
JobTitle
BirthDate
MaritalStatus
Gender
1
2
3
4
5
6
7
8
9
10
11
12
13
14
15
16
17
1
295847284
adventure-works\ken0
NULL
NULL
Chief Executive Officer
1969-01-29
S
M
2
245797967
adventure-works\terri0
0x58
1
Vice President of Engineering
1971-08-01
S
F
3
509647174
adventure-works\roberto0
0x5AC0
2
Engineering Manager
1974-11-12
M
M
4
112457891
adventure-works\rob0
0x5AD6
3
Senior Tool Designer
1974-12-23
S
M
5
695256908
adventure-works\gail0
0x5ADA
3
Design Engineer
1952-09-27
M
F
6
998320692
adventure-works\jossef0
0x5ADE
3
Design Engineer
1959-03-11
M
M
7
134969118
adventure-works\dylan0
0x5AE1
3
Research and Development Manager
1987-02-24
M
M
8
811994146
adventure-works\diane1
0x5AE158
4
Research and Development Engineer
1986-06-05
S
F
9
658797903
adventure-works\gigi0
0x5AE168
4
Research and Development Engineer
1979-01-21
M
F
10
879342154
adventure-works\michael6
0x5AE178
4
Research and Development Manager
1984-11-30
M
M
11
974026903
adventure-works\ovidiu0
0x5AE3
3
Senior Tool Designer
1978-01-17
S
M
12
480168528
adventure-works\thierry0
0x5AE358
4
Tool Designer
1959-07-29
M
M
13
486228782
adventure-works\janice0
0x5AE368
4
Tool Designer
1989-05-28
M
F
14
42487730
adventure-works\michael8
0x5AE5
3
Senior Design Engineer
1979-06-16
S
M
15
56920285
adventure-works\sharon0
0x5AE7
3
Design Engineer
1961-05-02
M
F
16
24756624
adventure-works\david0
0x68
1
Marketing Manager
1975-03-19
S
M
17
253022876
adventure-works\kevin0
0x6AC0
2
Marketing Assistant
1987-05-03
S
M
Showing all 290 rows.

 

Command took 0.58 seconds -- by usaferreiratiago@gmail.com at 8/24/2022, 3:47:15 PM on Databricks
Cmd 95
1
#View Constructs a virtual table that has no physical data
2
#CreateOrReplace TempView:It is session based. It is saved in default database
3
#CreateOrReplaceGlobalTempView:It is not session based. It is saved in global_temp database
Command took 0.09 seconds -- by usaferreiratiago@gmail.com at 8/24/2022, 3:49:07 PM on Databricks
Cmd 96
1
df.createOrReplaceGlobalTempView('HumanResources_Employee.csv')
Command took 0.10 seconds -- by usaferreiratiago@gmail.com at 8/24/2022, 3:45:49 PM on Databricks
