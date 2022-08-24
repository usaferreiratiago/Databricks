databricks-logoProject_1(Python)


 Import Notebook
 
# Import Libraries 
 
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import numpy as np
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

#import findspark as fd
dbutils.fs.ls("/FileStore/tables/input/")
 
Out[5]: [FileInfo(path='dbfs:/FileStore/tables/input/HumanResources_Department.csv', name='HumanResources_Department.csv', size=1136, modificationTime=1660999948000),
 FileInfo(path='dbfs:/FileStore/tables/input/HumanResources_Employee.csv', name='HumanResources_Employee.csv', size=49935, modificationTime=1660999874000),
 FileInfo(path='dbfs:/FileStore/tables/input/HumanResources_EmployeeDepartmentHistory.csv', name='HumanResources_EmployeeDepartmentHistory.csv', size=14550, modificationTime=1660999874000),
 FileInfo(path='dbfs:/FileStore/tables/input/HumanResources_EmployeePayHistory.csv', name='HumanResources_EmployeePayHistory.csv', size=19343, modificationTime=1660999875000),
 FileInfo(path='dbfs:/FileStore/tables/input/HumanResources_JobCandidate.csv', name='HumanResources_JobCandidate.csv', size=64287, modificationTime=1660999875000),
 FileInfo(path='dbfs:/FileStore/tables/input/HumanResources_Shift.csv', name='HumanResources_Shift.csv', size=249, modificationTime=1660999876000),
 FileInfo(path='dbfs:/FileStore/tables/input/Person_Address.csv', name='Person_Address.csv', size=3082183, modificationTime=1660999893000),
 FileInfo(path='dbfs:/FileStore/tables/input/Person_AddressType.csv', name='Person_AddressType.csv', size=478, modificationTime=1660999877000),
 FileInfo(path='dbfs:/FileStore/tables/input/Person_BusinessEntity.csv', name='Person_BusinessEntity.csv', size=1401772, modificationTime=1660999885000),
 FileInfo(path='dbfs:/FileStore/tables/input/Person_BusinessEntityAddress.csv', name='Person_BusinessEntityAddress.csv', size=1478939, modificationTime=1660999893000),
 FileInfo(path='dbfs:/FileStore/tables/input/Person_BusinessEntityContact.csv', name='Person_BusinessEntityContact.csv', size=67492, modificationTime=1660999894000),
 FileInfo(path='dbfs:/FileStore/tables/input/Person_ContactType.csv', name='Person_ContactType.csv', size=1002, modificationTime=1660999894000),
 FileInfo(path='dbfs:/FileStore/tables/input/Person_CountryRegion.csv', name='Person_CountryRegion.csv', size=9352, modificationTime=1660999894000),
 FileInfo(path='dbfs:/FileStore/tables/input/Person_EPhoneNumberType.csv', name='Person_EPhoneNumberType.csv', size=136, modificationTime=1660999895000),
 FileInfo(path='dbfs:/FileStore/tables/input/Person_EmailAddress.csv', name='Person_EmailAddress.csv', size=2027800, modificationTime=1660999978000),
 FileInfo(path='dbfs:/FileStore/tables/input/Person_Password.csv', name='Person_Password.csv', size=2426705, modificationTime=1660999982000),
 FileInfo(path='dbfs:/FileStore/tables/input/Person_Person.csv', name='Person_Person.csv', size=13646947, modificationTime=1661000048000),
 FileInfo(path='dbfs:/FileStore/tables/input/Person_PersonPhone.csv', name='Person_PersonPhone.csv', size=973138, modificationTime=1660999911000),
 FileInfo(path='dbfs:/FileStore/tables/input/Person_StateProvince.csv', name='Person_StateProvince.csv', size=16212, modificationTime=1660999912000),
 FileInfo(path='dbfs:/FileStore/tables/input/Production_BillOfMaterials.csv', name='Production_BillOfMaterials.csv', size=217415, modificationTime=1660999913000),
 FileInfo(path='dbfs:/FileStore/tables/input/Production_Culture.csv', name='Production_Culture.csv', size=391, modificationTime=1660999991000)]
# Create SparkSession
 
spark = (
    SparkSession.builder
    .master('local')
    .appName('Project_01')
    .getOrCreate()
)
df = spark.read.format("csv").option("infer Schema" , True) .option("header", True ) .option("sep",","). load("/FileStore/tables/input/HumanResources_Department.csv")
#display(df)
#print(df.count())
df.show()
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

# Check Schema
df.printSchema()
root
 |-- DepartmentID: string (nullable = true)
 |-- Name: string (nullable = true)
 |-- GroupName: string (nullable = true)
 |-- ModifiedDate: string (nullable = true)

# Checking Datas null -- Pandas has limitations #Don't use - Only try
 
df.toPandas().isna().sum()
Out[13]: DepartmentID    0
Name            0
GroupName       0
ModifiedDate    0
dtype: int64
# Searching for Nulls
 
for column in df.columns:
    print(column,df.filter(df[column].isNull()).count())
DepartmentID 0
Name 0
GroupName 0
ModifiedDate 0
# Rename Columns
# Always you need to put ## [ df = ]## to Save
 
df = df.withColumnRenamed('Modified Date','ModifiedDate')
df.show(truncate = False)
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

# Check all Columns
 
df.columns
Out[16]: ['DepartmentID', 'Name', 'GroupName', 'ModifiedDate']
# Select Columns
 
df.select(col('GroupName'),col('Name')).show(truncate = False)
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

# Create Alias
# Always you need to put ## [ df = ]## to Save
 
df.select(col('Name').alias('Names')).show(truncate = False)
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

# using Split - only to know about it, if see on another code
# Always you need to put ## [ df = ]## to Save
 
df.select('DepartmentID Name GroupName ModifiedDate'.split()).show(truncate = False)
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

# Showing Columns as you want to see 
 
df.select('Name','GroupName').show(truncate = False)
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

# Filtring df  --Showing only the specific column and specific filtern and putting distinct function to not duplicate information
 
df.select(col('GroupName')).filter(col('GroupName') == "Inventory Management").distinct().show(truncate = False)
+--------------------+
|GroupName           |
+--------------------+
|Inventory Management|
+--------------------+

#Showing all df
 
df.show(truncate = False)
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

# Filtring df with more conditions and specific columns (AND / &)
 
df.select('Name','ModifiedDate').filter((col('Name') == "Finance")).show(truncate = False)
+-------+-----------------------+
|Name   |ModifiedDate           |
+-------+-----------------------+
|Finance|2008-04-30 00:00:00.000|
+-------+-----------------------+

# Filtring df with specific columns and more conditions  (AND / &)
 
df.select('DepartmentID','Name').filter((col('Name') == "Finance") & (col('DepartmentID') == 10)).show(truncate = False) 
+------------+-------+
|DepartmentID|Name   |
+------------+-------+
|10          |Finance|
+------------+-------+

# Filtring df with specific columns and more conditions  (AND / &)
 
df.select('DepartmentId','Name').filter((col('DepartmentID') == 2)).show(truncate = False)
+------------+-----------+
|DepartmentId|Name       |
+------------+-----------+
|2           |Tool Design|
+------------+-----------+

# Filtring df with specific columns and more conditions  (AND / &)
 
df.select('DepartmentId','Name').filter((col('DepartmentID') != 12)).show(truncate = False)
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

# Filtring df with specific columns and more conditions  (AND / &)
## df.filter('Name = "Finance"').filter(col('DepartmentId') == 1).show()
 
df.select('DepartmentId','Name').filter((col('DepartmentID') >= 3)).show(truncate = False)
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

# Filtring df with specific columns and more conditions  (AND / &)
 
df.select('DepartmentId','Name').filter((col('DepartmentID') <= 16)).show(truncate = False)
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

# Filtring df with more conditions (OR / |)
 
df.filter('DepartmentID = "1"').show(truncate = False)
+------------+-----------+------------------------+-----------------------+
|DepartmentID|Name       |GroupName               |ModifiedDate           |
+------------+-----------+------------------------+-----------------------+
|1           |Engineering|Research and Development|2008-04-30 00:00:00.000|
+------------+-----------+------------------------+-----------------------+

# Filtring df with more conditions (OR / |)
 
df.filter((col('Name') == 'Finance') | (col('Name') == 'Sales') | (col('DepartmentID') == 12)).show(truncate = False)
+------------+----------------+------------------------------------+-----------------------+
|DepartmentID|Name            |GroupName                           |ModifiedDate           |
+------------+----------------+------------------------------------+-----------------------+
|3           |Sales           |Sales and Marketing                 |2008-04-30 00:00:00.000|
|10          |Finance         |Executive General and Administration|2008-04-30 00:00:00.000|
|12          |Document Control|Quality Assurance                   |2008-04-30 00:00:00.000|
+------------+----------------+------------------------------------+-----------------------+

# # Filtring df with more conditions (OR / |)
 
df.filter(col('GroupName') == 'Quality Assurance').show(truncate = False)
+------------+-----------------+-----------------+-----------------------+
|DepartmentID|Name             |GroupName        |ModifiedDate           |
+------------+-----------------+-----------------+-----------------------+
|12          |Document Control |Quality Assurance|2008-04-30 00:00:00.000|
|13          |Quality Assurance|Quality Assurance|2008-04-30 00:00:00.000|
+------------+-----------------+-----------------+-----------------------+

# # Filtring df combining & and | # And e OR #
 
df.filter((col('GroupName') == "Quality Assurance")  | (col('Name') == "Sales") | (col('DepartmentID') == 10)).show(truncate = False)
+------------+-----------------+------------------------------------+-----------------------+
|DepartmentID|Name             |GroupName                           |ModifiedDate           |
+------------+-----------------+------------------------------------+-----------------------+
|3           |Sales            |Sales and Marketing                 |2008-04-30 00:00:00.000|
|10          |Finance          |Executive General and Administration|2008-04-30 00:00:00.000|
|12          |Document Control |Quality Assurance                   |2008-04-30 00:00:00.000|
|13          |Quality Assurance|Quality Assurance                   |2008-04-30 00:00:00.000|
+------------+-----------------+------------------------------------+-----------------------+

# Concatenate Columns without space
 
df.withColumn("DepartmentID + GroupName", concat('DepartmentID','GroupName')).show()
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

# Concatenate Columns with space
 
df.withColumn("DepartmentID + GroupName", concat_ws(' ', 'DepartmentID','GroupName')).show()
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

# Alter type of Column
 
df.show(truncate = False)
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

# Alter type of Column
 
df.printSchema()
root
 |-- DepartmentID: string (nullable = true)
 |-- Name: string (nullable = true)
 |-- GroupName: string (nullable = true)
 |-- ModifiedDate: string (nullable = true)

# Alter type of Metada of the Column
 
#df.withColumn('ModifiedDate', col('ModifiedDate').cast(IntegerType())).show(truncate=False)
 
df.show()
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

# Alter type of Column
# Didn't change yet because we didn't put variable "" df = df.withColumn('ModifiedDate', col('ModifiedDate').cast(IntegerType())).show(truncate=False)"  bfore the execution of code,
# only when we put this everything will change
 
df.printSchema()
root
 |-- DepartmentID: string (nullable = true)
 |-- Name: string (nullable = true)
 |-- GroupName: string (nullable = true)
 |-- ModifiedDate: string (nullable = true)

#The below example returns the difference between two dates using datediff().
 
df.select(col('ModifiedDate'),
         datediff(current_timestamp(), col('ModifiedDate')).alias('difference between two dates')).show(truncate=False)
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

#The below example returns the months between two dates 
 
df.select(col("ModifiedDate"), 
    months_between(current_timestamp(),col("ModifiedDate")).alias("months_between")  
  ).show()
 
#round(col("score")
+--------------------+--------------+
|        ModifiedDate|months_between|
+--------------------+--------------+
|2008-04-30 00:00:...|  171.81781623|
|2008-04-30 00:00:...|  171.81781623|
|2008-04-30 00:00:...|  171.81781623|
|2008-04-30 00:00:...|  171.81781623|
|2008-04-30 00:00:...|  171.81781623|
|2008-04-30 00:00:...|  171.81781623|
|2008-04-30 00:00:...|  171.81781623|
|2008-04-30 00:00:...|  171.81781623|
|2008-04-30 00:00:...|  171.81781623|
|2008-04-30 00:00:...|  171.81781623|
|2008-04-30 00:00:...|  171.81781623|
|2008-04-30 00:00:...|  171.81781623|
|2008-04-30 00:00:...|  171.81781623|
|2008-04-30 00:00:...|  171.81781623|
|2008-04-30 00:00:...|  171.81781623|
|2008-04-30 00:00:...|  171.81781623|
+--------------------+--------------+

#Using round numbers ('Arredontar numeros')
 
df.select("*",round(col("DepartmentID")).alias("Teste")).show(truncate=False)
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

## test modifying dates##
 
#(df_date
#.withColumn("to_date", f.to_date("input_date"))
 
df.withColumn("year",year("ModifiedDate")).show(2)
df.withColumn("quarter", quarter("ModifiedDate")).show(2)
df.withColumn("month",month("ModifiedDate")).show(2)
df.withColumn("week",weekofyear("ModifiedDate")).show(2)
df.withColumn("dayofyear",dayofyear("ModifiedDate")).show(2)
df.withColumn("dayofmonth ",dayofmonth("ModifiedDate")).show(2)
df.withColumn("dayofweek" , dayofweek("ModifiedDate")).show(2)
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

# Extract Hour, Minutes and Seconds
 
 
#(df_date
#.withColumn("to_timestamp",f.to_timestamp("input_date"))
df.withColumn("hour", hour("ModifiedDate")).show(2)
df.withColumn("minute",minute("ModifiedDate")).show(2)
df.withColumn("second",second("ModifiedDate")).show(2)
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

#Days and Month in Words
 
df.withColumn("dayofweek" ,dayofweek("ModifiedDate")).show(2)
df.withColumn("dayinwords",date_format("ModifiedDate" , "EEEE")).show(2)
##df.withColumn("monthinwords", date_format("ModifiedDate" , "LLLL")).show(2)
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

#Hadling Dates
 
df.withColumn("cur_date",current_date()).show(2)
df.withColumn("Days",datediff(current_date(),"ModifiedDate" )).show(2) 
df.withColumn("dateadd" ,date_add("ModifiedDate",5)).show(2) 
df.withColumn("datesub" ,date_sub("ModifiedDate",5)).show(2) 
df.withColumn("datetrnc",date_trunc('mm' , "ModifiedDate")).show(2) 
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

#Using Distinct #
 
df.select(col('GroupName')).distinct().show(truncate=False)
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

# Using Collect - show all the rows#
 
df.select(col('GroupName')).distinct().collect()
Out[47]: [Row(GroupName='Executive General and Administration'),
 Row(GroupName='Sales and Marketing'),
 Row(GroupName='Research and Development'),
 Row(GroupName='Quality Assurance'),
 Row(GroupName='Manufacturing'),
 Row(GroupName='Inventory Management')]
list = df.select(col('GroupName')).collect()
type(list[0][0])
Out[49]: str
list[5][0]
Out[50]: 'Research and Development'
list[0][0]
Out[51]: 'Research and Development'
# Generating a list GroupName = []
 
for GroupName in list:
    GroupName.asDict(GroupName[0])
GroupName
Out[52]: Row(GroupName='Executive General and Administration')
## Working with When () / Otherwise()##
 
##df.withColumn('Correct', when(col('GroupName') == "Manufacturing", lit("OK"))).otherwise("NOT")
 
df.withColumn("Correct", when(col("GroupName") == "Manufacturing" , lit("OK")).otherwise("")).distinct().show(truncate=False)
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

## Working with When () / Otherwise()##
 
##df.withColumn('Correct', when(col('GroupName') == "Manufacturing", lit("OK"))).otherwise("NOT")
 
df.withColumn("Correct", when(col("GroupName").isin("GroupName"),'Correct')
             
 
.otherwise("Ok")).distinct().show(truncate=False)
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

## Working with OrderBy desc
 
df.orderBy(col("GroupName").desc()).show(truncate=False)
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

## Working with OrderBy asc
 
df.orderBy(col("GroupName").asc()).show(truncate=False)
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

## Working with OrderBy, Distinct asc
 
df.orderBy(col("GroupName").asc()).distinct().show(truncate=False)
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

## Working with GroupBy, Count, Distinct and asc
 
df.groupBy("GroupName").count().distinct().show(truncate=False)
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

df2 = spark.read.format("csv").option("infer Schema" , True) .option("header", True ) .option("sep",","). load("/FileStore/tables/input/HumanResources_Employee.csv")
#display(df)
#print(df.count())
df2.show(truncate=False)
+----------------+----------------+------------------------+----------------+-----------------+---------------------------------+----------+-------------+------+----------+------------+-------------+--------------+-----------+------------------------------------+-----------------------+
|BusinessEntityID|NationalIDNumber|LoginID                 |OrganizationNode|OrganizationLevel|JobTitle                         |BirthDate |MaritalStatus|Gender|HireDate  |SalariedFlag|VacationHours|SickLeaveHours|CurrentFlag|rowguid                             |ModifiedDate           |
+----------------+----------------+------------------------+----------------+-----------------+---------------------------------+----------+-------------+------+----------+------------+-------------+--------------+-----------+------------------------------------+-----------------------+
|1               |295847284       |adventure-works\ken0    |NULL            |NULL             |Chief Executive Officer          |1969-01-29|S            |M     |2009-01-14|1           |99           |69            |1          |F01251E5-96A3-448D-981E-0F99D789110D|2014-06-30 00:00:00.000|
|2               |245797967       |adventure-works\terri0  |0x58            |1                |Vice President of Engineering    |1971-08-01|S            |F     |2008-01-31|1           |1            |20            |1          |45E8F437-670D-4409-93CB-F9424A40D6EE|2014-06-30 00:00:00.000|
|3               |509647174       |adventure-works\roberto0|0x5AC0          |2                |Engineering Manager              |1974-11-12|M            |M     |2007-11-11|1           |2            |21            |1          |9BBBFB2C-EFBB-4217-9AB7-F97689328841|2014-06-30 00:00:00.000|
|4               |112457891       |adventure-works\rob0    |0x5AD6          |3                |Senior Tool Designer             |1974-12-23|S            |M     |2007-12-05|0           |48           |80            |1          |59747955-87B8-443F-8ED4-F8AD3AFDF3A9|2014-06-30 00:00:00.000|
|5               |695256908       |adventure-works\gail0   |0x5ADA          |3                |Design Engineer                  |1952-09-27|M            |F     |2008-01-06|1           |5            |22            |1          |EC84AE09-F9B8-4A15-B4A9-6CCBAB919B08|2014-06-30 00:00:00.000|
|6               |998320692       |adventure-works\jossef0 |0x5ADE          |3                |Design Engineer                  |1959-03-11|M            |M     |2008-01-24|1           |6            |23            |1          |E39056F1-9CD5-478D-8945-14ACA7FBDCDD|2014-06-30 00:00:00.000|
|7               |134969118       |adventure-works\dylan0  |0x5AE1          |3                |Research and Development Manager |1987-02-24|M            |M     |2009-02-08|1           |61           |50            |1          |4F46DECA-EF01-41FD-9829-0ADAB368E431|2014-06-30 00:00:00.000|
|8               |811994146       |adventure-works\diane1  |0x5AE158        |4                |Research and Development Engineer|1986-06-05|S            |F     |2008-12-29|1           |62           |51            |1          |31112635-663B-4018-B4A2-A685C0BF48A4|2014-06-30 00:00:00.000|
|9               |658797903       |adventure-works\gigi0   |0x5AE168        |4                |Research and Development Engineer|1979-01-21|M            |F     |2009-01-16|1           |63           |51            |1          |50B6CDC6-7570-47EF-9570-48A64B5F2ECF|2014-06-30 00:00:00.000|
|10              |879342154       |adventure-works\michael6|0x5AE178        |4                |Research and Development Manager |1984-11-30|M            |M     |2009-05-03|1           |16           |64            |1          |EAA43680-5571-40CB-AB1A-3BF68F04459E|2014-06-30 00:00:00.000|
|11              |974026903       |adventure-works\ovidiu0 |0x5AE3          |3                |Senior Tool Designer             |1978-01-17|S            |M     |2010-12-05|0           |7            |23            |1          |F68C7C19-FAC1-438C-9BB7-AC33FCC341C3|2014-06-30 00:00:00.000|
|12              |480168528       |adventure-works\thierry0|0x5AE358        |4                |Tool Designer                    |1959-07-29|M            |M     |2007-12-11|0           |9            |24            |1          |1D955171-E773-4FAD-8382-40FD898D5D4D|2014-06-30 00:00:00.000|
|13              |486228782       |adventure-works\janice0 |0x5AE368        |4                |Tool Designer                    |1989-05-28|M            |F     |2010-12-23|0           |8            |24            |1          |954B91B6-5AA7-48C2-8685-6E11C6E5C49A|2014-06-30 00:00:00.000|
|14              |42487730        |adventure-works\michael8|0x5AE5          |3                |Senior Design Engineer           |1979-06-16|S            |M     |2010-12-30|1           |3            |21            |1          |46286CA4-46DD-4DDB-9128-85B67E98D1A9|2014-06-30 00:00:00.000|
|15              |56920285        |adventure-works\sharon0 |0x5AE7          |3                |Design Engineer                  |1961-05-02|M            |F     |2011-01-18|1           |4            |22            |1          |54F2FDC0-87C4-4065-A7A8-9AC8EA624235|2014-06-30 00:00:00.000|
|16              |24756624        |adventure-works\david0  |0x68            |1                |Marketing Manager                |1975-03-19|S            |M     |2007-12-20|1           |40           |40            |1          |E87029AA-2CBA-4C03-B948-D83AF0313E28|2014-06-30 00:00:00.000|
|17              |253022876       |adventure-works\kevin0  |0x6AC0          |2                |Marketing Assistant              |1987-05-03|S            |M     |2007-01-26|0           |42           |41            |1          |1B480240-95C0-410F-A717-EB29943C8886|2014-06-30 00:00:00.000|
|18              |222969461       |adventure-works\john5   |0x6B40          |2                |Marketing Specialist             |1978-03-06|S            |M     |2011-02-07|0           |48           |44            |1          |64730415-1F58-4E5B-8FA8-5E4DAEBA53B4|2014-06-30 00:00:00.000|
|19              |52541318        |adventure-works\mary2   |0x6BC0          |2                |Marketing Assistant              |1978-01-29|S            |F     |2011-02-14|0           |43           |41            |1          |1F6DA901-C7F7-48A8-8EEF-D81868D72B52|2014-06-30 00:00:00.000|
|20              |323403273       |adventure-works\wanida0 |0x6C20          |2                |Marketing Assistant              |1975-03-17|M            |F     |2011-01-07|0           |41           |40            |1          |43CCA446-DA1C-454C-8530-873AD2923E1B|2014-06-30 00:00:00.000|
+----------------+----------------+------------------------+----------------+-----------------+---------------------------------+----------+-------------+------+----------+------------+-------------+--------------+-----------+------------------------------------+-----------------------+
only showing top 20 rows

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

# Searching for Nulls
 
for column in df2.columns:
    print(column,df2.filter(df2[column].isNull()).count())
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
# Check all Columns
 
df2.columns
Out[62]: ['BusinessEntityID',
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
# using Split - only to know about it, if see on another code
# Always you need to put ## [ df = ]## to Save
 
df2.select('BusinessEntityID LoginID OrganizationNode OrganizationLevel JobTitle '.split()).show(truncate=False)
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

#empDF.join(deptDF,empDF.emp_dept_id ==  deptDF.dept_id,"inner") \
     #.show(truncate=False)
 
 
#(deptDF,empDF.emp_dept_id ==  deptDF.dept_id,"inner")
 
dfinner = df.join(df2,df.DepartmentID ==  df2.BusinessEntityID,"leftouter")
dfinner.show(2)
+------------+-----------+--------------------+--------------------+----------------+----------------+--------------------+----------------+-----------------+--------------------+----------+-------------+------+----------+------------+-------------+--------------+-----------+--------------------+--------------------+
|DepartmentID|       Name|           GroupName|        ModifiedDate|BusinessEntityID|NationalIDNumber|             LoginID|OrganizationNode|OrganizationLevel|            JobTitle| BirthDate|MaritalStatus|Gender|  HireDate|SalariedFlag|VacationHours|SickLeaveHours|CurrentFlag|             rowguid|        ModifiedDate|
+------------+-----------+--------------------+--------------------+----------------+----------------+--------------------+----------------+-----------------+--------------------+----------+-------------+------+----------+------------+-------------+--------------+-----------+--------------------+--------------------+
|           1|Engineering|Research and Deve...|2008-04-30 00:00:...|               1|       295847284|adventure-works\ken0|            NULL|             NULL|Chief Executive O...|1969-01-29|            S|     M|2009-01-14|           1|           99|            69|          1|F01251E5-96A3-448...|2014-06-30 00:00:...|
|           2|Tool Design|Research and Deve...|2008-04-30 00:00:...|               2|       245797967|adventure-works\t...|            0x58|                1|Vice President of...|1971-08-01|            S|     F|2008-01-31|           1|            1|            20|          1|45E8F437-670D-440...|2014-06-30 00:00:...|
+------------+-----------+--------------------+--------------------+----------------+----------------+--------------------+----------------+-----------------+--------------------+----------+-------------+------+----------+------------+-------------+--------------+-----------+--------------------+--------------------+
only showing top 2 rows

#empDF.join(deptDF,empDF.emp_dept_id ==  deptDF.dept_id,"inner") \
     #.show(truncate=False)
 
 
#(deptDF,empDF.emp_dept_id ==  deptDF.dept_id,"inner")
 
dfHR = df.join(df2,df.DepartmentID ==  df2.BusinessEntityID,"leftouter")\
.select(col("JobTitle"))
dfHR.show(truncate=False)
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

### Joins Dataframes ###
 
 
df.join(df2, df.DepartmentID == df2.BusinessEntityID, "inner")\
.select(df2.BusinessEntityID.alias("Entity"), \
df.DepartmentID.alias('Department'), \
df.Name.alias('Name')).show(truncate= False)
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

df2.columns
Out[70]: ['BusinessEntityID',
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
df2.select(col("LoginID")).show(truncate=False)
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

df3 = spark.read.format("csv").option("infer Schema" , True) .option("header", True ) .option("sep",","). load("/FileStore/tables/input/HumanResources_EmployeeDepartmentHistory.csv")
#display(df)
#print(df.count())
df3.show()
+----------------+------------+-------+----------+----------+--------------------+
|BusinessEntityID|DepartmentID|ShiftID| StartDate|   EndDate|        ModifiedDate|
+----------------+------------+-------+----------+----------+--------------------+
|               1|          16|      1|2009-01-14|      NULL|2009-01-13 00:00:...|
|               2|           1|      1|2008-01-31|      NULL|2008-01-30 00:00:...|
|               3|           1|      1|2007-11-11|      NULL|2007-11-10 00:00:...|
|               4|           1|      1|2007-12-05|2010-05-30|2010-05-28 00:00:...|
|               4|           2|      1|2010-05-31|      NULL|2010-05-30 00:00:...|
|               5|           1|      1|2008-01-06|      NULL|2008-01-05 00:00:...|
|               6|           1|      1|2008-01-24|      NULL|2008-01-23 00:00:...|
|               7|           6|      1|2009-02-08|      NULL|2009-02-07 00:00:...|
|               8|           6|      1|2008-12-29|      NULL|2008-12-28 00:00:...|
|               9|           6|      1|2009-01-16|      NULL|2009-01-15 00:00:...|
|              10|           6|      1|2009-05-03|      NULL|2009-05-02 00:00:...|
|              11|           2|      1|2010-12-05|      NULL|2010-12-04 00:00:...|
|              12|           2|      1|2007-12-11|      NULL|2007-12-10 00:00:...|
|              13|           2|      1|2010-12-23|      NULL|2010-12-22 00:00:...|
|              14|           1|      1|2010-12-30|      NULL|2010-12-29 00:00:...|
|              15|           1|      1|2011-01-18|      NULL|2011-01-17 00:00:...|
|              16|           5|      1|2007-12-20|2009-07-14|2009-07-12 00:00:...|
|              16|           4|      1|2009-07-15|      NULL|2009-07-14 00:00:...|
|              17|           4|      1|2007-01-26|      NULL|2007-01-25 00:00:...|
|              18|           4|      1|2011-02-07|      NULL|2011-02-06 00:00:...|
+----------------+------------+-------+----------+----------+--------------------+
only showing top 20 rows

for column in df3.columns:
    print(column,df3.filter(df3[column].isNull()).count())
BusinessEntityID 0
DepartmentID 0
ShiftID 0
StartDate 0
EndDate 0
ModifiedDate 0
df3.columns
Out[74]: ['BusinessEntityID',
 'DepartmentID',
 'ShiftID',
 'StartDate',
 'EndDate',
 'ModifiedDate']
%fs ls '/'
 
path
name
size
modificationTime
1
2
3
4
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
Showing all 4 rows.

 # List the DBFS root
    
 %fs ls
    
 # Recursively remove the files under foobar
    
 %fs rm -r dbfs:/foobar
 %fs ls
 
path
name
size
modificationTime
1
2
3
4
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
Showing all 4 rows.

# To Delete Files
 
%fs rm -r dbfs:/FileStore/tables/input/Put here File to delete.csv
UsageError: Line magic function `%fs` not found.
UsageError: Line magic function `%fs` not found.
# using Split - only to know about it, if see on another code
# Always you need to put ## [ df = ]## to Save
 
df2.select('BusinessEntityID LoginID OrganizationNode OrganizationLevel JobTitle '.split()).show(truncate=False)
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

# Create TempView
 
data_test = df2.createOrReplaceTempView("HumanResources_Employee")
# SQL inside Pyspark
 
data_test = spark.sql("""SELECT
 
 
 SUBSTRING(LoginID, 17, 100) AS Login, 
 SUBSTRING(HireDate, 1, 10) AS HireDate,
 JobTitle,
 SUBSTRING(BirthDate, 1, 10) AS BirthDate,
 CASE
    WHEN MaritalStatus = 'S' THEN 'Single'
    WHEN MaritalStatus = 'M' THEN 'Married'
    ELSE ''
END AS MaritalStatus,
CASE
    WHEN Gender = 'M' THEN 'Male'
    WHEN Gender = 'F' THEN 'Female'
    ELSE ''
END AS Gender
FROM HumanResources_Employee
 
where JobTitle in ('Senior Tool Designer','Tool Designer') 
 
 
 
""").show(truncate=False)
+--------+----------+--------------------+----------+-------------+------+
|Login   |HireDate  |JobTitle            |BirthDate |MaritalStatus|Gender|
+--------+----------+--------------------+----------+-------------+------+
|rob0    |2007-12-05|Senior Tool Designer|1974-12-23|Single       |Male  |
|ovidiu0 |2010-12-05|Senior Tool Designer|1978-01-17|Single       |Male  |
|thierry0|2007-12-11|Tool Designer       |1959-07-29|Married      |Male  |
|janice0 |2010-12-23|Tool Designer       |1989-05-28|Married      |Female|
+--------+----------+--------------------+----------+-------------+------+

spark.catalog.listCatalogs()
Out[135]: []
spark.catalog.listDatabases()
Out[136]: [Database(name='default', catalog='spark_catalog', description='Default Hive database', locationUri='dbfs:/user/hive/warehouse')]
dbutils.fs.ls("/FileStore/tables/input/")
Out[137]: [FileInfo(path='dbfs:/FileStore/tables/input/HumanResources_Department.csv', name='HumanResources_Department.csv', size=1136, modificationTime=1660999948000),
 FileInfo(path='dbfs:/FileStore/tables/input/HumanResources_Employee.csv', name='HumanResources_Employee.csv', size=49935, modificationTime=1660999874000),
 FileInfo(path='dbfs:/FileStore/tables/input/HumanResources_EmployeeDepartmentHistory.csv', name='HumanResources_EmployeeDepartmentHistory.csv', size=14550, modificationTime=1660999874000),
 FileInfo(path='dbfs:/FileStore/tables/input/HumanResources_EmployeePayHistory.csv', name='HumanResources_EmployeePayHistory.csv', size=19343, modificationTime=1660999875000),
 FileInfo(path='dbfs:/FileStore/tables/input/HumanResources_JobCandidate.csv', name='HumanResources_JobCandidate.csv', size=64287, modificationTime=1660999875000),
 FileInfo(path='dbfs:/FileStore/tables/input/HumanResources_Shift.csv', name='HumanResources_Shift.csv', size=249, modificationTime=1660999876000),
 FileInfo(path='dbfs:/FileStore/tables/input/Person_Address.csv', name='Person_Address.csv', size=3082183, modificationTime=1660999893000),
 FileInfo(path='dbfs:/FileStore/tables/input/Person_AddressType.csv', name='Person_AddressType.csv', size=478, modificationTime=1660999877000),
 FileInfo(path='dbfs:/FileStore/tables/input/Person_BusinessEntity.csv', name='Person_BusinessEntity.csv', size=1401772, modificationTime=1660999885000),
 FileInfo(path='dbfs:/FileStore/tables/input/Person_BusinessEntityAddress.csv', name='Person_BusinessEntityAddress.csv', size=1478939, modificationTime=1660999893000),
 FileInfo(path='dbfs:/FileStore/tables/input/Person_BusinessEntityContact.csv', name='Person_BusinessEntityContact.csv', size=67492, modificationTime=1660999894000),
 FileInfo(path='dbfs:/FileStore/tables/input/Person_ContactType.csv', name='Person_ContactType.csv', size=1002, modificationTime=1660999894000),
 FileInfo(path='dbfs:/FileStore/tables/input/Person_CountryRegion.csv', name='Person_CountryRegion.csv', size=9352, modificationTime=1660999894000),
 FileInfo(path='dbfs:/FileStore/tables/input/Person_EPhoneNumberType.csv', name='Person_EPhoneNumberType.csv', size=136, modificationTime=1660999895000),
 FileInfo(path='dbfs:/FileStore/tables/input/Person_EmailAddress.csv', name='Person_EmailAddress.csv', size=2027800, modificationTime=1660999978000),
 FileInfo(path='dbfs:/FileStore/tables/input/Person_Password.csv', name='Person_Password.csv', size=2426705, modificationTime=1660999982000),
 FileInfo(path='dbfs:/FileStore/tables/input/Person_Person.csv', name='Person_Person.csv', size=13646947, modificationTime=1661000048000),
 FileInfo(path='dbfs:/FileStore/tables/input/Person_PersonPhone.csv', name='Person_PersonPhone.csv', size=973138, modificationTime=1660999911000),
 FileInfo(path='dbfs:/FileStore/tables/input/Person_StateProvince.csv', name='Person_StateProvince.csv', size=16212, modificationTime=1660999912000),
 FileInfo(path='dbfs:/FileStore/tables/input/Production_BillOfMaterials.csv', name='Production_BillOfMaterials.csv', size=217415, modificationTime=1660999913000),
 FileInfo(path='dbfs:/FileStore/tables/input/Production_Culture.csv', name='Production_Culture.csv', size=391, modificationTime=1660999991000)]
dbutils.fs.ls("/user/hive/warehouse/")
Out[138]: [FileInfo(path='dbfs:/user/hive/warehouse/data_test/', name='data_test/', size=0, modificationTime=0)]
dbutils.fs.ls("/user/")
Out[139]: [FileInfo(path='dbfs:/user/hive/', name='hive/', size=0, modificationTime=0)]
data_test = df2.createOrReplaceTempView("HumanResources_Employee")
df2.write.saveAsTable("data_test")
AnalysisException: Table default.data_test already exists
spark.catalog.listTables()
Out[141]: [Table(name='data_test', catalog='spark_catalog', namespace=['default'], description=None, tableType='MANAGED', isTemporary=False),
 Table(name='humanresources_employee', catalog='spark_catalog', namespace=None, description=None, tableType='TEMPORARY', isTemporary=True)]
spark.catalog.listColumns("data_test")
Out[142]: [Column(name='BusinessEntityID', description=None, dataType='string', nullable=True, isPartition=False, isBucket=False),
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
spark.catalog.listFunctions()
Out[143]: [Function(name='!', catalog=None, namespace=None, description=None, className='org.apache.spark.sql.catalyst.expressions.Not', isTemporary=True),
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
dbutils.fs.mkdirs("/FileStore/tables/Bronze/")
Out[146]: True
# To Delete Files
 
%fs rm -r dbfs:/FileStore/tables/input/Put here File to delete.csv
 
# create directory in dbfs
 
dbutils.fs.mkdirs(" /path/directoryname")
 
# create file and write data to it
 
dbutils.fs.put(" /path/filename.txt", "content")
 
# display file content
 
dbutils.fs.head("/path/filename.txt")
 
# list down content in a directory
 
dbutils.fs.ls("/path/")
 
# move files from one directory to another directory
 
dbutils.fs.mv("path1","path2")
 
# copy file from one directory to another directory
 
dbutils.fs.cp("path1", "path2")
 
# remove file and directories
 
dbutils.fs.rm("path1/file.txt")
dbutils.fs.rm("path1/", True)
 
# mount and unmount file system
 
dbutils.fs.mount("mountpoint")
dbutils.fs.unmount("mountpoint")
 
# list down mount
 
dbutils.fs.mounts()
 
# refresh mount points
 
dbutils.fs.refreshMounts()
 
# install the packages
 
dbutils.library.installPyPI("tensorflow")
 
# find current notebook path/from UI
 
dbutils.notebook.getContext.notebookPath
 
# run one notebook from another notebook
 
%run path $name="rama" $location="bangalore"
 
dbutils.notebook.run("path",600,{"name":"rama","location":"bangalore"})
 
#exit notebook execution
 
dbutils.notebook.exit("exit message")
 
# list down secret scopes Go to Settings to
 
dbutils.secrets.listScopes()
 
 
 
 
 
UsageError: Line magic function `%fs` not found.
UsageError: Line magic function `%fs` not found.
