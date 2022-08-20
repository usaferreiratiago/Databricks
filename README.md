# Databricks
Databricks - Pyspark


# Take Datasets

display(dbutils.fs.ls("/databricks-datasets"))

file = "dbfs:/databricks-datasets/flights/"

# Create Pyspark Session



databricks-logoProject_1(Python)


# Import Libraries 
 
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
dbutils.fs.ls("/FileStore/tables/input/")
 
Out[132]: [FileInfo(path='dbfs:/FileStore/tables/input/HumanResources_Department.csv', name='HumanResources_Department.csv', size=1136, modificationTime=1660999948000),
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

 
root
 |-- DepartmentID: string (nullable = true)
 |-- Name: string (nullable = true)
 |-- GroupName: string (nullable = true)
 |-- ModifiedDate: string (nullable = true)

# Checking Datas null -- Pandas has limitations #Don't use - Only try
 
df.toPandas().isna().sum()
Out[64]: DepartmentID    0
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
Out[67]: ['DepartmentID', 'Name', 'GroupName', 'ModifiedDate']
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
|2008-04-30 00:00:00.000|5225                        |
|2008-04-30 00:00:00.000|5225                        |
|2008-04-30 00:00:00.000|5225                        |
|2008-04-30 00:00:00.000|5225                        |
|2008-04-30 00:00:00.000|5225                        |
|2008-04-30 00:00:00.000|5225                        |
|2008-04-30 00:00:00.000|5225                        |
|2008-04-30 00:00:00.000|5225                        |
|2008-04-30 00:00:00.000|5225                        |
|2008-04-30 00:00:00.000|5225                        |
|2008-04-30 00:00:00.000|5225                        |
|2008-04-30 00:00:00.000|5225                        |
|2008-04-30 00:00:00.000|5225                        |
|2008-04-30 00:00:00.000|5225                        |
|2008-04-30 00:00:00.000|5225                        |
|2008-04-30 00:00:00.000|5225                        |
+-----------------------+----------------------------+

#The below example returns the months between two dates 
 
df.select(col("ModifiedDate"), 
    months_between(current_timestamp(),col("ModifiedDate")).alias("months_between")  
  ).show()
 
#round(col("score")
+--------------------+--------------+
|        ModifiedDate|months_between|
+--------------------+--------------+
|2008-04-30 00:00:...|  171.69574037|
|2008-04-30 00:00:...|  171.69574037|
|2008-04-30 00:00:...|  171.69574037|
|2008-04-30 00:00:...|  171.69574037|
|2008-04-30 00:00:...|  171.69574037|
|2008-04-30 00:00:...|  171.69574037|
|2008-04-30 00:00:...|  171.69574037|
|2008-04-30 00:00:...|  171.69574037|
|2008-04-30 00:00:...|  171.69574037|
|2008-04-30 00:00:...|  171.69574037|
|2008-04-30 00:00:...|  171.69574037|
|2008-04-30 00:00:...|  171.69574037|
|2008-04-30 00:00:...|  171.69574037|
|2008-04-30 00:00:...|  171.69574037|
|2008-04-30 00:00:...|  171.69574037|
|2008-04-30 00:00:...|  171.69574037|
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
|           1|Engineering|Research and Deve...|2008-04-30 00:00:...|2022-08-20|
|           2|Tool Design|Research and Deve...|2008-04-30 00:00:...|2022-08-20|
+------------+-----------+--------------------+--------------------+----------+
only showing top 2 rows

+------------+-----------+--------------------+--------------------+----+
|DepartmentID|       Name|           GroupName|        ModifiedDate|Days|
+------------+-----------+--------------------+--------------------+----+
|           1|Engineering|Research and Deve...|2008-04-30 00:00:...|5225|
|           2|Tool Design|Research and Deve...|2008-04-30 00:00:...|5225|
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
Out[99]: [Row(GroupName='Executive General and Administration'),
 Row(GroupName='Sales and Marketing'),
 Row(GroupName='Research and Development'),
 Row(GroupName='Quality Assurance'),
 Row(GroupName='Manufacturing'),
 Row(GroupName='Inventory Management')]
list = df.select(col('GroupName')).collect()
type(list[0][0])
Out[101]: str
list[5][0]
Out[102]: 'Research and Development'
list[0][0]
Out[103]: 'Research and Development'
# Generating a list GroupName = []
 
for GroupName in list:
    GroupName.asDict(GroupName[0])
GroupName
Out[104]: Row(GroupName='Executive General and Administration')
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
Out[117]: ['BusinessEntityID',
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

df2.columns
Out[122]: ['BusinessEntityID',
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
Out[131]: ['BusinessEntityID',
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







