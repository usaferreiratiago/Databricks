databricks-logoProject_1(Python)


# Import Libraries 
 
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import numpy as np
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
#import findspark as fd
dbutils.fs.ls("/FileStore/tables/Bronze/")
 
Out[3]: [FileInfo(path='dbfs:/FileStore/tables/Bronze/HumanResources_Department.csv', name='HumanResources_Department.csv', size=1136, modificationTime=1661338419000),
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
 FileInfo(path='dbfs:/FileStore/tables/Bronze/Production_Culture.csv', name='Production_Culture.csv', size=391, modificationTime=1661338464000),
 FileInfo(path='dbfs:/FileStore/tables/Bronze/Production_Document.csv', name='Production_Document.csv', size=533938, modificationTime=1661766413000),
 FileInfo(path='dbfs:/FileStore/tables/Bronze/Production_Illustration.csv', name='Production_Illustration.csv', size=123618, modificationTime=1661766411000),
 FileInfo(path='dbfs:/FileStore/tables/Bronze/Production_Location.csv', name='Production_Location.csv', size=807, modificationTime=1661766412000),
 FileInfo(path='dbfs:/FileStore/tables/Bronze/Production_Product.csv', name='Production_Product.csv', size=105251, modificationTime=1661766413000),
 FileInfo(path='dbfs:/FileStore/tables/Bronze/Production_ProductCategory.csv', name='Production_ProductCategory.csv', size=342, modificationTime=1661766414000),
 FileInfo(path='dbfs:/FileStore/tables/Bronze/Production_ProductCostHistory.csv', name='Production_ProductCostHistory.csv', size=30173, modificationTime=1661766414000),
 FileInfo(path='dbfs:/FileStore/tables/Bronze/Production_ProductDescription.csv', name='Production_ProductDescription.csv', size=143882, modificationTime=1661766415000),
 FileInfo(path='dbfs:/FileStore/tables/Bronze/Production_ProductDocument.csv', name='Production_ProductDocument.csv', size=1192, modificationTime=1661766415000),
 FileInfo(path='dbfs:/FileStore/tables/Bronze/Production_ProductInventory.csv', name='Production_ProductInventory.csv', size=82519, modificationTime=1661766416000),
 FileInfo(path='dbfs:/FileStore/tables/Bronze/Production_ProductListPriceHistory.csv', name='Production_ProductListPriceHistory.csv', size=29742, modificationTime=1661766416000),
 FileInfo(path='dbfs:/FileStore/tables/Bronze/Production_ProductModel.csv', name='Production_ProductModel.csv', size=54401, modificationTime=1661766416000),
 FileInfo(path='dbfs:/FileStore/tables/Bronze/Production_ProductModelIllustration.csv', name='Production_ProductModelIllustration.csv', size=256, modificationTime=1661766417000),
 FileInfo(path='dbfs:/FileStore/tables/Bronze/Production_ProductModelProductDescriptionCulture.csv', name='Production_ProductModelProductDescriptionCulture.csv', size=31829, modificationTime=1661766417000),
 FileInfo(path='dbfs:/FileStore/tables/Bronze/Production_ProductPhoto.csv', name='Production_ProductPhoto.csv', size=4021899, modificationTime=1661766437000),
 FileInfo(path='dbfs:/FileStore/tables/Bronze/Production_ProductProductPhoto.csv', name='Production_ProductProductPhoto.csv', size=16954, modificationTime=1661766418000),
 FileInfo(path='dbfs:/FileStore/tables/Bronze/Production_ProductReview.csv', name='Production_ProductReview.csv', size=5393, modificationTime=1661766419000),
 FileInfo(path='dbfs:/FileStore/tables/Bronze/Production_ProductSubcategory.csv', name='Production_ProductSubcategory.csv', size=2905, modificationTime=1661766420000),
 FileInfo(path='dbfs:/FileStore/tables/Bronze/Production_Purchasing_ProductVendor.csv', name='Production_Purchasing_ProductVendor.csv', size=41246, modificationTime=1661766421000),
 FileInfo(path='dbfs:/FileStore/tables/Bronze/Production_ScrapReason.csv', name='Production_ScrapReason.csv', size=832, modificationTime=1661766422000),
 FileInfo(path='dbfs:/FileStore/tables/Bronze/Production_TransactionHistory.csv', name='Production_TransactionHistory.csv', size=8909021, modificationTime=1661766466000),
 FileInfo(path='dbfs:/FileStore/tables/Bronze/Production_TransactionHistoryArchive.csv', name='Production_TransactionHistoryArchive.csv', size=6953529, modificationTime=1661766473000),
 FileInfo(path='dbfs:/FileStore/tables/Bronze/Production_UnitMeasure.csv', name='Production_UnitMeasure.csv', size=1548, modificationTime=1661766467000),
 FileInfo(path='dbfs:/FileStore/tables/Bronze/Production_WorkOrder.csv', name='Production_WorkOrder.csv', size=8600044, modificationTime=1661766510000),
 FileInfo(path='dbfs:/FileStore/tables/Bronze/Production_WorkOrderRouting.csv', name='Production_WorkOrderRouting.csv', size=10397211, modificationTime=1661766525000),
 FileInfo(path='dbfs:/FileStore/tables/Bronze/Purchasing_ProductVendor.csv', name='Purchasing_ProductVendor.csv', size=41246, modificationTime=1661766512000),
 FileInfo(path='dbfs:/FileStore/tables/Bronze/Purchasing_PurchaseOrderDetail.csv', name='Purchasing_PurchaseOrderDetail.csv', size=874739, modificationTime=1661766517000),
 FileInfo(path='dbfs:/FileStore/tables/Bronze/Purchasing_PurchaseOrderHeader.csv', name='Purchasing_PurchaseOrderHeader.csv', size=513118, modificationTime=1661766521000),
 FileInfo(path='dbfs:/FileStore/tables/Bronze/Purchasing_ShipMethod.csv', name='Purchasing_ShipMethod.csv', size=518, modificationTime=1661766523000),
 FileInfo(path='dbfs:/FileStore/tables/Bronze/Purchasing_Vendor.csv', name='Purchasing_Vendor.csv', size=7800, modificationTime=1661766524000),
 FileInfo(path='dbfs:/FileStore/tables/Bronze/Sales_CountryRegionCurrency.csv', name='Sales_CountryRegionCurrency.csv', size=3536, modificationTime=1661766527000),
 FileInfo(path='dbfs:/FileStore/tables/Bronze/Sales_CreditCard.csv', name='Sales_CreditCard.csv', size=1222858, modificationTime=1661766532000),
 FileInfo(path='dbfs:/FileStore/tables/Bronze/Sales_Currency.csv', name='Sales_Currency.csv', size=4452, modificationTime=1661766528000),
 FileInfo(path='dbfs:/FileStore/tables/Bronze/Sales_CurrencyRate.csv', name='Sales_CurrencyRate.csv', size=1025440, modificationTime=1661766534000),
 FileInfo(path='dbfs:/FileStore/tables/Bronze/Sales_Customer.csv', name='Sales_Customer.csv', size=1813963, modificationTime=1661766542000),
 FileInfo(path='dbfs:/FileStore/tables/Bronze/Sales_PersonCreditCard.csv', name='Sales_PersonCreditCard.csv', size=687647, modificationTime=1661766538000),
 FileInfo(path='dbfs:/FileStore/tables/Bronze/Sales_SalesOrderDetail.csv', name='Sales_SalesOrderDetail.csv', size=13801182, modificationTime=1661766607000),
 FileInfo(path='dbfs:/FileStore/tables/Bronze/Sales_SalesOrderHeader.csv', name='Sales_SalesOrderHeader.csv', size=8267704, modificationTime=1661766584000),
 FileInfo(path='dbfs:/FileStore/tables/Bronze/Sales_SalesOrderHeaderSalesReason.csv', name='Sales_SalesOrderHeaderSalesReason.csv', size=913790, modificationTime=1661766589000),
 FileInfo(path='dbfs:/FileStore/tables/Bronze/Sales_SalesPerson.csv', name='Sales_SalesPerson.csv', size=2050, modificationTime=1661766590000),
 FileInfo(path='dbfs:/FileStore/tables/Bronze/Sales_SalesPersonQuotaHistory.csv', name='Sales_SalesPersonQuotaHistory.csv', size=16372, modificationTime=1661766591000),
 FileInfo(path='dbfs:/FileStore/tables/Bronze/Sales_SalesReason.csv', name='Sales_SalesReason.csv', size=523, modificationTime=1661766592000),
 FileInfo(path='dbfs:/FileStore/tables/Bronze/Sales_SalesTaxRate.csv', name='Sales_SalesTaxRate.csv', size=2845, modificationTime=1661766592000),
 FileInfo(path='dbfs:/FileStore/tables/Bronze/Sales_SalesTerritory.csv', name='Sales_SalesTerritory.csv', size=1347, modificationTime=1661766593000),
 FileInfo(path='dbfs:/FileStore/tables/Bronze/Sales_SalesTerritoryHistory.csv', name='Sales_SalesTerritoryHistory.csv', size=1798, modificationTime=1661766594000),
 FileInfo(path='dbfs:/FileStore/tables/Bronze/Sales_ShoppingCartItem.csv', name='Sales_ShoppingCartItem.csv', size=271, modificationTime=1661766595000),
 FileInfo(path='dbfs:/FileStore/tables/Bronze/Sales_SpecialOffer.csv', name='Sales_SpecialOffer.csv', size=2886, modificationTime=1661766596000),
 FileInfo(path='dbfs:/FileStore/tables/Bronze/Sales_SpecialOfferProduct.csv', name='Sales_SpecialOfferProduct.csv', size=36680, modificationTime=1661766597000),
 FileInfo(path='dbfs:/FileStore/tables/Bronze/Sales_Store.csv', name='Sales_Store.csv', size=360866, modificationTime=1661766599000)]
# Create SparkSession
 
spark = (
    SparkSession.builder
    .master('local')
    .appName('Project_01')
    .getOrCreate()
)
df = spark.read.format("csv").option("infer Schema" , True) .option("header", True ) .option("sep",","). load("/FileStore/tables/Bronze/HumanResources_Department.csv")
#display(df)
#print(df.count())
df.show(3)
+------------+-----------+--------------------+--------------------+
|DepartmentID|       Name|           GroupName|        ModifiedDate|
+------------+-----------+--------------------+--------------------+
|           1|Engineering|Research and Deve...|2008-04-30 00:00:...|
|           2|Tool Design|Research and Deve...|2008-04-30 00:00:...|
|           3|      Sales| Sales and Marketing|2008-04-30 00:00:...|
+------------+-----------+--------------------+--------------------+
only showing top 3 rows

# Check Schema
df.printSchema()
root
 |-- DepartmentID: string (nullable = true)
 |-- Name: string (nullable = true)
 |-- GroupName: string (nullable = true)
 |-- ModifiedDate: string (nullable = true)

# Checking Datas null -- Pandas has limitations #Don't use - Only try
 
df.toPandas().isna().sum()
Out[7]: DepartmentID    0
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
df.show(3)
+------------+-----------+--------------------+--------------------+
|DepartmentID|       Name|           GroupName|        ModifiedDate|
+------------+-----------+--------------------+--------------------+
|           1|Engineering|Research and Deve...|2008-04-30 00:00:...|
|           2|Tool Design|Research and Deve...|2008-04-30 00:00:...|
|           3|      Sales| Sales and Marketing|2008-04-30 00:00:...|
+------------+-----------+--------------------+--------------------+
only showing top 3 rows

# Check all Columns
 
df.columns
Out[10]: ['DepartmentID', 'Name', 'GroupName', 'ModifiedDate']
# Select Columns
 
df.select(col('GroupName'),col('Name')).show(3)
+--------------------+-----------+
|           GroupName|       Name|
+--------------------+-----------+
|Research and Deve...|Engineering|
|Research and Deve...|Tool Design|
| Sales and Marketing|      Sales|
+--------------------+-----------+
only showing top 3 rows

# Create Alias
# Always you need to put ## [ df = ]## to Save
 
df.select(col('Name').alias('Names')).show(3)
+-----------+
|      Names|
+-----------+
|Engineering|
|Tool Design|
|      Sales|
+-----------+
only showing top 3 rows

# using Split - only to know about it, if see on another code
# Always you need to put ## [ df = ]## to Save
 
df.select('DepartmentID Name GroupName ModifiedDate'.split()).show(3)
+------------+-----------+--------------------+--------------------+
|DepartmentID|       Name|           GroupName|        ModifiedDate|
+------------+-----------+--------------------+--------------------+
|           1|Engineering|Research and Deve...|2008-04-30 00:00:...|
|           2|Tool Design|Research and Deve...|2008-04-30 00:00:...|
|           3|      Sales| Sales and Marketing|2008-04-30 00:00:...|
+------------+-----------+--------------------+--------------------+
only showing top 3 rows

# Showing Columns as you want to see 
 
df.select('Name','GroupName').show(3)
+-----------+--------------------+
|       Name|           GroupName|
+-----------+--------------------+
|Engineering|Research and Deve...|
|Tool Design|Research and Deve...|
|      Sales| Sales and Marketing|
+-----------+--------------------+
only showing top 3 rows

# Filtring df  --Showing only the specific column and specific filtern and putting distinct function to not duplicate information
 
df.select(col('GroupName')).filter(col('GroupName') == "Inventory Management").distinct().show(3)
+--------------------+
|           GroupName|
+--------------------+
|Inventory Management|
+--------------------+

#Showing all df
 
df.show(3)
+------------+-----------+--------------------+--------------------+
|DepartmentID|       Name|           GroupName|        ModifiedDate|
+------------+-----------+--------------------+--------------------+
|           1|Engineering|Research and Deve...|2008-04-30 00:00:...|
|           2|Tool Design|Research and Deve...|2008-04-30 00:00:...|
|           3|      Sales| Sales and Marketing|2008-04-30 00:00:...|
+------------+-----------+--------------------+--------------------+
only showing top 3 rows

# Filtring df with more conditions and specific columns (AND / &)
 
df.select('Name','ModifiedDate').filter((col('Name') == "Finance")).show(3)
+-------+--------------------+
|   Name|        ModifiedDate|
+-------+--------------------+
|Finance|2008-04-30 00:00:...|
+-------+--------------------+

# Filtring df with specific columns and more conditions  (AND / &)
 
df.select('DepartmentID','Name').filter((col('Name') == "Finance") & (col('DepartmentID') == 10)).show(3) 
+------------+-------+
|DepartmentID|   Name|
+------------+-------+
|          10|Finance|
+------------+-------+

# Filtring df with specific columns and more conditions  (AND / &)
 
df.select('DepartmentId','Name').filter((col('DepartmentID') == 2)).show(3)
+------------+-----------+
|DepartmentId|       Name|
+------------+-----------+
|           2|Tool Design|
+------------+-----------+

# Filtring df with specific columns and more conditions  (AND / &)
 
df.select('DepartmentId','Name').filter((col('DepartmentID') != 12)).show(3)
+------------+-----------+
|DepartmentId|       Name|
+------------+-----------+
|           1|Engineering|
|           2|Tool Design|
|           3|      Sales|
+------------+-----------+
only showing top 3 rows

# Filtring df with specific columns and more conditions  (AND / &)
## df.filter('Name = "Finance"').filter(col('DepartmentId') == 1).show()
 
df.select('DepartmentId','Name').filter((col('DepartmentID') >= 3)).show(3)
+------------+----------+
|DepartmentId|      Name|
+------------+----------+
|           3|     Sales|
|           4| Marketing|
|           5|Purchasing|
+------------+----------+
only showing top 3 rows

# Filtring df with specific columns and more conditions  (AND / &)
 
df.select('DepartmentId','Name').filter((col('DepartmentID') <= 16)).show(3)
+------------+-----------+
|DepartmentId|       Name|
+------------+-----------+
|           1|Engineering|
|           2|Tool Design|
|           3|      Sales|
+------------+-----------+
only showing top 3 rows

# Filtring df with more conditions (OR / |)
 
df.filter('DepartmentID = "1"').show(3)
+------------+-----------+--------------------+--------------------+
|DepartmentID|       Name|           GroupName|        ModifiedDate|
+------------+-----------+--------------------+--------------------+
|           1|Engineering|Research and Deve...|2008-04-30 00:00:...|
+------------+-----------+--------------------+--------------------+

# Filtring df with more conditions (OR / |)
 
df.filter((col('Name') == 'Finance') | (col('Name') == 'Sales') | (col('DepartmentID') == 12)).show(3)
+------------+----------------+--------------------+--------------------+
|DepartmentID|            Name|           GroupName|        ModifiedDate|
+------------+----------------+--------------------+--------------------+
|           3|           Sales| Sales and Marketing|2008-04-30 00:00:...|
|          10|         Finance|Executive General...|2008-04-30 00:00:...|
|          12|Document Control|   Quality Assurance|2008-04-30 00:00:...|
+------------+----------------+--------------------+--------------------+

# # Filtring df with more conditions (OR / |)
 
df.filter(col('GroupName') == 'Quality Assurance').show(3)
+------------+-----------------+-----------------+--------------------+
|DepartmentID|             Name|        GroupName|        ModifiedDate|
+------------+-----------------+-----------------+--------------------+
|          12| Document Control|Quality Assurance|2008-04-30 00:00:...|
|          13|Quality Assurance|Quality Assurance|2008-04-30 00:00:...|
+------------+-----------------+-----------------+--------------------+

# # Filtring df combining & and | # And e OR #
 
df.filter((col('GroupName') == "Quality Assurance")  | (col('Name') == "Sales") | (col('DepartmentID') == 10)).show(3)
+------------+----------------+--------------------+--------------------+
|DepartmentID|            Name|           GroupName|        ModifiedDate|
+------------+----------------+--------------------+--------------------+
|           3|           Sales| Sales and Marketing|2008-04-30 00:00:...|
|          10|         Finance|Executive General...|2008-04-30 00:00:...|
|          12|Document Control|   Quality Assurance|2008-04-30 00:00:...|
+------------+----------------+--------------------+--------------------+
only showing top 3 rows

# Concatenate Columns without space
 
df.withColumn("DepartmentID + GroupName", concat('DepartmentID','GroupName')).show(3)
+------------+-----------+--------------------+--------------------+------------------------+
|DepartmentID|       Name|           GroupName|        ModifiedDate|DepartmentID + GroupName|
+------------+-----------+--------------------+--------------------+------------------------+
|           1|Engineering|Research and Deve...|2008-04-30 00:00:...|    1Research and Dev...|
|           2|Tool Design|Research and Deve...|2008-04-30 00:00:...|    2Research and Dev...|
|           3|      Sales| Sales and Marketing|2008-04-30 00:00:...|    3Sales and Marketing|
+------------+-----------+--------------------+--------------------+------------------------+
only showing top 3 rows

# Concatenate Columns with space
 
df.withColumn("DepartmentID + GroupName", concat_ws(' ', 'DepartmentID','GroupName')).show(3)
+------------+-----------+--------------------+--------------------+------------------------+
|DepartmentID|       Name|           GroupName|        ModifiedDate|DepartmentID + GroupName|
+------------+-----------+--------------------+--------------------+------------------------+
|           1|Engineering|Research and Deve...|2008-04-30 00:00:...|    1 Research and De...|
|           2|Tool Design|Research and Deve...|2008-04-30 00:00:...|    2 Research and De...|
|           3|      Sales| Sales and Marketing|2008-04-30 00:00:...|    3 Sales and Marke...|
+------------+-----------+--------------------+--------------------+------------------------+
only showing top 3 rows

# Alter type of Column
 
df.show(3)
+------------+-----------+--------------------+--------------------+
|DepartmentID|       Name|           GroupName|        ModifiedDate|
+------------+-----------+--------------------+--------------------+
|           1|Engineering|Research and Deve...|2008-04-30 00:00:...|
|           2|Tool Design|Research and Deve...|2008-04-30 00:00:...|
|           3|      Sales| Sales and Marketing|2008-04-30 00:00:...|
+------------+-----------+--------------------+--------------------+
only showing top 3 rows

# Alter type of Column
 
df.printSchema()
root
 |-- DepartmentID: string (nullable = true)
 |-- Name: string (nullable = true)
 |-- GroupName: string (nullable = true)
 |-- ModifiedDate: string (nullable = true)

# Alter type of Metada of the Column
 
#df.withColumn('ModifiedDate', col('ModifiedDate').cast(IntegerType())).show(truncate=False)
 
df.show(3)
+------------+-----------+--------------------+--------------------+
|DepartmentID|       Name|           GroupName|        ModifiedDate|
+------------+-----------+--------------------+--------------------+
|           1|Engineering|Research and Deve...|2008-04-30 00:00:...|
|           2|Tool Design|Research and Deve...|2008-04-30 00:00:...|
|           3|      Sales| Sales and Marketing|2008-04-30 00:00:...|
+------------+-----------+--------------------+--------------------+
only showing top 3 rows

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
         datediff(current_timestamp(), col('ModifiedDate')).alias('difference between two dates')).show(3)
+--------------------+----------------------------+
|        ModifiedDate|difference between two dates|
+--------------------+----------------------------+
|2008-04-30 00:00:...|                        5235|
|2008-04-30 00:00:...|                        5235|
|2008-04-30 00:00:...|                        5235|
+--------------------+----------------------------+
only showing top 3 rows

#The below example returns the months between two dates 
 
df.select(col("ModifiedDate"), 
    months_between(current_timestamp(),col("ModifiedDate")).alias("months_between")  
  ).show(3)
 
#round(col("score")
+--------------------+--------------+
|        ModifiedDate|months_between|
+--------------------+--------------+
|2008-04-30 00:00:...|         172.0|
|2008-04-30 00:00:...|         172.0|
|2008-04-30 00:00:...|         172.0|
+--------------------+--------------+
only showing top 3 rows

#Using round numbers ('Arredontar numeros')
 
df.select("*",round(col("DepartmentID")).alias("Teste")).show(3)
+------------+-----------+--------------------+--------------------+-----+
|DepartmentID|       Name|           GroupName|        ModifiedDate|Teste|
+------------+-----------+--------------------+--------------------+-----+
|           1|Engineering|Research and Deve...|2008-04-30 00:00:...|  1.0|
|           2|Tool Design|Research and Deve...|2008-04-30 00:00:...|  2.0|
|           3|      Sales| Sales and Marketing|2008-04-30 00:00:...|  3.0|
+------------+-----------+--------------------+--------------------+-----+
only showing top 3 rows

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
|           1|Engineering|Research and Deve...|2008-04-30 00:00:...|2022-08-30|
|           2|Tool Design|Research and Deve...|2008-04-30 00:00:...|2022-08-30|
+------------+-----------+--------------------+--------------------+----------+
only showing top 2 rows

+------------+-----------+--------------------+--------------------+----+
|DepartmentID|       Name|           GroupName|        ModifiedDate|Days|
+------------+-----------+--------------------+--------------------+----+
|           1|Engineering|Research and Deve...|2008-04-30 00:00:...|5235|
|           2|Tool Design|Research and Deve...|2008-04-30 00:00:...|5235|
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
 
df.select(col('GroupName')).distinct().show(3)
+--------------------+
|           GroupName|
+--------------------+
| Sales and Marketing|
|Research and Deve...|
|       Manufacturing|
+--------------------+
only showing top 3 rows

# Using Collect - show all the rows#
 
df.select(col('GroupName')).distinct().collect()
Out[41]: [Row(GroupName='Executive General and Administration'),
 Row(GroupName='Sales and Marketing'),
 Row(GroupName='Research and Development'),
 Row(GroupName='Quality Assurance'),
 Row(GroupName='Manufacturing'),
 Row(GroupName='Inventory Management')]
list = df.select(col('GroupName')).collect()
type(list[0][0])
Out[43]: str
list[5][0]
Out[44]: 'Research and Development'
list[0][0]
Out[45]: 'Research and Development'
# Generating a list GroupName = []
 
for GroupName in list:
    GroupName.asDict(GroupName[0])
GroupName
Out[46]: Row(GroupName='Executive General and Administration')
## Working with When () / Otherwise()##
 
##df.withColumn('Correct', when(col('GroupName') == "Manufacturing", lit("OK"))).otherwise("NOT")
 
df.withColumn("Correct", when(col("GroupName") == "Manufacturing" , lit("OK")).otherwise("")).distinct().show(6)
+------------+--------------------+--------------------+--------------------+-------+
|DepartmentID|                Name|           GroupName|        ModifiedDate|Correct|
+------------+--------------------+--------------------+--------------------+-------+
|           6|Research and Deve...|Research and Deve...|2008-04-30 00:00:...|       |
|           3|               Sales| Sales and Marketing|2008-04-30 00:00:...|       |
|           2|         Tool Design|Research and Deve...|2008-04-30 00:00:...|       |
|           4|           Marketing| Sales and Marketing|2008-04-30 00:00:...|       |
|           7|          Production|       Manufacturing|2008-04-30 00:00:...|     OK|
|           1|         Engineering|Research and Deve...|2008-04-30 00:00:...|       |
+------------+--------------------+--------------------+--------------------+-------+
only showing top 6 rows

## Working with When () / Otherwise()##
 
##df.withColumn('Correct', when(col('GroupName') == "Manufacturing", lit("OK"))).otherwise("NOT")
 
df.withColumn("Correct", when(col("GroupName").isin("GroupName"),'Correct')
             
 
.otherwise("Ok")).distinct().show(3)
+------------+-----------+--------------------+--------------------+-------+
|DepartmentID|       Name|           GroupName|        ModifiedDate|Correct|
+------------+-----------+--------------------+--------------------+-------+
|           4|  Marketing| Sales and Marketing|2008-04-30 00:00:...|     Ok|
|           3|      Sales| Sales and Marketing|2008-04-30 00:00:...|     Ok|
|           2|Tool Design|Research and Deve...|2008-04-30 00:00:...|     Ok|
+------------+-----------+--------------------+--------------------+-------+
only showing top 3 rows

## Working with OrderBy desc
 
df.orderBy(col("GroupName").asc()).show(3)
+------------+--------------------+--------------------+--------------------+
|DepartmentID|                Name|           GroupName|        ModifiedDate|
+------------+--------------------+--------------------+--------------------+
|           9|     Human Resources|Executive General...|2008-04-30 00:00:...|
|          11|Information Services|Executive General...|2008-04-30 00:00:...|
|          10|             Finance|Executive General...|2008-04-30 00:00:...|
+------------+--------------------+--------------------+--------------------+
only showing top 3 rows

## Working with OrderBy asc
 
df.orderBy(col("GroupName").asc()).show(3)
+------------+--------------------+--------------------+--------------------+
|DepartmentID|                Name|           GroupName|        ModifiedDate|
+------------+--------------------+--------------------+--------------------+
|           9|     Human Resources|Executive General...|2008-04-30 00:00:...|
|          11|Information Services|Executive General...|2008-04-30 00:00:...|
|          10|             Finance|Executive General...|2008-04-30 00:00:...|
+------------+--------------------+--------------------+--------------------+
only showing top 3 rows

## Working with OrderBy, Distinct asc
 
df.orderBy(col("GroupName").asc()).distinct().show(3)
+------------+-----------+--------------------+--------------------+
|DepartmentID|       Name|           GroupName|        ModifiedDate|
+------------+-----------+--------------------+--------------------+
|           3|      Sales| Sales and Marketing|2008-04-30 00:00:...|
|           2|Tool Design|Research and Deve...|2008-04-30 00:00:...|
|           4|  Marketing| Sales and Marketing|2008-04-30 00:00:...|
+------------+-----------+--------------------+--------------------+
only showing top 3 rows

## Working with GroupBy, Count, Distinct and asc
 
df.groupBy("GroupName").count().distinct().show(3)
+--------------------+-----+
|           GroupName|count|
+--------------------+-----+
|Executive General...|    5|
| Sales and Marketing|    2|
|Research and Deve...|    3|
+--------------------+-----+
only showing top 3 rows

df2 = spark.read.format("csv").option("infer Schema" , True) .option("header", True ) .option("sep",","). load("/FileStore/tables/Bronze/HumanResources_Employee.csv")
#display(df)
#print(df.count())
df2.show(3)
+----------------+----------------+--------------------+----------------+-----------------+--------------------+----------+-------------+------+----------+------------+-------------+--------------+-----------+--------------------+--------------------+
|BusinessEntityID|NationalIDNumber|             LoginID|OrganizationNode|OrganizationLevel|            JobTitle| BirthDate|MaritalStatus|Gender|  HireDate|SalariedFlag|VacationHours|SickLeaveHours|CurrentFlag|             rowguid|        ModifiedDate|
+----------------+----------------+--------------------+----------------+-----------------+--------------------+----------+-------------+------+----------+------------+-------------+--------------+-----------+--------------------+--------------------+
|               1|       295847284|adventure-works\ken0|            NULL|             NULL|Chief Executive O...|1969-01-29|            S|     M|2009-01-14|           1|           99|            69|          1|F01251E5-96A3-448...|2014-06-30 00:00:...|
|               2|       245797967|adventure-works\t...|            0x58|                1|Vice President of...|1971-08-01|            S|     F|2008-01-31|           1|            1|            20|          1|45E8F437-670D-440...|2014-06-30 00:00:...|
|               3|       509647174|adventure-works\r...|          0x5AC0|                2| Engineering Manager|1974-11-12|            M|     M|2007-11-11|           1|            2|            21|          1|9BBBFB2C-EFBB-421...|2014-06-30 00:00:...|
+----------------+----------------+--------------------+----------------+-----------------+--------------------+----------+-------------+------+----------+------------+-------------+--------------+-----------+--------------------+--------------------+
only showing top 3 rows

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
Out[56]: ['BusinessEntityID',
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
 
df2.select('BusinessEntityID LoginID OrganizationNode OrganizationLevel JobTitle '.split()).show(3)
+----------------+--------------------+----------------+-----------------+--------------------+
|BusinessEntityID|             LoginID|OrganizationNode|OrganizationLevel|            JobTitle|
+----------------+--------------------+----------------+-----------------+--------------------+
|               1|adventure-works\ken0|            NULL|             NULL|Chief Executive O...|
|               2|adventure-works\t...|            0x58|                1|Vice President of...|
|               3|adventure-works\r...|          0x5AC0|                2| Engineering Manager|
+----------------+--------------------+----------------+-----------------+--------------------+
only showing top 3 rows

#empDF.join(deptDF,empDF.emp_dept_id ==  deptDF.dept_id,"inner") \
     #.show(truncate=False)
 
 
#(deptDF,empDF.emp_dept_id ==  deptDF.dept_id,"inner")
 
dfinner = df.join(df2,df.DepartmentID ==  df2.BusinessEntityID,"leftouter")
dfinner.show(3)
+------------+-----------+--------------------+--------------------+----------------+----------------+--------------------+----------------+-----------------+--------------------+----------+-------------+------+----------+------------+-------------+--------------+-----------+--------------------+--------------------+
|DepartmentID|       Name|           GroupName|        ModifiedDate|BusinessEntityID|NationalIDNumber|             LoginID|OrganizationNode|OrganizationLevel|            JobTitle| BirthDate|MaritalStatus|Gender|  HireDate|SalariedFlag|VacationHours|SickLeaveHours|CurrentFlag|             rowguid|        ModifiedDate|
+------------+-----------+--------------------+--------------------+----------------+----------------+--------------------+----------------+-----------------+--------------------+----------+-------------+------+----------+------------+-------------+--------------+-----------+--------------------+--------------------+
|           1|Engineering|Research and Deve...|2008-04-30 00:00:...|               1|       295847284|adventure-works\ken0|            NULL|             NULL|Chief Executive O...|1969-01-29|            S|     M|2009-01-14|           1|           99|            69|          1|F01251E5-96A3-448...|2014-06-30 00:00:...|
|           2|Tool Design|Research and Deve...|2008-04-30 00:00:...|               2|       245797967|adventure-works\t...|            0x58|                1|Vice President of...|1971-08-01|            S|     F|2008-01-31|           1|            1|            20|          1|45E8F437-670D-440...|2014-06-30 00:00:...|
|           3|      Sales| Sales and Marketing|2008-04-30 00:00:...|               3|       509647174|adventure-works\r...|          0x5AC0|                2| Engineering Manager|1974-11-12|            M|     M|2007-11-11|           1|            2|            21|          1|9BBBFB2C-EFBB-421...|2014-06-30 00:00:...|
+------------+-----------+--------------------+--------------------+----------------+----------------+--------------------+----------------+-----------------+--------------------+----------+-------------+------+----------+------------+-------------+--------------+-----------+--------------------+--------------------+
only showing top 3 rows

#empDF.join(deptDF,empDF.emp_dept_id ==  deptDF.dept_id,"inner") \
     #.show(truncate=False)
 
 
#(deptDF,empDF.emp_dept_id ==  deptDF.dept_id,"inner")
 
dfHR = df.join(df2,df.DepartmentID ==  df2.BusinessEntityID,"leftouter")\
.select(col("JobTitle"))
dfHR.show(3)
+--------------------+
|            JobTitle|
+--------------------+
|Chief Executive O...|
|Vice President of...|
| Engineering Manager|
+--------------------+
only showing top 3 rows

### Joins Dataframes ###
 
 
#df.join(df2, df.DepartmentID == df2.BusinessEntityID, "inner")\
#.select(df2.BusinessEntityID.alias("Entity"), \
#df.DepartmentID.alias('Department'), \
#df.Name.alias('Name')).show(truncate= False)
df2.columns
Out[61]: ['BusinessEntityID',
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
df2.select(col("LoginID")).show(3)
+--------------------+
|             LoginID|
+--------------------+
|adventure-works\ken0|
|adventure-works\t...|
|adventure-works\r...|
+--------------------+
only showing top 3 rows

df3 = spark.read.format("csv").option("infer Schema" , True) .option("header", True ) .option("sep",","). load("/FileStore/tables/Bronze/HumanResources_EmployeeDepartmentHistory.csv")
#display(df)
#print(df.count())
df3.show(3)
+----------------+------------+-------+----------+-------+--------------------+
|BusinessEntityID|DepartmentID|ShiftID| StartDate|EndDate|        ModifiedDate|
+----------------+------------+-------+----------+-------+--------------------+
|               1|          16|      1|2009-01-14|   NULL|2009-01-13 00:00:...|
|               2|           1|      1|2008-01-31|   NULL|2008-01-30 00:00:...|
|               3|           1|      1|2007-11-11|   NULL|2007-11-10 00:00:...|
+----------------+------------+-------+----------+-------+--------------------+
only showing top 3 rows

for column in df3.columns:
    print(column,df3.filter(df3[column].isNull()).count())
BusinessEntityID 0
DepartmentID 0
ShiftID 0
StartDate 0
EndDate 0
ModifiedDate 0
df3.columns
Out[65]: ['BusinessEntityID',
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

 # List the DBFS root
    
# %fs ls
    
 # Recursively remove the files under foobar
    
# %fs rm -r dbfs:/foobar
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

# using Split - only to know about it, if see on another code
# Always you need to put ## [ df = ]## to Save
 
df2.select('BusinessEntityID LoginID OrganizationNode OrganizationLevel JobTitle '.split()).show(3)
+----------------+--------------------+----------------+-----------------+--------------------+
|BusinessEntityID|             LoginID|OrganizationNode|OrganizationLevel|            JobTitle|
+----------------+--------------------+----------------+-----------------+--------------------+
|               1|adventure-works\ken0|            NULL|             NULL|Chief Executive O...|
|               2|adventure-works\t...|            0x58|                1|Vice President of...|
|               3|adventure-works\r...|          0x5AC0|                2| Engineering Manager|
+----------------+--------------------+----------------+-----------------+--------------------+
only showing top 3 rows

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
 
where JobTitle in ('Senior Tool Designer','Tool Designer') --and SUBSTRING(LoginID, 17, 100) = 'rob0'
 
 
 
""").show(truncate=False)
 
#display(data_test)
 
 
+--------+----------+--------------------+----------+-------------+------+
|Login   |HireDate  |JobTitle            |BirthDate |MaritalStatus|Gender|
+--------+----------+--------------------+----------+-------------+------+
|rob0    |2007-12-05|Senior Tool Designer|1974-12-23|Single       |Male  |
|ovidiu0 |2010-12-05|Senior Tool Designer|1978-01-17|Single       |Male  |
|thierry0|2007-12-11|Tool Designer       |1959-07-29|Married      |Male  |
|janice0 |2010-12-23|Tool Designer       |1989-05-28|Married      |Female|
+--------+----------+--------------------+----------+-------------+------+

# Save as TAble ("Will be saved as parquet file")
 
df2.write.option("path", "/FileStore/tables/Silver").saveAsTable("HumanResources_Employee")                                                                          ##df2.write.saveAsTable("HumanResources_Employee")
AnalysisException: Table default.HumanResources_Employee already exists
%sql
 
SHOW TABLES
 
database
tableName
isTemporary
1
humanresources_employee
true
Showing all 1 rows.

# Check List Databases
 
spark.catalog.listDatabases()
Out[74]: [Database(name='default', catalog='spark_catalog', description='Default Hive database', locationUri='dbfs:/user/hive/warehouse')]
# Check Columns on the Table
 
spark.catalog.listColumns("HumanResources_Employee")
Out[75]: [Column(name='BusinessEntityID', description=None, dataType='string', nullable=True, isPartition=False, isBucket=False),
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
# Check Files inside of Folder
 
dbutils.fs.ls("/FileStore/tables/Bronze/")
Out[76]: [FileInfo(path='dbfs:/FileStore/tables/Bronze/HumanResources_Department.csv', name='HumanResources_Department.csv', size=1136, modificationTime=1661338419000),
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
 FileInfo(path='dbfs:/FileStore/tables/Bronze/Production_Culture.csv', name='Production_Culture.csv', size=391, modificationTime=1661338464000),
 FileInfo(path='dbfs:/FileStore/tables/Bronze/Production_Document.csv', name='Production_Document.csv', size=533938, modificationTime=1661766413000),
 FileInfo(path='dbfs:/FileStore/tables/Bronze/Production_Illustration.csv', name='Production_Illustration.csv', size=123618, modificationTime=1661766411000),
 FileInfo(path='dbfs:/FileStore/tables/Bronze/Production_Location.csv', name='Production_Location.csv', size=807, modificationTime=1661766412000),
 FileInfo(path='dbfs:/FileStore/tables/Bronze/Production_Product.csv', name='Production_Product.csv', size=105251, modificationTime=1661766413000),
 FileInfo(path='dbfs:/FileStore/tables/Bronze/Production_ProductCategory.csv', name='Production_ProductCategory.csv', size=342, modificationTime=1661766414000),
 FileInfo(path='dbfs:/FileStore/tables/Bronze/Production_ProductCostHistory.csv', name='Production_ProductCostHistory.csv', size=30173, modificationTime=1661766414000),
 FileInfo(path='dbfs:/FileStore/tables/Bronze/Production_ProductDescription.csv', name='Production_ProductDescription.csv', size=143882, modificationTime=1661766415000),
 FileInfo(path='dbfs:/FileStore/tables/Bronze/Production_ProductDocument.csv', name='Production_ProductDocument.csv', size=1192, modificationTime=1661766415000),
 FileInfo(path='dbfs:/FileStore/tables/Bronze/Production_ProductInventory.csv', name='Production_ProductInventory.csv', size=82519, modificationTime=1661766416000),
 FileInfo(path='dbfs:/FileStore/tables/Bronze/Production_ProductListPriceHistory.csv', name='Production_ProductListPriceHistory.csv', size=29742, modificationTime=1661766416000),
 FileInfo(path='dbfs:/FileStore/tables/Bronze/Production_ProductModel.csv', name='Production_ProductModel.csv', size=54401, modificationTime=1661766416000),
 FileInfo(path='dbfs:/FileStore/tables/Bronze/Production_ProductModelIllustration.csv', name='Production_ProductModelIllustration.csv', size=256, modificationTime=1661766417000),
 FileInfo(path='dbfs:/FileStore/tables/Bronze/Production_ProductModelProductDescriptionCulture.csv', name='Production_ProductModelProductDescriptionCulture.csv', size=31829, modificationTime=1661766417000),
 FileInfo(path='dbfs:/FileStore/tables/Bronze/Production_ProductPhoto.csv', name='Production_ProductPhoto.csv', size=4021899, modificationTime=1661766437000),
 FileInfo(path='dbfs:/FileStore/tables/Bronze/Production_ProductProductPhoto.csv', name='Production_ProductProductPhoto.csv', size=16954, modificationTime=1661766418000),
 FileInfo(path='dbfs:/FileStore/tables/Bronze/Production_ProductReview.csv', name='Production_ProductReview.csv', size=5393, modificationTime=1661766419000),
 FileInfo(path='dbfs:/FileStore/tables/Bronze/Production_ProductSubcategory.csv', name='Production_ProductSubcategory.csv', size=2905, modificationTime=1661766420000),
 FileInfo(path='dbfs:/FileStore/tables/Bronze/Production_Purchasing_ProductVendor.csv', name='Production_Purchasing_ProductVendor.csv', size=41246, modificationTime=1661766421000),
 FileInfo(path='dbfs:/FileStore/tables/Bronze/Production_ScrapReason.csv', name='Production_ScrapReason.csv', size=832, modificationTime=1661766422000),
 FileInfo(path='dbfs:/FileStore/tables/Bronze/Production_TransactionHistory.csv', name='Production_TransactionHistory.csv', size=8909021, modificationTime=1661766466000),
 FileInfo(path='dbfs:/FileStore/tables/Bronze/Production_TransactionHistoryArchive.csv', name='Production_TransactionHistoryArchive.csv', size=6953529, modificationTime=1661766473000),
 FileInfo(path='dbfs:/FileStore/tables/Bronze/Production_UnitMeasure.csv', name='Production_UnitMeasure.csv', size=1548, modificationTime=1661766467000),
 FileInfo(path='dbfs:/FileStore/tables/Bronze/Production_WorkOrder.csv', name='Production_WorkOrder.csv', size=8600044, modificationTime=1661766510000),
 FileInfo(path='dbfs:/FileStore/tables/Bronze/Production_WorkOrderRouting.csv', name='Production_WorkOrderRouting.csv', size=10397211, modificationTime=1661766525000),
 FileInfo(path='dbfs:/FileStore/tables/Bronze/Purchasing_ProductVendor.csv', name='Purchasing_ProductVendor.csv', size=41246, modificationTime=1661766512000),
 FileInfo(path='dbfs:/FileStore/tables/Bronze/Purchasing_PurchaseOrderDetail.csv', name='Purchasing_PurchaseOrderDetail.csv', size=874739, modificationTime=1661766517000),
 FileInfo(path='dbfs:/FileStore/tables/Bronze/Purchasing_PurchaseOrderHeader.csv', name='Purchasing_PurchaseOrderHeader.csv', size=513118, modificationTime=1661766521000),
 FileInfo(path='dbfs:/FileStore/tables/Bronze/Purchasing_ShipMethod.csv', name='Purchasing_ShipMethod.csv', size=518, modificationTime=1661766523000),
 FileInfo(path='dbfs:/FileStore/tables/Bronze/Purchasing_Vendor.csv', name='Purchasing_Vendor.csv', size=7800, modificationTime=1661766524000),
 FileInfo(path='dbfs:/FileStore/tables/Bronze/Sales_CountryRegionCurrency.csv', name='Sales_CountryRegionCurrency.csv', size=3536, modificationTime=1661766527000),
 FileInfo(path='dbfs:/FileStore/tables/Bronze/Sales_CreditCard.csv', name='Sales_CreditCard.csv', size=1222858, modificationTime=1661766532000),
 FileInfo(path='dbfs:/FileStore/tables/Bronze/Sales_Currency.csv', name='Sales_Currency.csv', size=4452, modificationTime=1661766528000),
 FileInfo(path='dbfs:/FileStore/tables/Bronze/Sales_CurrencyRate.csv', name='Sales_CurrencyRate.csv', size=1025440, modificationTime=1661766534000),
 FileInfo(path='dbfs:/FileStore/tables/Bronze/Sales_Customer.csv', name='Sales_Customer.csv', size=1813963, modificationTime=1661766542000),
 FileInfo(path='dbfs:/FileStore/tables/Bronze/Sales_PersonCreditCard.csv', name='Sales_PersonCreditCard.csv', size=687647, modificationTime=1661766538000),
 FileInfo(path='dbfs:/FileStore/tables/Bronze/Sales_SalesOrderDetail.csv', name='Sales_SalesOrderDetail.csv', size=13801182, modificationTime=1661766607000),
 FileInfo(path='dbfs:/FileStore/tables/Bronze/Sales_SalesOrderHeader.csv', name='Sales_SalesOrderHeader.csv', size=8267704, modificationTime=1661766584000),
 FileInfo(path='dbfs:/FileStore/tables/Bronze/Sales_SalesOrderHeaderSalesReason.csv', name='Sales_SalesOrderHeaderSalesReason.csv', size=913790, modificationTime=1661766589000),
 FileInfo(path='dbfs:/FileStore/tables/Bronze/Sales_SalesPerson.csv', name='Sales_SalesPerson.csv', size=2050, modificationTime=1661766590000),
 FileInfo(path='dbfs:/FileStore/tables/Bronze/Sales_SalesPersonQuotaHistory.csv', name='Sales_SalesPersonQuotaHistory.csv', size=16372, modificationTime=1661766591000),
 FileInfo(path='dbfs:/FileStore/tables/Bronze/Sales_SalesReason.csv', name='Sales_SalesReason.csv', size=523, modificationTime=1661766592000),
 FileInfo(path='dbfs:/FileStore/tables/Bronze/Sales_SalesTaxRate.csv', name='Sales_SalesTaxRate.csv', size=2845, modificationTime=1661766592000),
 FileInfo(path='dbfs:/FileStore/tables/Bronze/Sales_SalesTerritory.csv', name='Sales_SalesTerritory.csv', size=1347, modificationTime=1661766593000),
 FileInfo(path='dbfs:/FileStore/tables/Bronze/Sales_SalesTerritoryHistory.csv', name='Sales_SalesTerritoryHistory.csv', size=1798, modificationTime=1661766594000),
 FileInfo(path='dbfs:/FileStore/tables/Bronze/Sales_ShoppingCartItem.csv', name='Sales_ShoppingCartItem.csv', size=271, modificationTime=1661766595000),
 FileInfo(path='dbfs:/FileStore/tables/Bronze/Sales_SpecialOffer.csv', name='Sales_SpecialOffer.csv', size=2886, modificationTime=1661766596000),
 FileInfo(path='dbfs:/FileStore/tables/Bronze/Sales_SpecialOfferProduct.csv', name='Sales_SpecialOfferProduct.csv', size=36680, modificationTime=1661766597000),
 FileInfo(path='dbfs:/FileStore/tables/Bronze/Sales_Store.csv', name='Sales_Store.csv', size=360866, modificationTime=1661766599000)]
#df.write.option("path", "/FileStore/tables/Silver").saveAsTable("HumanResources_Employee")
# Check List Tables
 
spark.catalog.listTables()
Out[79]: [Table(name='HumanResources_Employee', catalog=None, namespace=[], description=None, tableType='TEMPORARY', isTemporary=True)]
#SHOW VIEWS FROM default LIKE 'humanresources_employee'
 
#SHOW VIEWS LIKE ''
 
#DROP VIEW employeeView
 
 
spark.catalog.listColumns("humanresources_employee")
Out[81]: [Column(name='BusinessEntityID', description=None, dataType='string', nullable=True, isPartition=False, isBucket=False),
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
# Delete Files
 
%fs rm -r dbfs:/FileStore/tables/input/Put here File to delete.csv
dbutils.fs.rm('/mnt/adls2/demo/target/', True)
 
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
 
 
 
 
 
Command skipped
%fs
 
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
dbfs:/FileStore/tables/Bronze/Person_AddressType.csv
Person_AddressType.csv
478
1661338422000
dbfs:/FileStore/tables/Bronze/Person_BusinessEntity.csv
Person_BusinessEntity.csv
1401772
1661338430000
dbfs:/FileStore/tables/Bronze/Person_BusinessEntityAddress.csv
Person_BusinessEntityAddress.csv
1478939
1661338438000
dbfs:/FileStore/tables/Bronze/Person_BusinessEntityContact.csv
Person_BusinessEntityContact.csv
67492
1661338438000
dbfs:/FileStore/tables/Bronze/Person_ContactType.csv
Person_ContactType.csv
1002
1661338439000
dbfs:/FileStore/tables/Bronze/Person_CountryRegion.csv
Person_CountryRegion.csv
9352
1661338439000
dbfs:/FileStore/tables/Bronze/Person_EPhoneNumberType.csv
Person_EPhoneNumberType.csv
136
1661338440000
dbfs:/FileStore/tables/Bronze/Person_EmailAddress.csv
Person_EmailAddress.csv
2027800
1661338450000
dbfs:/FileStore/tables/Bronze/Person_Password.csv
Person_Password.csv
2426705
1661338453000
dbfs:/FileStore/tables/Bronze/Person_Person.csv
Person_Person.csv
13646947
1661338522000
Showing all 69 rows.

%fs
 
ls /FileStore/tables/Silver
 
path
name
size
modificationTime
1
2
dbfs:/FileStore/tables/Silver/_delta_log/
_delta_log/
0
0
dbfs:/FileStore/tables/Silver/part-00000-27fb4536-83d3-4d16-96bf-b9080c004f19-c000.snappy.parquet
part-00000-27fb4536-83d3-4d16-96bf-b9080c004f19-c000.snappy.parquet
28815
1661768971000
Showing all 2 rows.

%fs
 
ls /FileStore/tables/Gold
OK
%fs
 
ls /FileStore/tables
 
path
name
size
modificationTime
1
2
3
dbfs:/FileStore/tables/Bronze/
Bronze/
0
0
dbfs:/FileStore/tables/Gold/
Gold/
0
0
dbfs:/FileStore/tables/Silver/
Silver/
0
0
Showing all 3 rows.

%fs
 
ls /user
 
path
name
size
modificationTime
1
dbfs:/user/hive/
hive/
0
0
Showing all 1 rows.

%fs
 
ls /
 
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

df = spark.read.format("csv").option("infer Schema" , True).option("header", True ).option("sep",","). load("/FileStore/tables/Bronze/HumanResources_Employee.csv")
#display(df)
#print(df.count())
df.show(1)
+----------------+----------------+--------------------+----------------+-----------------+--------------------+----------+-------------+------+----------+------------+-------------+--------------+-----------+--------------------+--------------------+
|BusinessEntityID|NationalIDNumber|             LoginID|OrganizationNode|OrganizationLevel|            JobTitle| BirthDate|MaritalStatus|Gender|  HireDate|SalariedFlag|VacationHours|SickLeaveHours|CurrentFlag|             rowguid|        ModifiedDate|
+----------------+----------------+--------------------+----------------+-----------------+--------------------+----------+-------------+------+----------+------------+-------------+--------------+-----------+--------------------+--------------------+
|               1|       295847284|adventure-works\ken0|            NULL|             NULL|Chief Executive O...|1969-01-29|            S|     M|2009-01-14|           1|           99|            69|          1|F01251E5-96A3-448...|2014-06-30 00:00:...|
+----------------+----------------+--------------------+----------------+-----------------+--------------------+----------+-------------+------+----------+------------+-------------+--------------+-----------+--------------------+--------------------+
only showing top 1 row

%sql
 
show tables
 
database
tableName
isTemporary
1
humanresources_employee
true
Showing all 1 rows.

%sql
 
show views
 
namespace
viewName
isTemporary
1
humanresources_employee
true
Showing all 1 rows.

%sql
 
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
8
9
10
11
12
13
14
15
16
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
MaritalStatus
string
null
Gender
string
null
HireDate
string
null
SalariedFlag
string
null
VacationHours
string
null
SickLeaveHours
string
null
CurrentFlag
string
null
rowguid
string
null
ModifiedDate
string
null
Showing all 16 rows.

%sql
 
show views in global_temp
 
namespace
viewName
isTemporary
1
humanresources_employee
true
Showing all 1 rows.

%sql
 
select * from humanresources_employee
 
BusinessEntityID
NationalIDNumber
LoginID
OrganizationNode
OrganizationLevel
JobTitle
BirthDate
MaritalStatus
Gender
HireDate
SalariedFlag
VacationHours
SickLeaveHours
CurrentFlag
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
2009-01-14
1
99
69
1
2
245797967
adventure-works\terri0
0x58
1
Vice President of Engineering
1971-08-01
S
F
2008-01-31
1
1
20
1
3
509647174
adventure-works\roberto0
0x5AC0
2
Engineering Manager
1974-11-12
M
M
2007-11-11
1
2
21
1
4
112457891
adventure-works\rob0
0x5AD6
3
Senior Tool Designer
1974-12-23
S
M
2007-12-05
0
48
80
1
5
695256908
adventure-works\gail0
0x5ADA
3
Design Engineer
1952-09-27
M
F
2008-01-06
1
5
22
1
6
998320692
adventure-works\jossef0
0x5ADE
3
Design Engineer
1959-03-11
M
M
2008-01-24
1
6
23
1
7
134969118
adventure-works\dylan0
0x5AE1
3
Research and Development Manager
1987-02-24
M
M
2009-02-08
1
61
50
1
8
811994146
adventure-works\diane1
0x5AE158
4
Research and Development Engineer
1986-06-05
S
F
2008-12-29
1
62
51
1
9
658797903
adventure-works\gigi0
0x5AE168
4
Research and Development Engineer
1979-01-21
M
F
2009-01-16
1
63
51
1
10
879342154
adventure-works\michael6
0x5AE178
4
Research and Development Manager
1984-11-30
M
M
2009-05-03
1
16
64
1
11
974026903
adventure-works\ovidiu0
0x5AE3
3
Senior Tool Designer
1978-01-17
S
M
2010-12-05
0
7
23
1
12
480168528
adventure-works\thierry0
0x5AE358
4
Tool Designer
1959-07-29
M
M
2007-12-11
0
9
24
1
13
486228782
adventure-works\janice0
0x5AE368
4
Tool Designer
1989-05-28
M
F
2010-12-23
0
8
24
1
14
42487730
adventure-works\michael8
0x5AE5
3
Senior Design Engineer
1979-06-16
S
M
2010-12-30
1
3
21
1
15
56920285
adventure-works\sharon0
0x5AE7
3
Design Engineer
1961-05-02
M
F
2011-01-18
1
4
22
1
16
24756624
adventure-works\david0
0x68
1
Marketing Manager
1975-03-19
S
M
2007-12-20
1
40
40
1
17
253022876
adventure-works\kevin0
0x6AC0
2
Marketing Assistant
1987-05-03
S
M
2007-01-26
0
42
41
1
Showing all 290 rows.

#View Constructs a virtual table that has no physical data
#CreateOrReplace TempView:It is session based. It is saved in default database
#CreateOrReplaceGlobalTempView:It is not session based. It is saved in global_temp database
# Create Global Temp View
 
df.createOrReplaceGlobalTempView('HumanResources_Employee.csv')
# Save as csv file
 
#df.write.csv("/FileStore/tables/Silver/Test.csv")
df2 = spark.read.format("csv").option("infer Schema" , True) .option("header", True ) .option("sep",","). load("/FileStore/tables/Bronze/HumanResources_Employee.csv")
display(df2)
#print(df.count())
#df2.show(3)
 
BusinessEntityID
NationalIDNumber
LoginID
OrganizationNode
OrganizationLevel
JobTitle
BirthDate
MaritalStatus
Gender
HireDate
SalariedFlag
VacationHours
SickLeaveHours
CurrentFlag
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
2009-01-14
1
99
69
1
2
245797967
adventure-works\terri0
0x58
1
Vice President of Engineering
1971-08-01
S
F
2008-01-31
1
1
20
1
3
509647174
adventure-works\roberto0
0x5AC0
2
Engineering Manager
1974-11-12
M
M
2007-11-11
1
2
21
1
4
112457891
adventure-works\rob0
0x5AD6
3
Senior Tool Designer
1974-12-23
S
M
2007-12-05
0
48
80
1
5
695256908
adventure-works\gail0
0x5ADA
3
Design Engineer
1952-09-27
M
F
2008-01-06
1
5
22
1
6
998320692
adventure-works\jossef0
0x5ADE
3
Design Engineer
1959-03-11
M
M
2008-01-24
1
6
23
1
7
134969118
adventure-works\dylan0
0x5AE1
3
Research and Development Manager
1987-02-24
M
M
2009-02-08
1
61
50
1
8
811994146
adventure-works\diane1
0x5AE158
4
Research and Development Engineer
1986-06-05
S
F
2008-12-29
1
62
51
1
9
658797903
adventure-works\gigi0
0x5AE168
4
Research and Development Engineer
1979-01-21
M
F
2009-01-16
1
63
51
1
10
879342154
adventure-works\michael6
0x5AE178
4
Research and Development Manager
1984-11-30
M
M
2009-05-03
1
16
64
1
11
974026903
adventure-works\ovidiu0
0x5AE3
3
Senior Tool Designer
1978-01-17
S
M
2010-12-05
0
7
23
1
12
480168528
adventure-works\thierry0
0x5AE358
4
Tool Designer
1959-07-29
M
M
2007-12-11
0
9
24
1
13
486228782
adventure-works\janice0
0x5AE368
4
Tool Designer
1989-05-28
M
F
2010-12-23
0
8
24
1
14
42487730
adventure-works\michael8
0x5AE5
3
Senior Design Engineer
1979-06-16
S
M
2010-12-30
1
3
21
1
15
56920285
adventure-works\sharon0
0x5AE7
3
Design Engineer
1961-05-02
M
F
2011-01-18
1
4
22
1
16
24756624
adventure-works\david0
0x68
1
Marketing Manager
1975-03-19
S
M
2007-12-20
1
40
40
1
17
253022876
adventure-works\kevin0
0x6AC0
2
Marketing Assistant
1987-05-03
S
M
2007-01-26
0
42
41
1
Showing all 290 rows.

df4 = spark.read.format("csv").option("infer Schema" , True) .option("header", True ) .option("sep",","). load("/FileStore/tables/Bronze/HumanResources_Department.csv")
#display(df)
#print(df.count())
df4.show(3)
+------------+-----------+--------------------+--------------------+
|DepartmentID|       Name|           GroupName|        ModifiedDate|
+------------+-----------+--------------------+--------------------+
|           1|Engineering|Research and Deve...|2008-04-30 00:00:...|
|           2|Tool Design|Research and Deve...|2008-04-30 00:00:...|
|           3|      Sales| Sales and Marketing|2008-04-30 00:00:...|
+------------+-----------+--------------------+--------------------+
only showing top 3 rows

spark.read.format("delta").load("/FileStore/tables/Silver")
 
display(df)
 
BusinessEntityID
NationalIDNumber
LoginID
OrganizationNode
OrganizationLevel
JobTitle
BirthDate
MaritalStatus
Gender
HireDate
SalariedFlag
VacationHours
SickLeaveHours
CurrentFlag
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
18
1
295847284
adventure-works\ken0
NULL
NULL
Chief Executive Officer
1969-01-29
S
M
2009-01-14
1
99
69
1
2
245797967
adventure-works\terri0
0x58
1
Vice President of Engineering
1971-08-01
S
F
2008-01-31
1
1
20
1
3
509647174
adventure-works\roberto0
0x5AC0
2
Engineering Manager
1974-11-12
M
M
2007-11-11
1
2
21
1
4
112457891
adventure-works\rob0
0x5AD6
3
Senior Tool Designer
1974-12-23
S
M
2007-12-05
0
48
80
1
5
695256908
adventure-works\gail0
0x5ADA
3
Design Engineer
1952-09-27
M
F
2008-01-06
1
5
22
1
6
998320692
adventure-works\jossef0
0x5ADE
3
Design Engineer
1959-03-11
M
M
2008-01-24
1
6
23
1
7
134969118
adventure-works\dylan0
0x5AE1
3
Research and Development Manager
1987-02-24
M
M
2009-02-08
1
61
50
1
8
811994146
adventure-works\diane1
0x5AE158
4
Research and Development Engineer
1986-06-05
S
F
2008-12-29
1
62
51
1
9
658797903
adventure-works\gigi0
0x5AE168
4
Research and Development Engineer
1979-01-21
M
F
2009-01-16
1
63
51
1
10
879342154
adventure-works\michael6
0x5AE178
4
Research and Development Manager
1984-11-30
M
M
2009-05-03
1
16
64
1
11
974026903
adventure-works\ovidiu0
0x5AE3
3
Senior Tool Designer
1978-01-17
S
M
2010-12-05
0
7
23
1
12
480168528
adventure-works\thierry0
0x5AE358
4
Tool Designer
1959-07-29
M
M
2007-12-11
0
9
24
1
13
486228782
adventure-works\janice0
0x5AE368
4
Tool Designer
1989-05-28
M
F
2010-12-23
0
8
24
1
14
42487730
adventure-works\michael8
0x5AE5
3
Senior Design Engineer
1979-06-16
S
M
2010-12-30
1
3
21
1
15
56920285
adventure-works\sharon0
0x5AE7
3
Design Engineer
1961-05-02
M
F
2011-01-18
1
4
22
1
16
24756624
adventure-works\david0
0x68
1
Marketing Manager
1975-03-19
S
M
2007-12-20
1
40
40
1
17
253022876
adventure-works\kevin0
0x6AC0
2
Marketing Assistant
1987-05-03
S
M
2007-01-26
0
42
41
1
18
222969461
adventure-works\john5
0x6B40
2
Marketing Specialist
1978-03-06
S
M
2011-02-07
0
48
44
1
Showing all 290 rows.

# Auto Optimize
 
%sql
 set spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite = true;
 
key
value
1
spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite
true
Showing all 1 rows.

#%fs rm -r dbfs:("/FileStore/tables/Silver/part-00000-18b45f5f-7bcb-4fa7-8b16-9e5793e9134c-c000.snappy.parquet")
 
#dbutils.fs.rm('/FileStore/tables/Silver/part-00000-2147ffad-94ea-4022-84a6-39f0a33d9460-c000.snappy.parquet', True)
 
#/dbfs/FileStore /dbfs/FileStore/tables/Silver/_delta_log
 
dbutils.fs.rm('/FileStore/tables/Silver/_delta_log', True)
Out[109]: True
# Create lobal TempView
 
 
df.createOrReplaceGlobalTempView('HumanResources_Employee.csv')
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
 
where JobTitle in ('Senior Tool Designer','Tool Designer') and SUBSTRING(LoginID, 17, 100) = 'rob0'
 
 
 
""")
 
 
display(data_test)
#3.show(truncate=False)
 
Login
HireDate
JobTitle
BirthDate
MaritalStatus
Gender
1
rob0
2007-12-05
Senior Tool Designer
1974-12-23
Single
Male
Showing all 1 rows.

#df.write.option("path", "/FileStore/tables/Silver").saveAsTable("HumanResources_Employee")    
# Save Table with overwrite mode
 
data_test.write.mode("overwrite").format("delta").option("path", "/FileStore/tables/Silver").saveAsTable("HumanResources_Employee")
# Save Table with Append mode
 
#data_test.write.mode("Append").format("delta").option("path", "/FileStore/tables/Silver").saveAsTable("HumanResources_Employee")
spark.read.format("delta").load("/FileStore/tables/Silver")
 
display(spark.read.format("delta").load("/FileStore/tables/Silver"))
 
Login
HireDate
JobTitle
BirthDate
MaritalStatus
Gender
1
rob0
2007-12-05
Senior Tool Designer
1974-12-23
Single
Male
Showing all 1 rows.
