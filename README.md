databricks-logoProject_1(Python)


# Import Libraries 
2
 
3
from pyspark.sql import SparkSession
4
from pyspark.sql.functions import *
5
from pyspark.sql.types import *
6
from pyspark.sql import *
Command took 0.10 seconds -- by usaferreiratiago@gmail.com at 8/31/2022, 9:14:56 AM on study-databricks9
1
#dbutils.fs.ls("/FileStore/tables/Bronze/")
2
 
3
#display(dbutils.fs.ls("/FileStore/tables/Bronze/"))
Command took 0.12 seconds -- by usaferreiratiago@gmail.com at 8/31/2022, 9:15:02 AM on study-databricks9
1
# Create SparkSession
2
# Working in the Table Sales Customer
3
 
4
spark = (
5
    SparkSession.builder
6
    .master('local')
7
    .appName('Project_1_Sales_Customer')
8
    .getOrCreate()
9
)
Command took 0.16 seconds -- by usaferreiratiago@gmail.com at 8/31/2022, 9:15:03 AM on study-databricks9
1
# Count of Rows
2
print("Reading Data Sales Customer")
3
Sales_Customerdf = spark.read.format("csv").option("inferSchema" , True) .option("header", True ) .option("sep",","). load("/FileStore/tables/Bronze/Sales_Customer.csv")
4
#display(Sales_Customerdf)
5
#print(df.count())
6
Sales_Customerdf.count()
(4) Spark Jobs
Sales_Customerdf:pyspark.sql.dataframe.DataFrame = [CustomerID: integer, PersonID: string ... 5 more fields]
Reading Data Sales Customer
Out[71]: 19820
Command took 3.20 seconds -- by usaferreiratiago@gmail.com at 8/31/2022, 9:15:04 AM on study-databricks9
1
# Read Files "csv"
2
 
3
Sales_Customerdf = spark.read.format("csv").option("inferSchema" , True) .option("header", True ) .option("sep",","). load("/FileStore/tables/Bronze/Sales_Customer.csv")
4
#display(Sales_Customerdf)
5
#print(df.count())
6
Sales_Customerdf.show(10)
(3) Spark Jobs
Sales_Customerdf:pyspark.sql.dataframe.DataFrame = [CustomerID: integer, PersonID: string ... 5 more fields]
+----------+--------+-------+-----------+-------------+--------------------+--------------------+
|CustomerID|PersonID|StoreID|TerritoryID|AccountNumber|             rowguid|        ModifiedDate|
+----------+--------+-------+-----------+-------------+--------------------+--------------------+
|         1|    NULL|    934|          1|   AW00000001|3F5AE95E-B87D-4AE...|2014-09-12 11:15:...|
|         2|    NULL|   1028|          1|   AW00000002|E552F657-A9AF-4A7...|2014-09-12 11:15:...|
|         3|    NULL|    642|          4|   AW00000003|130774B1-DB21-4EF...|2014-09-12 11:15:...|
|         4|    NULL|    932|          4|   AW00000004|FF862851-1DAA-404...|2014-09-12 11:15:...|
|         5|    NULL|   1026|          4|   AW00000005|83905BDC-6F5E-4F7...|2014-09-12 11:15:...|
|         6|    NULL|    644|          4|   AW00000006|1A92DF88-BFA2-467...|2014-09-12 11:15:...|
|         7|    NULL|    930|          1|   AW00000007|03E9273E-B193-448...|2014-09-12 11:15:...|
|         8|    NULL|   1024|          5|   AW00000008|801368B1-4323-4BF...|2014-09-12 11:15:...|
|         9|    NULL|    620|          5|   AW00000009|B900BB7F-23C3-481...|2014-09-12 11:15:...|
|        10|    NULL|    928|          6|   AW00000010|CDB6698D-2FF1-4FB...|2014-09-12 11:15:...|
+----------+--------+-------+-----------+-------------+--------------------+--------------------+
only showing top 10 rows

Command took 2.21 seconds -- by usaferreiratiago@gmail.com at 8/31/2022, 9:15:05 AM on study-databricks9
1
# Check Nulls
2
 
3
Sales_Customerdf.toPandas().isna().sum()
(1) Spark Jobs
Out[73]: CustomerID       0
PersonID         0
StoreID          0
TerritoryID      0
AccountNumber    0
rowguid          0
ModifiedDate     0
dtype: int64
Command took 0.89 seconds -- by usaferreiratiago@gmail.com at 8/31/2022, 9:15:06 AM on study-databricks9
1
Sales_Customerdf.columns
Out[74]: ['CustomerID',
 'PersonID',
 'StoreID',
 'TerritoryID',
 'AccountNumber',
 'rowguid',
 'ModifiedDate']
Command took 0.09 seconds -- by usaferreiratiago@gmail.com at 8/31/2022, 9:15:08 AM on study-databricks9
1
# Check Schema
2
 
3
Sales_Customerdf.printSchema()
root
 |-- CustomerID: integer (nullable = true)
 |-- PersonID: string (nullable = true)
 |-- StoreID: string (nullable = true)
 |-- TerritoryID: integer (nullable = true)
 |-- AccountNumber: string (nullable = true)
 |-- rowguid: string (nullable = true)
 |-- ModifiedDate: timestamp (nullable = true)

Command took 0.08 seconds -- by usaferreiratiago@gmail.com at 8/31/2022, 9:15:13 AM on study-databricks9
1
# Alter type of Metada of the Column
2
 
3
Sales_Customerdf.withColumn('StoreID', col('StoreID').cast(IntegerType())).show(10)
(1) Spark Jobs
+----------+--------+-------+-----------+-------------+--------------------+--------------------+
|CustomerID|PersonID|StoreID|TerritoryID|AccountNumber|             rowguid|        ModifiedDate|
+----------+--------+-------+-----------+-------------+--------------------+--------------------+
|         1|    NULL|    934|          1|   AW00000001|3F5AE95E-B87D-4AE...|2014-09-12 11:15:...|
|         2|    NULL|   1028|          1|   AW00000002|E552F657-A9AF-4A7...|2014-09-12 11:15:...|
|         3|    NULL|    642|          4|   AW00000003|130774B1-DB21-4EF...|2014-09-12 11:15:...|
|         4|    NULL|    932|          4|   AW00000004|FF862851-1DAA-404...|2014-09-12 11:15:...|
|         5|    NULL|   1026|          4|   AW00000005|83905BDC-6F5E-4F7...|2014-09-12 11:15:...|
|         6|    NULL|    644|          4|   AW00000006|1A92DF88-BFA2-467...|2014-09-12 11:15:...|
|         7|    NULL|    930|          1|   AW00000007|03E9273E-B193-448...|2014-09-12 11:15:...|
|         8|    NULL|   1024|          5|   AW00000008|801368B1-4323-4BF...|2014-09-12 11:15:...|
|         9|    NULL|    620|          5|   AW00000009|B900BB7F-23C3-481...|2014-09-12 11:15:...|
|        10|    NULL|    928|          6|   AW00000010|CDB6698D-2FF1-4FB...|2014-09-12 11:15:...|
+----------+--------+-------+-----------+-------------+--------------------+--------------------+
only showing top 10 rows

Command took 0.63 seconds -- by usaferreiratiago@gmail.com at 8/31/2022, 9:15:14 AM on study-databricks9
1
Sales_Customerdf.printSchema()
root
 |-- CustomerID: integer (nullable = true)
 |-- PersonID: string (nullable = true)
 |-- StoreID: string (nullable = true)
 |-- TerritoryID: integer (nullable = true)
 |-- AccountNumber: string (nullable = true)
 |-- rowguid: string (nullable = true)
 |-- ModifiedDate: timestamp (nullable = true)

Command took 0.11 seconds -- by usaferreiratiago@gmail.com at 8/31/2022, 9:15:17 AM on study-databricks9
1
# Datatypes Supported
2
 
3
#ByteType    ByteType()    
4
#ShortType    ShortType()    
5
#IntegerType    IntegerType()
6
#LongType    LongType()    
7
#FloatType    FloatType()    
8
#DoubleType    DoubleType()
9
#DecimalType    DecimalType()
10
#StringType    StringType()
11
#BinaryType    BinaryType()
12
#BooleanType    BooleanType()
13
#TimestampType    TimestampType()
14
#DateType    DateType()
15
 
16
 
17
#ArrayType    ArrayType(elementType, [containsNull])
18
#MapType    MapType(keyType, valueType, [valueContainsNull])
19
#StructType    StructType(fields)
20
#StructField    StructField(name, dataType, [nullable])
21
 
22
 
Command took 0.08 seconds -- by usaferreiratiago@gmail.com at 8/31/2022, 9:15:19 AM on study-databricks9
1
# Alter type of Metada of the Column
2
 
3
Sales_Customerdf.select('CustomerID PersonID StoreID TerritoryID AccountNumber ModifiedDate'.split()).show(10)
4
 
5
 
6
#Sales_Customerdf.select('CustomerID PersonID StoreID TerritoryID AccountNumber ModifiedDate'.split()).show()
(1) Spark Jobs
+----------+--------+-------+-----------+-------------+--------------------+
|CustomerID|PersonID|StoreID|TerritoryID|AccountNumber|        ModifiedDate|
+----------+--------+-------+-----------+-------------+--------------------+
|         1|    NULL|    934|          1|   AW00000001|2014-09-12 11:15:...|
|         2|    NULL|   1028|          1|   AW00000002|2014-09-12 11:15:...|
|         3|    NULL|    642|          4|   AW00000003|2014-09-12 11:15:...|
|         4|    NULL|    932|          4|   AW00000004|2014-09-12 11:15:...|
|         5|    NULL|   1026|          4|   AW00000005|2014-09-12 11:15:...|
|         6|    NULL|    644|          4|   AW00000006|2014-09-12 11:15:...|
|         7|    NULL|    930|          1|   AW00000007|2014-09-12 11:15:...|
|         8|    NULL|   1024|          5|   AW00000008|2014-09-12 11:15:...|
|         9|    NULL|    620|          5|   AW00000009|2014-09-12 11:15:...|
|        10|    NULL|    928|          6|   AW00000010|2014-09-12 11:15:...|
+----------+--------+-------+-----------+-------------+--------------------+
only showing top 10 rows

Command took 0.71 seconds -- by usaferreiratiago@gmail.com at 8/31/2022, 9:15:22 AM on study-databricks9
1
# Create Global Temp View
2
 
3
Sales_Customerdf.createOrReplaceGlobalTempView('Sales_Customer.csv')
