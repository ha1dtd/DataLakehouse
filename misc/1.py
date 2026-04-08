from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import * # col, from_json, split, when, avg
from pyspark.sql.types import * # StructType, StructField
import os
from datetime import datetime, date
import pandas as pd
spark = SparkSession.builder.appName("demo").getOrCreate()





df_parquet = spark.read.parquet('part-00000-f6863d4c-8589-4671-a389-b6b95eb06f5e-c000.snappy.parquet') # inferschema, header and multiline do not help.
df_parquet.printSchema()
df_parquet.limit(15).show()
#print(df_parquet.count())
'''
    SELECT COLS
'''
# df_sel = df_parquet.select(col('dispatching_base_num'),col('base_passenger_fare'),col('tips')).show() # select columns
'''
    ALIAS (RENAMING)
'''
#df_parquet.select(col('hvfhs_license_num').alias('hvfhs_license')).show() #select and rename
#df_renamed = df_parquet.withColumnRenamed('hvfhs_license_num','hvfhs_license').show() #rename only
'''
    FILTER/WHERE
'''
#df_parquet.filter(col('tips')>0.0).show()
#df_parquet.filter(col('wav_match_flag')=='Y').show()
#df_parquet.filter((col('tips')>0.0) & (col('wav_match_flag')=='Y')).show()
#df_parquet.filter((col('originating_base_num').isNull() & (col('tips').isin(0.0,3.0)))).show() 

'''
    WITHCOLUMN
'''
#df_parquet.withColumn('const_flag',lit('Y')).show() # lit = literal   create a constant value col
#df_parquet.withColumn('totals',col('base_passenger_fare')+col('tips')+col('tolls')-col('sales_tax')-col('driver_pay')).show() # calculated and create new col
#df_parquet.withColumn('wav_match_flag',regexp_replace(col('wav_match_flag'),"Y","Yes"))\
#            .withColumn('wav_match_flag',regexp_replace(col('wav_match_flag'),"N","No")).show() # transform values in a column

'''
    TYPECASTING
'''
#df = df_parquet.withColumn('airport_fee', col('airport_fee').cast(StringType())).show()
#df.printSchema()  
'''
    SORTING
'''
#df_parquet.sort(col('tips')).show()
#df_parquet.sort(col('tips').desc()).show() 
#df_parquet.sort(col('tips').asc()).show() 
#df_parquet.sort(col('base_passenger_fare').asc()).show()
#df_parquet.sort(['tips','base_passenger_fare'],ascending = [0,0]).show() #sort multiple columns, perform descending twice on the two columns in that order, the list represents yes,no for both columns
#df_parquet.sort(['tips','base_passenger_fare'],ascending = [0,1]).show() # now this sorts descending on tips, then ascending on base passenger fare

'''
    LIMITING
'''
#df_parquet.limit(10).show() # you can sort your data and then limit to show the top or bottom values in the dataset.
'''
    DROP
'''
#df_parquet.drop('tolls','dispatching_base_num').show() # dropping columns
#df_parquet.dropDuplicates() # don't show, a bit taxing for memory
#df_parquet.drop_duplicates(subset=['cbd_congestion_fee']).show() # drop according to a column

'''
    UNION and UNION BY NAME
'''
# data1 = [('kad','1'),
#         ('sid','2')]
# schema1 = 'name STRING,id STRING'

# df1 = spark.createDataFrame(data1,schema1)

# data2 = [('3','rahul'),
#         ('4','jas')]
# schema2 = 'id STRING, name STRING'

# df2 = spark.createDataFrame(data2,schema2)

# df1.union(df2).show() # can cause smearing of data
# df1.unionByName(df2).show() #union with column names in mind
'''
    STRING FUNCTIONS
'''
# Initcap:
# df_parquet.select(initcap("hvfhs_license_num")).show() # capitalize the first letter of every word in this col
# df_parquet.select(upper("hvfhs_license_num")).show() # same with lower
'''
    DATE FUNCTIONS
'''
# df = df_parquet.withColumn("curr_date",current_date())
# df.show()
# df = df.withColumn("week_after",date_add('curr_date',7)) # adds days, same thing with date_sub
# df.show()
# df = df.withColumn("datediff",datediff('curr_date','week_after'))
# df.show()
# df = df.withColumn('week_after',date_format('week_after','dd-MM-yyyy'))
# df.show()
'''
    NULL HANDLING
'''
# df_parquet.dropna('all').show() # drops rows that is all NULL -> NULL NULL NULL NULL
# df_parquet.dropna('any').show() # drops rows that contains any amount of NULLs ->       2 3 NULL 5    or   NULL 4 1 5
# df_parquet.dropna(subset=['originating_base_num']).show() # drops rows where originating base num col is NULL

# # Filling Nulls:

# df_parquet.fillna('NotAvailable').show()
# df_parquet.fillna('NotAvailable',subset=['originating_base_num']).show()

'''
    SPLIT and INDEXING
'''
# df_parquet.withColumn('hvfhs_license_num',split('hvfhs_license_num','V')[0]).show() # split into a list by spaces and indexes 
# # in this case hv0003 -> [h,0003] -> h

# # explode:
# df_exp = df_parquet.withColumn('hvfhs_license_num',split('hvfhs_license_num','V'))
# df_exp.withColumn('hvfhs_license_num',explode('hvfhs_license_num')).show()
# # [h,0003] -> h    0003

# #finding string in array
# df_exp.withColumn('hvfhs_0003_flag',array_contains('hvfhs_license_num',"0003")).show()

'''
    GROUP BY
'''
# grouping aggregate functions
# df_parquet.groupBy('hvfhs_license_num').agg(sum('tips')).show()
# df_parquet.groupBy('hvfhs_license_num').agg(avg('tips')).show()
# df_parquet.groupBy('hvfhs_license_num','originating_base_num').agg(avg('tips').alias('avg_tips_scenarios')).show()
# df_parquet.groupBy('hvfhs_license_num','originating_base_num').agg(sum('tips'),avg('tips')).show()

'''
    COLLECT LIST
'''
# data = [('user1','book1'),
#         ('user1','book2'),
#         ('user2','book2'),
#         ('user2','book4'),
#         ('user3','book1')]
# schema = 'user string, book string'

# df_book = spark.createDataFrame(data,schema)

# df_book.show()

# df_book.groupBy('user').agg(collect_list('book')).show() # grouping by user and creating a list from the collected items 
'''
    PIVOT
'''
# grouped by hvfhs license num.
# pivoting, using the different values of originating_base_num as the new cols,
# then calculate the aggregated average of tips.
# df_parquet.groupBy('hvfhs_license_num').pivot('originating_base_num').agg(avg('tips')).show()
'''
    WHEN OTHERWISE
'''
# df_parquet.withColumn('hvfhs_flag',when(col('hvfhs_license_num')=='HV0003','003').otherwise('not_003')).show()

# df_parquet.withColumn('hvfhs_flag',when((col('hvfhs_license_num')=='HV0003') & (col('tips')<2.0),'cheap_riders_of_003')\
#                                 .when((col('hvfhs_license_num')=='HV0003') & (col('tips')>2.0),'exp_riders_of_003')\
#                                    .otherwise('neutral')).show()
'''
    JOINS
'''

# dataj1 = [('1', 'gaur', 'd01'),
#         ('2','kit','d02'),
#         ('3', ' sam', 'd03'),
#         ('4','tim','d03'),
#         ('5', 'aman', 'd05' ),
#         ('6', 'nad', 'd06' )]

# schemaj1 = 'emp_id STRING, emp_name STRING, dept_id STRING'
# df1 = spark.createDataFrame(dataj1, schemaj1)
# dataj2 = [('d01', 'HR'),
#         ('d02', 'Marketing'), 
#         ('d03', 'Accounts'),
#         ('d04', 'IT'),
#         ('d05', 'Finance')]

# schemaj2 = 'dept_id STRING, department STRING'
# df2 = spark.createDataFrame(dataj2, schemaj2)

# schemaj1 = 'emp_id STRING, emp_name STRING, dept_id STRING'

# df1.join(df2,df1['dept_id']==df2['dept_id'],'inner').show() #inner join
# df1.join(df2,df1['dept_id']==df2['dept_id'],'left').show() #left join
# df1.join(df2,df1['dept_id']==df2['dept_id'],'right').show() #right join
# df1.join(df2,df1['dept_id']==df2['dept_id'],'anti').show() #anti join

'''
    WINDOW FUNCTIONS
'''
#from pyspark.sql.window import Window

# df_parquet.withColumn('rowCol',row_number().over(Window.orderBy(col('tips')))).show() #orders by tips then numbers the rows.

# df_parquet.withColumn('rank',rank().over(Window.orderBy(col('tips').desc())))\
#         .withColumn('denserank',dense_rank().over(Window.orderBy(col('tips').desc()))).show()

'''
    CUMULATIVE SUM
'''
# df_parquet.withColumn('cumsum',sum('tips').over(Window.orderBy('tips').rowsBetween(Window.unboundedPreceding,Window.currentRow))).show() # cumulative sum in column 
# also check unboundedFollowing as well.


