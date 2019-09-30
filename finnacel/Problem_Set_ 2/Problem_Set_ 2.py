'''
Created on 20-Jul-2019

@author: Ramesh 
'''

#Problem Set 2 

from pyspark import SparkContext, SparkConf
import pyspark
from pyspark.sql.functions import *
from pyspark.sql.types import *


if __name__ == "__main__":
    #Driver Program
    inputEcomJsonPath = 'D:\\ecom.json'
    outputCsvWritePath= "D:\\ecom.csv"
    conf = SparkConf().setAppName("count").setMaster("local[*]")
    sc = SparkContext(conf = conf)
    
    #WinUtils Binary
    sc.setSystemProperty("hadoop.home.dir", "C:\\winutils")
    #creating pySpark Context
    sqlContext = pyspark.SQLContext(sc)
    
    #Reading the Json into Dataframe
    data = sqlContext.read.option("multiline","true").json(inputEcomJsonPath)
    
    #creating Temp View
    data.createOrReplaceTempView("tbl")
    
    #SQl Script to structure the Json Data
    structuredData=sqlContext.sql("""select * from (
    select timestamp,user,creation_date,id,col.* from (
    select timestamp,user,creation_date,id, explode(transactions) as col from (
    select timestamp,user_id as user,ecom1.* from (
    select timestamp,user_id,data.* from (
    select lol.* from (
    Select lol from tbl lateral view explode(users) as lol )))))
    union all 
    select timestamp,user,creation_date,id,col.* from (
    select timestamp,user,creation_date,id, explode(transactions) as col from (
    select timestamp,user_id as user,ecom2.* from (
    select timestamp,user_id,data.* from (
    select lol.* from (
    Select lol from tbl lateral view explode(users) as lol )))))
    union all 
    select timestamp,user,creation_date,id,col.* from (
    select timestamp,user,creation_date,id, explode(transactions) as col from (
    select timestamp,user_id as user,ecom3.* from (
    select timestamp,user_id,data.* from (
    select lol.* from (
    Select lol from tbl lateral view explode(users) as lol )))))
    union all 
    select timestamp,user,creation_date,id,col.* from (
    select timestamp,user,creation_date,id, explode(transactions) as col from (
    select timestamp,user_id as user,ecom4.* from (
    select timestamp,user_id,data.* from (
    select lol.* from (
    Select lol from tbl lateral view explode(users) as lol )))))
    union all 
    select timestamp,user,creation_date,id,col.* from (
    select timestamp,user,creation_date,id, explode(transactions) as col from (
    select timestamp,user_id as user,ecom5.* from (
    select timestamp,user_id,data.* from (
    select lol.* from (
    Select lol from tbl lateral view explode(users) as lol ))))) )  """)
    
    #creating temp view with the structured Data
    structuredData.createOrReplaceTempView("structured_tbl")
    
    #taking succcess_txn_count,avg_txn_amt from the structured Data
    aggregatedData=sqlContext.sql("""select count(1) as succcess_txn_count,avg(cast (trim( replace(substr(transaction_amount,3),'.','')) as Int)) as avg_txn_amt from structured_tbl 
    where std_status='DELIVERED' and 
    (cast(transaction_date as timestamp) between cast(date_sub(cast('2018-06-22 14:55:13' as timestamp), 90) || ' 14:55:13' as timestamp ) 
    and cast('2018-06-22 14:55:13' as timestamp))""")
    
    print("Problem set2 ==> 1.(a),1.(b)")
    aggregatedData.show()
    
    
    #Taking txn_length from the structured data and filtering  between (min(Txn_date) to the Submitted_date)
    len_txn=sqlContext.sql("select max(transaction_date) as max_txn_date from  structured_tbl where  cast(transaction_date as timestamp) between   (select min(cast(transaction_date as timestamp)) as mintime from  structured_tbl)  and    cast('2018-06-22 14:55:13' as timestamp)")
    print("Problem set2  ==> 1.(c)")
    len_txn.show()
   
    print("Problem set2 ==> 2")
    print("writing as Csv Started...")
    #Writing the structured Json into the CSV format
    structuredData.coalesce(1).write.csv(outputCsvWritePath)
    
    print("writing as Csv Finished.")


