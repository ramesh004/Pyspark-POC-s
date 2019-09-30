
package simulateevent


import org.apache.spark.sql._
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._ // not necessary since Spark 1.3
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.sql.functions.from_unixtime
import org.apache.spark.sql.functions._

import org.apache.spark.sql.{SaveMode,SparkSession,DataFrame,SQLContext}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.types._
import java.util.regex.Pattern
import org.apache.hadoop.fs.{FileSystem,Path,FileUtil}
import org.apache.spark.rdd.RDD
import java.net.URI



object simulateevent extends App  {
  
System.setProperty("hadoop.home.dir", "C:\\winutils") 
val hadoopConfig = new org.apache.hadoop.conf.Configuration()


  val sparkSession = SparkSession.builder.master("local").getOrCreate()
 
//val lol = sparkSession.sparkContext.textFile("C:\\Users\\TR20064147\\Documents\\Book1.xlsx")

//lol.collect().foreach(println )
  
  
val datasetPAth ="D:\\system\\Dataset.xlsx"
  
val userDestPath="D:\\system\\input_file"

def readExcel(sheetName: String): DataFrame = sparkSession.read
    .format("com.crealytics.spark.excel")
    .option("location", datasetPAth)
    .option("useHeader", "true")
    .option("treatEmptyValuesAsNulls", "False")
    .option("inferSchema", "true")
    .option("addColorColumns", "False")
    .option("sheetName", sheetName)
    .load()
    
    

val demographic = readExcel("demographic")
val raw_data_1 = readExcel("raw_data_call_1")
val raw_data_2 = readExcel("raw_data_call_2")
val raw_data_3 = readExcel("raw_data_call_2")

//Collecting the distinct Users
 val distinctUser=demographic.select("user_id").distinct().rdd.map(f=>f.mkString).collect()

 
for (user <- distinctUser)
{
  //{ for Demographic file creation  
      val batchUser=demographic.filter( demographic.col("user_id").isin(user))
	    val kvCols = demographic.columns.flatMap(c => Seq(lit(c), col(c)))
	    val demo = batchUser.withColumn("data", map(kvCols: _*)).select("data")
	    val data=demo.withColumn("type",lit("demographic")).select("type","data")
			//batchUser.show()
			data.coalesce(1).write.json(userDestPath+"\\"+user+"\\temp") 
			localMErge(userDestPath+"\\"+user+"\\temp",userDestPath+"\\"+user+"\\demographic.json")
			
			//}
						
			
  //{ for call_log file creation  
			
			{

    	      val all_data=raw_data_1.unionAll(raw_data_2).unionAll(raw_data_3)  //.show()
    			  //all_data.show()
    			  val filteredUserInfo=all_data.filter( all_data.col("user_id").cast(IntegerType).isin(user))
    			  //filteredUserInfo.show()
    			  filteredUserInfo.registerTempTable("tbl")
    			  val dataDf=sparkSession.sql("""select user_id,from_unixtime(date, "y-MM-dd HH:mm:ss") as date,duration ,type_cat from tbl  """)
    			  val dropUserId = dataDf.drop("user_id")
    			  val cols = dropUserId.columns.flatMap(c => Seq(lit(c), col(c)))
    			  val demo1 = dataDf.withColumn("data", map(cols: _*)).select("user_id","data") 
    			  val lol3=demo1.groupBy("user_id").agg(collect_list("data").alias("data"))
    			  val finalDf= lol3.withColumn("type", lit("call_log")).selectExpr("type", "cast(user_id as Int)", "data")
    			  finalDf.coalesce(1).write.json(userDestPath+"\\"+user+"\\temp") 
    	    	localMErge(userDestPath+"\\"+user+"\\temp",userDestPath+"\\"+user+"\\call_log.json")

			}
}  
 

//Changing the name of the part file as demographic and call_logs

  def localMErge(srcPath:String,DestPath :String)={
  	implicit val fs = FileSystem.getLocal(new org.apache.hadoop.conf.Configuration())  
    FileUtil.copyMerge(fs, new Path(srcPath), fs, new Path(DestPath), true, hadoopConfig, null)			
  }




}