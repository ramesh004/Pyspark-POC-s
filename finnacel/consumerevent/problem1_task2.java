
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
import org.apache.spark.rdd.RDD
import java.net.URI
import java.io.File



object obj1 extends App  {
  
System.setProperty("hadoop.home.dir", "C:\\winutils") 

 var previousFileArray = Array[File]()
    
 val sparkSession = SparkSession.builder.master("local").getOrCreate()
  
//val callLog =sparkSession.read.json("C:\\Users\\tamilselvan\\Downloads\\Data Engineer Test v3\\Data Engineer Test v3\\lol\\7\\call_log.json")

 val DirWatchPath = "C:\\Users\\tamilselvan\\Downloads\\Data Engineer Test v3\\Data Engineer Test v3\\lol"  
 val interval =3
 val outputCsvPath ="C:\\Users\\tamilselvan\\Downloads\\Data Engineer Test v3\\Data Engineer Test v3\\lol\\7\\ram"
 
def getListOfDirs(dir: String):Array[File] = {
    val d = new File(dir)
    if (d.exists && d.isDirectory) {
        d.listFiles.filterNot(_.isFile).toArray
    } else {
        Array[File]()
    }
}    
   

 
while(true)
{

	val listOfDir=   getListOfDirs(DirWatchPath)  //.foreach(println) 

			//listOfDir.foreach(println)

			for (dirPath <- listOfDir)
			{

				val needToProcess=  previousFileArray.filter(f=>f==dirPath)

						previousFileArray.foreach(println)

						System.err.println("Inside For Loop")
						if (needToProcess.isEmpty)
						{    
							System.err.println("Inside Isempty")
							val callLog =sparkSession.read.json(dirPath+"\\call_log.json")
							val demograph =sparkSession.read.json(dirPath+"\\demographic.json")


							//json1.show()

							callLog.registerTempTable("Call_Log")

							sparkSession.sql("select duration,date,type_cat,type,user_id from (select lol1.*,type,user_id from Call_Log lateral view explode(data) as lol1)").show()


							demograph.registerTempTable("demo_graph")

							//demograph.show()

							sparkSession.sql("select * from (select data.* from demo_graph)  d left join  (select duration,date,type_cat,type,user_id from (select lol1.*,type,user_id from Call_Log lateral view explode(data) as lol1))  l on (d.user_id=l.user_id)   ").show()


							val preFinalData=sparkSession.sql("""select user_id,d_age,age_Stmt as group_age,salary as group_salary,
									|  sum(outgoing_call) as outgoing_duration,sum(Incoming_call) as incoming_duration,count(missed_call) as num_missed_call,
									|  count(unknown) as num_unknown_call,current_timestamp from ( select d.user_id,d_age,case when cast(d_age as Int)>20 and cast(d_age as Int) <30 then 'group_1' 
									|  when cast(d_age as Int)>=30 and cast(d_age as Int) <40 then 'group_2' 
									|  when cast(d_age as Int)>=40 and cast(d_age as Int) <50 then 'group_3' else 'others' End as age_Stmt ,
									|  case when cast(d_monthly_salary as Int)<3000000  then 'group_1' 
									|  when cast(d_monthly_salary as Int)>=3000000 and cast(d_monthly_salary as Int) <10000000 then 'group_2' 
									|  when cast(d_monthly_salary as Int)>=10000000 and cast(d_monthly_salary as Int) <25000000 then 'group_3'
									|  when cast(d_monthly_salary as Int)>3000000  then 'group_4' End as salary,duration,type_cat,
									|  case when cast(type_cat as int) = 3 then duration else 0 end as outgoing_call,
									|  case when cast(type_cat as int) = 1 then duration else 0 end as Incoming_call,
									|  case when cast(type_cat as int) = 2 then 1 else 0 end as missed_call , 
									|  case when cast(type_cat as int) = 4 then 1 else 0 end as unknown  from (select data.* from demo_graph)  d 
									|  left join  (select duration,date,type_cat,type,user_id from 
									|  (select lol1.*,type,user_id from Call_Log lateral view explode(data) as lol1))  l on (d.user_id=l.user_id)  ) 
									|  group by user_id,d_age,age_Stmt,salary  """.stripMargin('|'))

							preFinalData.show(false)

							preFinalData.write.csv(outputCsvPath)

							previousFileArray +:= dirPath

						}

			}

	Thread.sleep(1000 * interval) 

}

System.exit(1)


}