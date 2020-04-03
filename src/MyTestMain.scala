import org.apache.spark._
import org.apache.spark.SparkContext  
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

object MyTestMain { 
   def main(args: Array[String]) { 
     Logger.getLogger("org").setLevel(Level.ERROR)
     Logger.getLogger("akka").setLevel(Level.ERROR)
     
   val conf = new SparkConf()
   .setAppName("WordCount").setMaster("local")

   val sc = new SparkContext(conf)
   val input = sc.textFile("files/names.txt") 
        sc.setLogLevel("WARN") 
        
   val spark = SparkSession.builder()
      .master("local[1]")
      .appName("SparkByExamples.com")
      .getOrCreate();

	val schema = sc.textFile("files/schema.txt").collect()
	
      val diamonds = spark.read.format("csv")
        .option("header", "false")
        .option("inferSchema", "true")
        .load("files/names.txt")
        
  diamonds.printSchema()
  
  val diamonds1 = diamonds.toDF(schema:_*)
  
  diamonds1.printSchema()
  
      System.out.println("OK"); 
   } 
} 