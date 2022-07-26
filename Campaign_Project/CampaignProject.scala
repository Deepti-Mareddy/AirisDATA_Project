import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object CampaignProject extends App {

  val campaignSchema = new StructType()
    .add("Search_Item",StringType,nullable = false)
    .add("Match_Type",StringType,nullable = false)
    .add("Include_Exclude",StringType,nullable = false)
    .add("Campaign_Name",StringType,nullable = false)
    .add("Group",StringType,nullable = false)
    .add("Displayed",IntegerType,nullable = false)
    .add("Clicks",IntegerType,nullable = false)
    .add("Interaction_Rate",FloatType,nullable = false)
    .add("Currency_Code",StringType,nullable = false)
    .add("Per_Click_Rate",FloatType,nullable = false)
    .add("Amount_Paid",DoubleType,nullable = false)
    .add("Conversion",IntegerType,nullable = false)
    .add("No_Conversion",IntegerType,nullable = false)
    .add("%Conversion",DoubleType,nullable = false)
    .add("Identity",StringType,nullable = false)

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name","session demo")
  sparkConf.set("spark.master","local[2]")

  val spark = SparkSession.builder()
    .config(sparkConf)
    //.enableHiveSupport()
    .getOrCreate()

  // Reading Campaign_Dataset file
  val df = spark.read
    .format("csv")
    .schema(campaignSchema)
    .option("header",true)
    .option("path","C:\\Users\\deept\\Desktop\\AirisDATA - Project\\1.Campaign_Project\\Campaign_Dataset.csv")
    .load()

  df.printSchema()
  df.show()

  // Converting the Search_Item column into an array of the present keywords
  val df2 = df.select(col("Amount_Paid"),split(col("Search_Item")," ").alias("Item_Array"))
  df2.printSchema()
  df2.show(false)

  // Exploding Item_Array
  df2.createOrReplaceTempView("explodeTable")
  val df3 = spark.sql("select Each_Item,Amount_Paid from explodeTable lateral view explode(Item_Array) t as Each_Item")
  df3.show(false)

  //calculating the total amount to be paid for each keyword
  df3.createOrReplaceTempView("Campaign")
  val df4 = spark.sql("select Each_Item,sum(Amount_Paid) as Total from Campaign group by Each_Item")
  df4.show(false)

  // Displaying keyword for which the maximum amount to be paid
  df4.createOrReplaceTempView("Campaign_max")
  val df5 = spark.sql("select Each_Item,Total from Campaign_max where total =(select max(total) from Campaign_max)")
  df5.show(false)

  // To find amount paid for the custom keyword

  //Reading the custom keyword
  print("Enter the key word you want to search : ")
  var search_key = scala.io.StdIn.readLine()

  // Amount to be paid for custom keyword
  val df6 = spark.sql(s"select Each_Item,Total from Campaign_max where Each_Item like '${search_key}'")
  df6.show(false)

  df4.write
    .format("csv")
    .option("path","C:\\Users\\deept\\Desktop\\AirisDATA - Project\\1.Campaign_Project\\Campaign_write")
    .mode("overwrite")
    .save()
}
