import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object MovieProject extends App {

  def profit_loss(budget:Long,gross:Long): String ={
    val res=gross-budget
    if (res>0) "Profit"
    else "Loss"
  }

  def review(Profit_or_Loss:String,score:Float): String={
    if(Profit_or_Loss=="Profit"){
      if(score>=7){
        "Blockbuster"
      }else if(score>=6){
        "Superhit"
      }else if(score>=5){
        "Hit"
      }else{
        "Average"
      }
    }
    else {
      if (score >= 4 ) {
        "Flop"
      } else {
        "Disaster"
      }
    }
  }

  val sparkConf=new SparkConf()
  sparkConf.setAppName("First App")
  sparkConf.setMaster("local[2]")

  val spark=SparkSession.builder()
    .config(sparkConf)
    //  .enableHiveSupport()
    .getOrCreate()

  val movieSchema = new StructType()
    .add("id",IntegerType,nullable = false)
    .add("name",StringType,nullable = false)
    .add("rating",StringType,nullable = false)
    .add("genre",StringType,nullable = false)
    .add("year",IntegerType,nullable = false)
    .add("released_date",DateType,nullable = false)
    .add("score",FloatType,nullable = false)
    .add("votes",IntegerType,nullable = false)
    .add("director",StringType,nullable = false)
    .add("writer",StringType,nullable = false)
    .add("star",StringType,nullable = false)
    .add("country",StringType,nullable = false)
    .add("budget",LongType,nullable = false)
    .add("gross",LongType,nullable = false)
    .add("company",StringType,nullable = false)
    .add("runtime",IntegerType,nullable = false)

  //Reading rental_data file
  val df_movies=spark.read
    .format("csv")
    .schema(movieSchema)
    .option("path","C:\\Users\\deept\\Desktop\\AirisDATA - Project\\movies_dataset.csv")
    .load()

  //Adding column names
  val df_columns_movies = df_movies.toDF("id","name","rating","genre","year","released_date","score","votes","director","writer","star","country","budget","gross","company","runtime")
  df_columns_movies.printSchema()
  df_columns_movies.show();

  //creating string expression udf for calculating profit_loss
  spark.udf.register("Profit_Loss",profit_loss(_:Long,_:Long):String)
  val df_movies_pl=df_columns_movies.withColumn("Profit_or_Loss",expr("Profit_Loss(budget,gross)"))

  //Finding review based on profit_loss and score
  spark.udf.register("Review",review(_:String,_:Float):String)
  val df_movies_r=df_movies_pl.withColumn("Review",expr("Review(Profit_or_Loss,score)"))
  df_movies_r.show(7615,false)

  //Display genres by average score in descending order
  val df_yearWiseGross=df_columns_movies
    .groupBy(col("year"))
    .agg(sum("gross"))
    .withColumnRenamed("sum(gross)","total gross")
    .orderBy(col("year"))
    .select("year","total gross")
  df_yearWiseGross.show();

  //Display top 3 gross films in each country
  df_columns_movies.createOrReplaceTempView("Movies")
  val df_final=spark.sql("select * from (select id,name,genre,country,gross,row_number() over (partition by country order by gross desc) r from Movies) t where r<=2;")
  df_final.show();

  //Writing the above dataFrames into file

  df_final.write
    .format("csv")
    .option("path","C:\\Users\\deept\\Desktop\\AirisDATA - Project\\write")
    .mode("overwrite")
    .save()

  df_yearWiseGross.write
    .format("csv")
    .option("path","C:\\Users\\deept\\Desktop\\AirisDATA - Project\\write2")
    .mode("overwrite")
    .save()

  df_movies_r.write
    .format("csv")
    .option("path","C:\\Users\\deept\\Desktop\\AirisDATA - Project\\write3")
    .mode("overwrite")
    .save()
}