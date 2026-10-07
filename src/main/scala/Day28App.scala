import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger

object Day28App {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Structured Streaming")
      .master("local[*]")
      .config("spark.ui.enabled", "false")
      .getOrCreate()

    println("===== DAY 28: STRUCTURED STREAMING =====")

    val input = spark.readStream
      .format("rate")
      .option("rowsPerSecond",5)
      .load()

    val processed = input
      .withColumn(
        "category",
        when(col("value") % 2 === 0,"EVEN")
          .otherwise("ODD")
      )
      .groupBy(
        window(col("timestamp"),"10 seconds"),
        col("category")
      )
      .count()

    val query = processed.writeStream
      .outputMode("complete")
      .format("console")
      .option("truncate","false")
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .start()

    query.awaitTermination(15)

    query.stop()
    spark.stop()
  }
}
