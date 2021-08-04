package graph

import org.apache.spark.sql.types._
import io.prophecy.libs._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import config.ConfigStore._
import udfs.UDFs._
import udfs._
import graph._

@Visual(id = "Aggregate0", label = "Aggregate0", x = 707, y = 200, phase = 0)
object Aggregate0 {

  def apply(spark: SparkSession, in: DataFrame): Aggregate = {
    import spark.implicits._

    val dfGroupBy = in.groupBy(
      col("first_name").as("first_name"),
      col("last_name").as("last_name"),
      col("phone").as("phone"),
      col("customer_id").as("customer_id"),
      col("full_name").as("full_name")
    )
    val out = dfGroupBy.agg(count(col("order_id")).as("orders"))

    out

  }

}
