package graph

import io.prophecy.libs._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import config.ConfigStore._
import udfs.UDFs._
import udfs._
import graph._

@Visual(id = "ByCustomerId", label = "ByCustomerId", x = 391, y = 200, phase = 0)
object ByCustomerId {

  def apply(spark: SparkSession, left: DataFrame, right: DataFrame): Join = {
    import spark.implicits._

    val leftAlias  = left.as("left")
    val rightAlias = right.as("right")
    val dfJoin     = leftAlias.join(rightAlias, col("left.customer_id") === col("right.customer_id"), "inner")

    val out = dfJoin.select(
      col("left.first_name").as("first_name"),
      col("left.last_name").as("last_name"),
      col("left.phone").as("phone"),
      col("right.order_id").as("order_id"),
      col("right.customer_id").as("customer_id"),
      col("right.order_status").as("order_status")
    )

    out

  }

}
