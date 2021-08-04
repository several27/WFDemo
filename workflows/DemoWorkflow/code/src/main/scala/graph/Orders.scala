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

@Visual(id = "Orders", label = "Orders", x = 190, y = 267, phase = 0)
object Orders {

  @UsesDataset(id = "3355", version = 0)
  def apply(spark: SparkSession): Source = {
    import spark.implicits._

    val out = Config.fabricName match {
      case "dev" =>
        val schemaArg = StructType(
          Array(
            StructField("order_id",       IntegerType, true),
            StructField("customer_id",    IntegerType, true),
            StructField("order_status",   StringType,  true),
            StructField("order_category", StringType,  true),
            StructField("order_date",     StringType,  true),
            StructField("amount",         DoubleType,  true)
          )
        )
        spark.read
          .format("csv")
          .option("header", true)
          .option("sep",    ",")
          .schema(schemaArg)
          .load("dbfs:/DatabricksSession/OrdersDatasetInput.csv")
          .cache()
      case _ => throw new Exception(s"The fabric is not handled")
    }

    out

  }

}
