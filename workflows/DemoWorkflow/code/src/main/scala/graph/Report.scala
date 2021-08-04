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

@Visual(id = "Report", label = "Report", x = 866, y = 203, phase = 0)
object Report {

  @UsesDataset(id = "3356", version = 0)
  def apply(spark: SparkSession, in: DataFrame): Target = {
    import spark.implicits._

    Config.fabricName match {
      case "dev" =>
        val schemaArg = StructType(
          Array(
            StructField("first_name",  StringType,  false),
            StructField("last_name",   StringType,  false),
            StructField("phone",       StringType,  false),
            StructField("customer_id", IntegerType, false),
            StructField("full_name",   StringType,  false),
            StructField("orders",      LongType,    false)
          )
        )
        in.write
          .format("parquet")
          .mode("overwrite")
          .save("dbfs:/DatabricksSession/Report.parqq")
      case _ => throw new Exception("Unknown Fabric")
    }

  }

}
