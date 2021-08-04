import org.apache.spark.sql.types._
import io.prophecy.libs._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import config.ConfigStore._
import udfs.UDFs._
import udfs._

import graph._

@Visual(mode = "batch", interimMode = "full")
object Main {

  def graph(spark: SparkSession): Unit = {

    val df_Customers: Source = Customers(spark)

    val df_Orders:       Source    = Orders(spark)
    val df_ByCustomerId: Join      = ByCustomerId(spark, df_Customers, df_Orders)
    val df_AddFullName:  Reformat  = AddFullName(spark,  df_ByCustomerId)
    val df_Aggregate0:   Aggregate = Aggregate0(spark,   df_AddFullName)
    Report(spark, df_Aggregate0)

  }

  def main(args: Array[String]): Unit = {
    import config._
    ConfigStore.Config = ConfigurationFactoryImpl.fromCLI(args)

    val spark = SparkSession
      .builder()
      .appName("DemoWorkflow")
      .config("spark.sql.legacy.allowUntypedScalaUDF", "true")
      .enableHiveSupport()
      .getOrCreate()

    UDFs.registerUDFs(spark)
    UDAFs.registerUDAFs(spark)

    val sc = spark.sparkContext
    sc.setCheckpointDir("/tmp/checkpoints")

    graph(spark)
  }

}
