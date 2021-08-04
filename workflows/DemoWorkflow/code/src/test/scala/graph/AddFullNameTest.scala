package graph

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import io.prophecy.libs.SparkTestingUtils._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._
import org.junit.runner.RunWith
import org.junit.Assert
import org.scalatest.FunSuite
import java.time.LocalDateTime
import org.scalatest.junit.JUnitRunner
import java.sql.{Date, Timestamp}

@RunWith(classOf[JUnitRunner])
class AddFullNameTest extends FunSuite with DataFrameSuiteBase {
  import sqlContext.implicits._

  test("Test Unit test 0 for out columns: full_name") {
    implicit def addDTMethods(s: String): StringColumnExtensions  = new StringColumnExtensions(s)

    val dfIn = inDf(Seq("first_name", "last_name"), Seq(
      Seq[Any]("Steve","Jobs")
    ))

    val dfOut = outDf(Seq("full_name"), Seq(
      Seq[Any]("Steve - Jobs")
    ))

    val dfOutComputed = graph.AddFullName(spark, dfIn)
    val res = assertDFEquals(dfOut.select("full_name"), dfOutComputed.select("full_name"), maxUnequalRowsToShow, 1.0)

    val msg = if (res.isLeft) res.left.get.getMessage else ""
    Assert.assertTrue(msg, res.isRight)
    
  }

  def inDf(columns: Seq[String], values: Seq[Seq[Any]]): DataFrame = {
    implicit def addDTMethods(s: String): StringColumnExtensions  = new StringColumnExtensions(s)

    // defaults for each column
    val defaults = Map[String, Any](
      "order_status" -> "",
      "first_name" -> "",
      "last_name" -> "",
      "customer_id" -> "",
      "phone" -> "",
      "order_id" -> ""
    )

    // column types for each column
    val columnToTypeMap = Map[String, DataType](
      "order_status" -> StringType,
      "first_name" -> StringType,
      "last_name" -> StringType,
      "customer_id" -> StringType,
      "phone" -> StringType,
      "order_id" -> StringType
    )

    createDF(spark, columns, values, defaults, columnToTypeMap, "in")
  }

  def outDf(columns: Seq[String], values: Seq[Seq[Any]]): DataFrame = {
    implicit def addDTMethods(s: String): StringColumnExtensions  = new StringColumnExtensions(s)

    // defaults for each column
    val defaults = Map[String, Any](
      "first_name" -> "",
      "last_name" -> "",
      "phone" -> "",
      "order_id" -> "",
      "customer_id" -> "",
      "order_status" -> "",
      "full_name" -> ""
    )

    // column types for each column
    val columnToTypeMap = Map[String, DataType](
      "first_name" -> StringType,
      "last_name" -> StringType,
      "phone" -> StringType,
      "order_id" -> StringType,
      "customer_id" -> StringType,
      "order_status" -> StringType,
      "full_name" -> StringType
    )

    createDF(spark, columns, values, defaults, columnToTypeMap, "out")
  }

  def assertPredicates(port: String, df: DataFrame, predicates: Seq[(Column, String)]): Unit = {
    predicates.foreach({
      case (pred, name) =>
        Assert.assertEquals(
          s"Predicate $name [[`$pred`]] not universally true for port $port",
          df.filter(pred).count(),
          df.count()
        )
    })
  }
}
