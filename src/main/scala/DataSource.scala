import io.prediction.controller.{EmptyActualResult, EmptyEvaluationInfo, PDataSource}
import org.apache.spark.SparkContext
import java.sql.{DriverManager, ResultSet, Statement}

import io.prediction.data.storage.Storage

import scala.reflect.ClassTag
import org.apache.spark.rdd.{JdbcRDD, RDD}

import scala.util.{Success, Try}

class DataSource extends PDataSource[TrainingData, EmptyEvaluationInfo, Query, EmptyActualResult] {
  override def readTraining(sc: SparkContext): TrainingData = {
    val query = s"SELECT property__c, user__c FROM ${DataSource.schema}.favorite__c WHERE ? <= id AND id <= ?"

    def mapRow(row: ResultSet) = Favorite(row.getString("property__c"), row.getString("user__c"))

    TrainingData(DataSource.jdbcRDD(sc, query, mapRow))
  }
}

object DataSource {
  def conn() = {
    val config = Storage.getConfig("PGSQL").get

    DriverManager.getConnection(
      config.properties("URL"),
      config.properties("USERNAME"),
      config.properties("PASSWORD"))
  }

  def jdbcRDD[T: ClassTag](sc: SparkContext, query: String, mapRow: ResultSet => T): RDD[T] = {
    new JdbcRDD(sc, conn, query, Long.MinValue, Long.MaxValue, 1, mapRow).cache()
  }

  def withStatement[A](f: Statement => A): Try[A] = {
    Try(f(conn().createStatement()))
  }

  lazy val schema = {
    val theSchema = withStatement(_.executeQuery("SELECT count(id) FROM salesforce.favorite__c")) match {
      case s: Success[ResultSet] => "salesforce"
      case _ => "public"
    }

    if (theSchema == "public") { loadDemoData() }

    theSchema
  }

  def loadDemoData() = {
    withStatement(_.executeQuery(
      """CREATE TABLE IF NOT EXISTS favorite__c (
        |  id SERIAL NOT NULL,
        |  property__c CHARACTER VARYING(18),
        |  user__c CHARACTER VARYING(18)
        |)
      """.stripMargin))


    withStatement(_.executeQuery("INSERT INTO favorite__c (property__c, user__c) VALUES ('0032800000huZ5TAAU', '0032800000jNrKsAAK')"))
    withStatement(_.executeQuery("INSERT INTO favorite__c (property__c, user__c) VALUES ('0032800000huZ5TAAU', '0032800000jNuppAAC')"))
    withStatement(_.executeQuery("INSERT INTO favorite__c (property__c, user__c) VALUES ('0032800000huZ5TAAU', '0032800000jNvaHAAS')"))
    withStatement(_.executeQuery("INSERT INTO favorite__c (property__c, user__c) VALUES ('0032800000huYirAAE', '0032800000jNuppAAC')"))
    withStatement(_.executeQuery("INSERT INTO favorite__c (property__c, user__c) VALUES ('0032800000huYrSAAU', '0032800000jNuppAAC')"))
    withStatement(_.executeQuery("INSERT INTO favorite__c (property__c, user__c) VALUES ('0032800000hNPEkAAO', '0032800000jNuppAAC')"))
    withStatement(_.executeQuery("INSERT INTO favorite__c (property__c, user__c) VALUES ('0032800000huYuRAAU', '0032800000jNvaHAAS')"))
    withStatement(_.executeQuery("INSERT INTO favorite__c (property__c, user__c) VALUES ('0032800000huYc0AAE', '0032800000jNvaHAAS')"))
    withStatement(_.executeQuery("INSERT INTO favorite__c (property__c, user__c) VALUES ('0032800000huYepAAE', '0032800000jNvaHAAS')"))
  }

}

case class Favorite(propertyId: String, userId: String)

case class TrainingData(favorites: RDD[Favorite])
