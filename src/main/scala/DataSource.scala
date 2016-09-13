import io.prediction.controller.{EmptyActualResult, EmptyEvaluationInfo, PDataSource}
import org.apache.commons.httpclient.HttpClient
import org.apache.commons.httpclient.methods.GetMethod
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.json4s.JsonAST.{JArray, JField, JObject, JString}
import org.json4s.jackson.JsonMethods._


class DataSource extends PDataSource[TrainingData, EmptyEvaluationInfo, Query, EmptyActualResult] {

  override def readTraining(sc: SparkContext): TrainingData = {

    val dreamHouseWebAppUrl = sys.env("DREAMHOUSE_WEB_APP_URL")

    val httpClient = new HttpClient()

    val getFavorites = new GetMethod(dreamHouseWebAppUrl + "/favorite-all")

    httpClient.executeMethod(getFavorites)

    val json = parse(getFavorites.getResponseBodyAsStream)

    val favorites = for {
      JArray(favorites) <- json
      JObject(favorite) <- favorites
      JField("sfid", JString(propertyId)) <- favorite
      JField("favorite__c_user__c", JString(userId)) <- favorite
    } yield Favorite(propertyId, userId)

    val rdd = sc.parallelize[Favorite](favorites)

    TrainingData(rdd)
  }
}

case class Favorite(propertyId: String, userId: String)

case class TrainingData(favorites: RDD[Favorite])
