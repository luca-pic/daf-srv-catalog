package it.gov.daf.catalogmanager.nifi

import com.google.inject.{Inject, Singleton}
import catalog_manager.yaml.{Error, Success}
import play.api.inject.ConfigurationProvider
import play.api.libs.json._
import play.api.libs.ws.{WSClient, WSResponse}
import play.api.Logger

import scala.concurrent.Future

@Singleton
class Nifi @Inject()(ws :WSClient, config: ConfigurationProvider){

  import scala.concurrent.ExecutionContext.Implicits._

  private val NIFIURL = config.get.getString("nifi.url").get

  def startDerivedProcessor(orgName: String, feedName: String): Future[Either[Error, Success]] ={
    for {
      responseGetProcessors <- getProcessors
      id <- extractIdFromJson(responseGetProcessors, orgName, feedName)
      (clientId, version) <- getRevision(id)
      _ <- callProcessor(id, clientId, version, "stopped")
      _ <- callProcessor(id, clientId, version+1, "running")
      response <- callProcessor(id, clientId, version+2, "stopped")
    } yield response
  }

  private def parseJsValueToString(jsValue: JsValue): String = jsValue.toString().replaceAll("\\\"", "")

  private def getProcessors: Future[WSResponse] = {
    ws.url(s"$NIFIURL/nifi-api/flow/process-groups/root/status?recursive=true")
      .withHeaders("Content-type" -> "application/json")
      .get()
  }

  private def extractIdFromJson(responseNifi: WSResponse, orgName: String, feedName: String): Future[String] = {
    Logger.debug(s"nifi reponse for feed $feedName is ${responseNifi.statusText}")
    val orgJson: JsValue = (responseNifi.json \ "processGroupStatus" \ "aggregateSnapshot" \ "processGroupStatusSnapshots" \\ "processGroupStatusSnapshot")
      .filter(jsValue =>
        (jsValue \ "name").getOrElse(JsNull).toString().equals(s""""$orgName"""")
      ).head
    val feedJson: JsValue = (orgJson \ "processGroupStatusSnapshots" \\ "processGroupStatusSnapshot")
      .filter(jsValue =>
        (jsValue \ "name").getOrElse(JsNull).toString().equals(s""""$feedName"""")
      ).head
    val id: JsValue = (feedJson \ "processorStatusSnapshots" \\ "processorStatusSnapshot")
      .filter(jsValue => (jsValue \ "name").getOrElse(JsNull).toString().equals("\"Run Feed\"")).map(x => x \ "id")
      .head.getOrElse(JsNull)
    Logger.debug(s"id for $feedName is $id")
    Future.successful(parseJsValueToString(id))
  }

  private def getRevision(id: String): Future[(String, Int)] = {
    import java.util.UUID.randomUUID
    ws.url(s"$NIFIURL/nifi-api/processors/$id").get()
      .map{resp =>
        val json: JsLookupResult = resp.json \ "revision"
        Logger.debug(s"$id: revision{ $json }")
        (
          parseJsValueToString((json \ "clientId").getOrElse(JsString(randomUUID().toString))),
          parseJsValueToString((json \ "version").get).toInt
        )
      }
  }

  private def callProcessor(id: String, clientId: String, version: Int, state: String): Future[Either[Error, Success]] = {
    val json: JsValue = Json.parse(
      s"""
        |{
        |  "revision": {
        |    "clientId": "$clientId",
        |    "version": $version
        |  },
        |  "component": {
        |    "id": "$id",
        |    "state": "${state.toUpperCase}"
        |  }
        |}
      """.stripMargin
    )
    ws.url(s"$NIFIURL/nifi-api/processors/$id").withHeaders("Content-type" -> "application/json")
      .put(json)
      .map{ res =>
        Logger.debug(s"nifi response status $state: $res")
        res.status match {
        case 200 => Right(Success(res.statusText, None))
        case _   => Left(Error(res.statusText, Some(res.status), None))
      }
    }
  }
}
