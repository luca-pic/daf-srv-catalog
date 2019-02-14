package it.gov.daf.catalogmanager.nifi

import com.google.inject.{Inject, Singleton}
import it.gov.daf.common.config.ConfigReadException
import it.gov.daf.config.NifiConfig
import it.gov.daf.model.Error
import play.api.libs.json._
import play.api.libs.ws.{WSClient, WSResponse}
import play.api.{Configuration, Logger}

import scala.concurrent.Future
import scala.util.{Failure, Success}
import scala.concurrent.ExecutionContext.Implicits._

@Singleton
class NifiRepository @Inject()(implicit ws :WSClient, configuration: Configuration){

  type DafSuccess = it.gov.daf.model.Success

  val nifiConfigConfig: NifiConfig = NifiConfig.reader.read(configuration) match {
    case Failure(error)  => throw ConfigReadException(s"Unable to read [kafka proxy config]", error)
    case Success(config) => config
  }

  private val NIFIURL = s"${nifiConfigConfig.host}:${nifiConfigConfig.port}"

  def startDerivedProcessor(orgName: String, feedName: String): Future[Either[Error, DafSuccess]] ={
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

  private def callProcessor(id: String, clientId: String, version: Int, state: String): Future[Either[Error, DafSuccess]] = {
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
          case 200 => Right(it.gov.daf.model.Success(res.statusText, None))
          case _   => Left(Error(Some(res.status), res.statusText, None))
        }
      }
  }
}
