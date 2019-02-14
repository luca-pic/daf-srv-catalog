package it.gov.daf.catalogmanager.kylo

import play.api.libs.json.{JsObject, JsValue}
import it.gov.daf.model.{Error, MetaCatalog}
import play.api.libs.json._
import play.api.libs.ws.{WSAuthScheme, WSClient, WSResponse}
import com.google.inject.{Inject, Singleton}
import it.gov.daf.common.config.ConfigReadException
import it.gov.daf.config.KyloConfig
import it.gov.daf.model
import play.api.{Configuration, Logger}

import scala.util.{Failure, Success, Try}
import scala.concurrent.Future

@Singleton
class KyloRepository @Inject()(implicit configuration: Configuration, val ws: WSClient){

  type DafSuccess = it.gov.daf.model.Success
  val DafResponseSuccess: model.Success.type = it.gov.daf.model.Success

  implicit val kyloConfig: KyloConfig = KyloConfig.reader.read(configuration) match {
    case Failure(error) => throw ConfigReadException(s"Unable to read [kylo config]", error)
    case Success(config) => config
  }

  val KYLOURL = kyloConfig.url
  val KYLOUSER = kyloConfig.user
  val KYLOPWD = kyloConfig.userPwd
  val SFTPHOSTNAME = kyloConfig.sftHostName

  import scala.concurrent.ExecutionContext.Implicits._

  private def getFeedInfo(feedName: String, user: String): Future[Either[Error, String]] = {
    val futureResponseFeedInfo = ws.url(KYLOURL + "/api/v1/feedmgr/feeds/by-name/" + feedName)
      .withAuth(KYLOUSER, KYLOPWD, WSAuthScheme.BASIC)
      .get()

    val res = futureResponseFeedInfo.map{
      resp =>
        val json: JsValue = Try(resp.json).getOrElse(JsNull)
        val id: Option[JsValue] = Try{Some((json \ "feedId").get)}.getOrElse(None)
        id match {
          case Some(x) => Logger.logger.debug(s"$feedName: $x"); Right(x.toString().replace("\"", ""))
          case None    => Logger.logger.debug(s"id of $feedName not found"); Left(Error(Some(404), s"feed $feedName not found", None))
        }
    }
    res
  }

  def deleteFeed(feedName: String, user: String): Future[Either[Error, DafSuccess]] = {
    getFeedInfo(feedName, user).flatMap{
      case Right(idFeed) =>
        for{
          _ <- disableFeed(idFeed)
          res <- patchDelete(user, feedName, idFeed)
        } yield res
      case Left(left) => Future.successful(Right(DafResponseSuccess(left.message, None)))
    }
  }

  private def disableFeed(feedId: String): Future[Either[Error, DafSuccess]] = {
    val disbleFeedResponse: Future[WSResponse] = ws.url(KYLOURL + "/api/v1/feedmgr/feeds/disable/" + feedId)
      .withAuth(KYLOUSER, KYLOPWD, WSAuthScheme.BASIC).withHeaders("Content-Type" -> "application/json")
      .post("")

    val futureResponseDisableFeed = disbleFeedResponse
      .map { res =>
        if (res.status == 200) {
          Logger.logger.debug(s"$feedId disabled")
          Right(DafResponseSuccess(s"$feedId disabled", None))
        }
        else {
          Logger.logger.debug(s"$feedId not disabled")
          Left(Error(Some(res.status), s"$feedId not disabled", None))
        }
      }

    futureResponseDisableFeed
  }

  private def patchDelete(user: String, feedName: String, feedId: String): Future[Either[Error, DafSuccess]] = {
    val firstDelete: Future[Either[Error, DafSuccess]] = delete(user, feedName, feedId)

    firstDelete flatMap {
      case Right(success) => Future.successful(Right(success))
      case Left(_) => Logger.debug("second call"); Thread.sleep(10000); delete(user, feedName, feedId)
    }

  }

  private def delete(user: String, feedName: String, feedId: String): Future[Either[Error, DafSuccess]] = {
    val futureResponseDelete: Future[WSResponse] = ws.url(KYLOURL + "/api/v1/feedmgr/feeds/" + feedId)
      .withAuth(KYLOUSER, KYLOPWD, WSAuthScheme.BASIC)
      .delete()

    futureResponseDelete.map{ res =>
      res.status match {
        case 204 => Logger.logger.debug(s"$user deleted $feedName");     Right(DafResponseSuccess(s"$feedName deleted", None))
        case 404 => Logger.logger.debug(s"$feedName not found");         Right(DafResponseSuccess(s"$feedName not found", None))
        case _   => Logger.logger.debug(s"$user not deleted $feedName"); Left(Error(Some(res.status), s"kylo feed $feedName ${Try{(res.json \ "message").get.toString().replace("\"", "")}.getOrElse("generic error")}", None))
      }
    }
  }

  private def templateIdByName(templateName :String): Future[Option[String]] = {
    ws.url(KYLOURL + "/api/v1/feedmgr/templates/registered")
      .withAuth(KYLOUSER, KYLOPWD, scheme = WSAuthScheme.BASIC)
      .get()
      .map { resp =>
        val js: JsValue = resp.json
        val result = js.as[List[JsValue]]
          .filter(x => (x \ "templateName").as[String].equals(templateName))
        val res = result match {
          case Nil => None
          case h :: _  => Some((h \ "id").as[String])
        }
        Logger.logger.debug(s"templateId: $res")
        res
      }
  }


  private def templatesById(id :Option[String],
                            fileType :String,
                            feed :MetaCatalog): Future[(JsValue, List[JsObject])] = {
    val idExt = id.getOrElse("")
    val url = s"/api/v1/feedmgr/templates/registered/$idExt?allProperties=true&feedEdit=true"
    ws.url(KYLOURL + url)
      .withAuth(KYLOUSER, KYLOPWD, scheme = WSAuthScheme.BASIC)
      .get().map { resp =>
      val templates = resp.json
      val templatesEditable = (templates \ "properties").as[List[JsValue]]
        .filter(x => { (x \ "userEditable").as[Boolean] })

      val template1 = templatesEditable(0).transform(KyloTrasformers.transformTemplates(KyloTrasformers.generateInputSftpPath(feed)))
      val template2 = templatesEditable(1).transform(KyloTrasformers.transformTemplates(".*" + fileType))
      //logger.debug(List(template1,template2).toString())
      (templates, List(template1.get, template2.get))
    }
  }

  private def webServiceTemplates(id :Option[String],
                                  fileType: String,
                                  feed :MetaCatalog): Future[(JsValue, List[JsObject])]
  = {
    val idExt = id.getOrElse("")
    val url = s"/api/v1/feedmgr/templates/registered/$idExt?allProperties=true&feedEdit=true"
    val testWs  = feed.operational.input_src.srv_pull.get.head.url
    val nameExp = feed.dcatapit.name + """_${now():format('yyyyMMddHHmmss')}"""
    ws.url(KYLOURL + url)
      .withAuth(KYLOUSER, KYLOPWD, scheme = WSAuthScheme.BASIC)
      .get().map { resp =>
      val templates = resp.json
      val templatesEditable  = (templates \ "properties").as[List[JsValue]]
        .filter(x => { (x \ "userEditable").as[Boolean] })
      val modifiedTemplates: List[JsObject] = templatesEditable.map { temp =>
        val key = (temp \ "key").as[String]
        val transTemplate = key match {
          case "URL" => temp.transform(KyloTrasformers.transformTemplates(testWs)).get
          case "Filename" => temp.transform(KyloTrasformers.transformTemplates(nameExp)).get
          case "Username" => temp.transform(KyloTrasformers.transformTemplates("dsda")).get
          case "Password" => temp.transform(KyloTrasformers.transformTemplates("dasds")).get
        }
        transTemplate
      }

      (templates, modifiedTemplates)
    }
  }

  private def hdfsTemplate(id: Option[String], fileType: String, hdfsPath: String) = {
    val url = s"/api/v1/feedmgr/templates/registered/${id.getOrElse("")}?allProperties=true&feedEdit=true"
    ws.url(KYLOURL + url)
      .withAuth(KYLOUSER, KYLOPWD, WSAuthScheme.BASIC)
      .get().map { resp =>
      val template = resp.json
      val propertiesEditable = (template \ "properties").as[List[JsValue]]
        .filter(x => (x \ "userEditable").as[Boolean])
      val modifiedTemplate: List[JsObject] = propertiesEditable.map { temp =>
        val key = (temp \ "key").as[String]
        val transTemplate = key match {
          case "File Filter Regex" => temp.transform(KyloTrasformers.transformTemplates(".*" + fileType)).get
          case "Directory" => temp.transform(KyloTrasformers.transformTemplates(hdfsPath)).get
        }
        transTemplate
      }

      (template, modifiedTemplate)
    }
  }

  private def sftpTemplates(id :Option[String],
                            fileType: String,
                            sftpPath: String): Future[(JsValue, List[JsObject])]
  = {
    val idExt = id.getOrElse("")
    val url = s"/api/v1/feedmgr/templates/registered/$idExt?allProperties=true&feedEdit=true"
    ws.url(KYLOURL + url)
      .withAuth(KYLOUSER, KYLOPWD, scheme = WSAuthScheme.BASIC)
      .get().map { resp =>
      val templates = resp.json
      val templatesEditable  = (templates \ "properties").as[List[JsValue]]
        .filter(x => { (x \ "userEditable").as[Boolean] })
      val modifiedTemplates: List[JsObject] = templatesEditable.map { temp =>
        val key = (temp \ "key").as[String]
        val transTemplate = key match {
          case "Hostname" => temp.transform(KyloTrasformers.transformTemplates(SFTPHOSTNAME)).get
          case "Remote Path" => temp.transform(KyloTrasformers.transformTemplates(sftpPath)).get
          case "File Filter Regex" => temp.transform(KyloTrasformers.transformTemplates(".*" + fileType)).get
        }
        transTemplate
      }

      (templates, modifiedTemplates)
    }
  }

  private def transformationTemplate(id :Option[String],
                            fileType: String,
                            sftpPath: String): Future[(JsValue, List[JsObject])]
  = {
    val idExt = id.getOrElse("")
    val url = s"/api/v1/feedmgr/templates/registered/$idExt?allProperties=true&feedEdit=true"
    ws.url(KYLOURL + url)
      .withAuth(KYLOUSER, KYLOPWD, scheme = WSAuthScheme.BASIC)
      .get().map { resp =>
      val templates = resp.json
      val templatesEditable  = (templates \ "properties").as[List[JsValue]]
        .filter(x => { (x \ "userEditable").as[Boolean] })
      val modifiedTemplates: List[JsObject] = templatesEditable.map { temp =>
        val key = (temp \ "key").as[String]
        val transTemplate = key match {
          case "Hostname" => temp.transform(KyloTrasformers.transformTemplates(SFTPHOSTNAME)).get
          case "Remote Path" => temp.transform(KyloTrasformers.transformTemplates(sftpPath)).get
          case "File Filter Regex" => temp.transform(KyloTrasformers.transformTemplates(".*" + fileType)).get
        }
        transTemplate
      }

      (templates, modifiedTemplates)
    }
  }


  def datasetIngest(fileType :String, meta :MetaCatalog) :Future[(JsValue, List[JsObject])] = {
    for {
      idOpt <- templateIdByName("Dataset Ingest")
      templates <- templatesById(idOpt, fileType, meta)
    } yield templates
  }

  def wsIngest(fileType: String, meta: MetaCatalog): Future[(JsValue, List[JsObject])] = {
    for {
      idOpt <- templateIdByName("Webservice Ingest")
      templates <- webServiceTemplates(idOpt, fileType, meta)
    } yield templates
  }

  def sftpRemoteIngest(fileType: String, sftpPath: String): Future[(JsValue, List[JsObject])] = {
    for {
      idOpt <- templateIdByName("Sftp Ingest")
      templates <- sftpTemplates(idOpt, fileType, sftpPath)
    } yield templates
  }

  def hdfsIngest(fileType: String, hdfsPath: String) = {
    for{
      idOpt <- templateIdByName("HDFS Ingest")
      templates <- hdfsTemplate(idOpt, fileType, hdfsPath)
    } yield templates
  }

  def trasformerIngest(fileType: String, hdfsPath: String) = {
    for{
      idOpt <- templateIdByName("Dataset Transformation")
      templates <- transformationTemplate(idOpt, fileType, hdfsPath)
    } yield templates
  }



  //WsResponse passed as parameter only fro chaining the call
  def categoryFuture(meta :MetaCatalog): Future[JsValue] = {
    val categoriesWs = ws.url(KYLOURL + "/api/v1/feedmgr/categories")
      .withAuth(KYLOUSER, KYLOPWD, scheme = WSAuthScheme.BASIC)
      .get()

    val categoryFuture = categoriesWs.map { x =>
      val categoriesJson = x.json
      val categories = categoriesJson.as[List[JsValue]]
      val found =categories.filter(cat => {(cat \ "systemName").as[String].equals(meta.dcatapit.owner_org.get)})
      found.head
    }
    categoryFuture
  }

}
