package it.gov.daf.catalogmanager.utils
import it.gov.daf.model.MetaCatalog
import play.api.Logger
import play.api.mvc.Headers

import scala.util.{Failure, Success, Try}

/**
  * Created by ale on 30/05/17.
  */
object CatalogManager {

  def writeOrdinaryWithStandard(metaCatalogOrdinary: MetaCatalog, metaCatalogStandard: MetaCatalog): Option[MetaCatalog] = {

    val checkSchema: Try[Boolean] = CoherenceChecker.checkCoherenceSchemas(metaCatalogOrdinary, metaCatalogStandard)

    checkSchema match {
      case Success(value) =>
        Logger.info(s"Storing schema having url ${metaCatalogOrdinary.operational.logical_uri}.")

        val toSave: Try[MetaCatalog] = for {
          uriDataset <- Try(UriDataset.convertToUriDataset(metaCatalogOrdinary))
          logicalUri <- Try(uriDataset.getUri())
          physicalUri <- Try(uriDataset.getUrl())
          operational <- Try(metaCatalogOrdinary.operational.copy(logical_uri = Some(logicalUri), physical_uri = Some(physicalUri)))
          newSchema <- Try(metaCatalogOrdinary.copy( operational = operational))
        } yield newSchema

        toSave match {
          case Success(save) => Some(save)
          case Failure(ex)   => Logger.debug(ex.getMessage); None
        }

      case Failure(ex)  =>
        Logger.debug(s"Unable to write the schema with uri ${metaCatalogOrdinary.operational.logical_uri}. ERROR message: \t ${ex.getMessage} ${ex.getStackTrace.mkString("\n\t")}")
        None
    }
  }

  def writeOrdAndStdOrDerived(metaCatalog: MetaCatalog) : Option[MetaCatalog] = {

    val datasetType = if (metaCatalog.operational.is_std)
      Standard
    else if(metaCatalog.operational.type_info.isDefined && metaCatalog.operational.type_info.get.dataset_type.equals("derived_sql"))
      Derived
    else if (metaCatalog.operational.ext_opendata.isDefined)
      OpenData
    else
      Ordinary

    val datasetConverter = new TmpDafUriConverter(
      datasetType,
      metaCatalog.dcatapit.holder_identifier.get,
      metaCatalog.operational.theme,
      metaCatalog.operational.subtheme,
      metaCatalog.dcatapit.name
    )

    val toSave: Option[MetaCatalog] = for {
      uriDataset <- Option(datasetConverter)
      logicalUri <- Option(uriDataset.toLogicalUri)
      physicalUri <- Option(uriDataset.toPhysicalUri())
      operational <- Option(metaCatalog.operational.copy(logical_uri = Some(logicalUri), physical_uri = Some(physicalUri)))
      newSchema <- Option(metaCatalog.copy( operational = operational))
    } yield newSchema
    toSave
  }

  def readTokenFromRequest(requestHeader: Headers, allToken: Boolean): Option[String] = {
    val authHeader = requestHeader.get("authorization").get.split(" ")
    val authType = authHeader(0)
    val authCredentials = authHeader(1)

    allToken match {
      case true => Some(s"$authType $authCredentials")
      case false => if( authType.equalsIgnoreCase("bearer")) Some(authCredentials) else None
    }
  }

}
