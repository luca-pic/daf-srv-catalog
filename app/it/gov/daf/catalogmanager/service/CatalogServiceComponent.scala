package it.gov.daf.catalogmanager.service


import catalog_manager.yaml.{Dataset, DatasetNameFields, Error, LinkedDataset, LinkedParams, MetaCatalog, MetadataCat, Success}
import it.gov.daf.catalogmanager.repository.catalog.CatalogRepositoryComponent
import play.api.libs.json.JsValue

import scala.concurrent.Future
import play.api.libs.ws._
import play.api.libs.ws.ahc.AhcWSComponents

/**
  * Created by ale on 05/05/17.
  */
trait CatalogServiceComponent {
  this: CatalogRepositoryComponent  =>
  val catalogService: CatalogService

  class CatalogService {


    def listCatalogs(page :Option[Int], limit :Option[Int]) :Seq[MetaCatalog] = {
       catalogRepository.listCatalogs(page, limit)

    }
    def catalog(catalogId :String): Option[MetaCatalog] = {
      catalogRepository.catalog(catalogId)
    }

    def internalCatalogByName(name: String, user: String, org: String) = {
      catalogRepository.internalCatalogByName(name, user, org)
    }

    def catalogByName(name :String, user: String, groups: List[String]): Option[MetaCatalog] = {
      catalogRepository.catalogByName(name, user, groups)
    }

    def publicCatalogByName(name :String): Option[MetaCatalog] = {
      catalogRepository.publicCatalogByName(name)
    }

    def createCatalog(metaCatalog: MetaCatalog, callingUserid :MetadataCat, ws :WSClient): Either[Error, Success] = {
      println("Service : " +  callingUserid)
      catalogRepository.createCatalog(metaCatalog, callingUserid, ws)
    }

    def createCatalogExtOpenData(metaCatalog: MetaCatalog, callingUserid :MetadataCat, ws :WSClient) :Success = {
      println("Service : " +  callingUserid)
      catalogRepository.createCatalogExtOpenData(metaCatalog, callingUserid, ws)
    }

    def isPresentOnCatalog(name :String) :Option[Boolean] = {
      catalogRepository.isDatasetOnCatalog(name)
    }

    def deleteCatalogByName(nameCatalog: String, user: String, token: String, wsClient: WSClient): Future[Either[Error, Success]] = {
      catalogRepository.deleteCatalogByName(nameCatalog, user, token: String, wsClient)
    }

    def getDatasetStandardFields(user: String, groups: List[String]): Future[Seq[DatasetNameFields]] = {
      catalogRepository.getDatasetStandardFields(user, groups)
    }

    def getTag: Future[Seq[String]] = {
      catalogRepository.getTag
    }

    def getFieldsVoc: Future[Seq[DatasetNameFields]] = {
      catalogRepository.getFieldsVoc
    }

    def getLinkedDatasets(datasetName: String, linkedParams: LinkedParams, user: String, groups: List[String], limit: Option[Int]): Future[Seq[LinkedDataset]] = {
      catalogRepository.getLinkedDatasets(datasetName, linkedParams, user, groups, limit)
    }
  }
}
