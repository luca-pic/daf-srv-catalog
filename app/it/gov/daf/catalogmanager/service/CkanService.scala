package it.gov.daf.catalogmanager.service

/**
  * Created by ale on 18/07/17.
  */
import catalog_manager.yaml.{AutocompRes, Credentials, Dataset, Error, Organization, Success, User}
import play.api.{Configuration, Environment}
import play.api.libs.json.{JsResult, JsValue}
import it.gov.daf.catalogmanager.repository.ckan.{CkanRepository, CkanRepositoryComponent}
import play.api.libs.ws.WSClient

import scala.concurrent.Future
import scala.math.BigInt

/**
  * Created by ale on 01/07/17.
  */

trait CkanServiceComponent {
  this: CkanRepositoryComponent =>
  val ckanService: CkanService

  class CkanService {

    def getMongoUser(name:String, callingUserid :Option[String] ): JsResult[User]  = {
      ckanRepository.getMongoUser(name, callingUserid)
    }

    def verifyCredentials(credentials: Credentials):Boolean = {
      ckanRepository.verifyCredentials(credentials: Credentials)
    }
    def updateOrganization(orgId: String, jsonOrg: JsValue, callingUserid :Option[String] ): Future[String] = {
      ckanRepository.updateOrganization(orgId,jsonOrg, callingUserid)
    }
    def patchOrganization(orgId: String, jsonOrg: JsValue, callingUserid :Option[String] ): Future[String] = {
      ckanRepository.patchOrganization(orgId,jsonOrg, callingUserid)
    }

    def createUser(jsonUser: JsValue, callingUserid :Option[String]): Future[String] = {
      ckanRepository.createUser(jsonUser, callingUserid)
    }
    def getUserOrganizations(userName :String, callingUserid :Option[String]) : Future[JsResult[Seq[Organization]]] = {
      ckanRepository.getUserOrganizations(userName, callingUserid)
    }

    def createDataset(jsonDataset: JsValue, callingUserid :Option[String]): Future[String] = {
      ckanRepository.createDataset(jsonDataset,callingUserid)
    }

    def createCatalogCkanGeo(dataset: Dataset, user: String, token: String, ws: WSClient): Future[Either[Error, Success]] = {
      ckanRepository.createDatasetCkanGeo(dataset, user, token, ws)
    }

    def deleteDatasetCkanGeo(catalog: Dataset, user: String, token: String, wsClient: WSClient): Future[Either[Error, Success]] = {
      ckanRepository.deleteDatasetCkanGeo(catalog, user, token, wsClient)
    }

    def createOrganization(jsonDataset: JsValue, callingUserid :Option[String]): Future[String] = {
      ckanRepository.createOrganization(jsonDataset,callingUserid)
    }
    def dataset(datasetId: String, callingUserid :Option[String]): JsValue = {
      ckanRepository.dataset(datasetId,callingUserid)
    }

    def getOrganization(orgId :String, callingUserid :Option[String]) : Future[JsResult[Organization]] = {
      ckanRepository.getOrganization(orgId,callingUserid)
    }

    def getOrganizations(callingUserid :Option[String]) : Future[JsValue] = {
      ckanRepository.getOrganizations(callingUserid)
    }

    def getDatasets(callingUserid :Option[String]) : Future[JsValue] = {
      ckanRepository.getDatasets(callingUserid)
    }

    def searchDatasets( input: (Option[String], Option[String], Option[BigInt], Option[BigInt]), callingUserid :Option[String]) : Future[JsResult[Seq[Dataset]]] = {
      ckanRepository.searchDatasets(input, callingUserid)
    }
    def autocompleteDatasets( input: (Option[String], Option[BigInt]), callingUserid :Option[String]) : Future[JsResult[Seq[AutocompRes]]] = {
      ckanRepository.autocompleteDatasets(input, callingUserid)
    }

    def getDatasetsWithRes( input: (Option[BigInt], Option[BigInt]),callingUserid :Option[String] ) : Future[JsResult[Seq[Dataset]]] = {
      ckanRepository.getDatasetsWithRes(input, callingUserid)
    }

    def testDataset(datasetId :String, callingUserid :Option[String]) : Future[JsResult[Dataset]] = {
      ckanRepository.testDataset(datasetId, callingUserid)
    }

  }
}


object CkanRegistry extends
  CkanServiceComponent with
  CkanRepositoryComponent {
  val conf = Configuration.load(Environment.simple())
  val app: String = conf.getString("app.type").getOrElse("dev")
  val ckanRepository =  CkanRepository(app)
  val ckanService = new CkanService
}
