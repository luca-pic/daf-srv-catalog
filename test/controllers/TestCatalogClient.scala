package controllers

import com.mongodb.casbah.MongoConnection
import instance.ConfigurationInstance
import it.gov.daf.catalogmanager.catalog.MongoRepository
import play.api.mvc.Request
import it.gov.daf.common.utils.Profile
import javax.inject.Inject
import play.api.Configuration


trait TestCatalogClient extends ConfigurationInstance { this: CatalogController =>

  override protected def getUserInfo[A](request: Request[A]) = {
    Profile("test-user", "toke", List("test-org").toArray)
  }

  override protected val mongoClient = new TestMongoRepository

}

sealed class TestMongoRepository @Inject()(implicit val configuration: Configuration) extends MongoRepository {


  override val collection = MongoConnection()("ckan")("test-coll")

}
