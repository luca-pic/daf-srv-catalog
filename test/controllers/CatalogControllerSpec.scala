package controllers

import akka.stream.ActorMaterializer
import controllers.modules.TestAbstractModule
import instance.{AkkaInstance, ConfigurationInstance}
import org.pac4j.core.profile.{CommonProfile, ProfileManager}
import org.pac4j.play.PlayWebContext
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}
import play.api.test.FakeRequest
import play.api.mvc.{Headers, _}

import scala.concurrent.duration._
import scala.concurrent.Await

class CatalogControllerSpec extends TestAbstractModule
  with WordSpecLike
  with Matchers
  with BeforeAndAfterAll
  with ConfigurationInstance
  with AkkaInstance {

  implicit lazy val executionContext = actorSystem.dispatchers.lookup("akka.actor.test-dispatcher")

  protected implicit lazy val materializer = ActorMaterializer.create { actorSystem }

  private def withController[U](f: CatalogController => U) = f { new CatalogController(sessionStore) with TestCatalogClient }

  private def request[A](method: String, uri: String, body: A, authorization: Option[String] = None, headers: Headers = Headers())(action: => Action[A]) = Await.result(
    action {
      val k = FakeRequest(
        method  = method,
        uri     = uri,
        body    = body,
        headers = authorization.fold(headers) { auth => println(s"auth: $auth");headers.add("Authorization" -> auth) }
      )
      println("header fake request: " + k.headers)
      k
    },
    5.seconds
  )

  private val userProfile = {
    val profile = new CommonProfile
    profile.setId("test-user")
    profile.setRemembered(true)
    profile
  }

  private def createSession() = {
    val context = new PlayWebContext(
      FakeRequest(
        method  = "OPTIONS",
        uri     = "/",
        body    = AnyContentAsEmpty,
        headers = Headers()
      ),
      sessionStore
    )
    val profileManager = new ProfileManager[CommonProfile](context)
    profileManager.save(true, userProfile, false)
  }

  override def beforeAll() = {
    startAkka()
    createSession()
  }

  override def afterAll(): Unit = {
    terminateAkka()
  }

  "A CatalogController" when {
    "calling an api" must {
      "return 401 when the auth header is missing" in withController { controller =>
        request[AnyContent]("GET", "/catalog-manager/v1/catalog-ds/getbyname/name", AnyContentAsEmpty, Some("Basic:token")) {
          controller.getCatalog("name")
        }.header.status should be { 401 }
      }
    }
  }
}
