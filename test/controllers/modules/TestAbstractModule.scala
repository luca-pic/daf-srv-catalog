package controllers.modules

import akka.stream.Materializer
import org.pac4j.play.store.{PlayCacheSessionStore, PlaySessionStore}
import play.api.{Configuration, Environment}
import play.api.inject.Module
import play.api.inject.guice.GuiceInjectorBuilder
import play.api.libs.ws.WSClient
import play.api.libs.ws.ahc.AhcWSClient
import play.cache.{CacheApi, DefaultCacheApi}
import play.api.cache.{CacheApi => ApiCacheApi}
import util.TestCache

abstract class TestAbstractModule extends Module {

  private lazy val injector = new GuiceInjectorBuilder().bindings(this).injector()
  private lazy val sessionStoreInstance: PlaySessionStore = injector.instanceOf { classOf[PlaySessionStore] }
  private lazy val wsClientInstance: WSClient = injector.instanceOf { classOf[WSClient] }

  final def sessionStore: PlaySessionStore = sessionStoreInstance
  implicit final def ws: WSClient = wsClientInstance

  protected implicit def materializer: Materializer

  override def bindings(environment: Environment, configuration: Configuration) = Seq(
    bind(classOf[ApiCacheApi]).to(classOf[TestCache]),
    bind(classOf[CacheApi]).to(classOf[DefaultCacheApi]),
    bind(classOf[PlaySessionStore]).to(classOf[PlayCacheSessionStore]),
    bind(classOf[WSClient]).toInstance(AhcWSClient())
  )

}
