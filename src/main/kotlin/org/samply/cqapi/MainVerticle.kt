package org.samply.cqapi

import dagger.Component
import io.vertx.core.AbstractVerticle
import io.vertx.core.Promise
import org.samply.cqapi.web.ServerVerticle
import javax.inject.Singleton

class MainVerticle : AbstractVerticle() {

  @Singleton
  @Component
  interface CatalogQueryApi {
    fun server(): ServerVerticle
  }

  override fun start(startPromise: Promise<Void>) {
    val serverVerticle = DaggerMainVerticle_CatalogQueryApi
      .create()
      .server()

    vertx.deployVerticle(serverVerticle) {
      if (it.failed()) {
        startPromise.fail(it.cause());
      } else {
        startPromise.complete()
      }
    }

  }

}
