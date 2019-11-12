package org.samply.cqapi.web

import io.vertx.core.AbstractVerticle
import io.vertx.core.Promise
import io.vertx.ext.web.Router
import javax.inject.Inject

class ServerVerticle @Inject constructor(private val itemsController: ItemsController) : AbstractVerticle() {

  override fun start(startPromise: Promise<Void>) {

    val router = Router.router(vertx)
    router.get("/items/:id").handler(itemsController::getItemById)

    vertx
      .createHttpServer()
      .requestHandler(router)
      .listen(8888) { http ->
        if (http.succeeded()) {
          startPromise.complete()
          println("HTTP server started on port 8888")
        } else {
          startPromise.fail(http.cause())
        }
      }
  }

}
