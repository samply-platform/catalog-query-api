package org.samply.cqapi.web

import io.vertx.ext.web.RoutingContext
import javax.inject.Inject
import javax.inject.Singleton

@Singleton
class ItemsController @Inject constructor() {

  fun getItemById(routingContext: RoutingContext) {
    routingContext.response()
      .putHeader("content-type", "text/plain")
      .end("GET for item with ID ${routingContext.request().getParam("id")}")
  }

}
