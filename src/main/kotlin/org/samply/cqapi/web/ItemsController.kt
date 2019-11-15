package org.samply.cqapi.web

import io.vertx.core.json.Json
import io.vertx.ext.web.RoutingContext
import io.vertx.kotlin.coroutines.awaitResult
import org.apache.kafka.streams.KafkaStreams
import org.samply.cqapi.domain.ItemDTO
import org.samply.cqapi.domain.ItemsQueryService
import javax.inject.Inject
import javax.inject.Singleton

@Singleton
class ItemsController @Inject constructor(private val itemsQueryService: ItemsQueryService) {

  suspend fun getItemById(routingContext: RoutingContext) {
    val itemById = itemsQueryService.findById(routingContext.request().getParam("id"))

    routingContext.response()
      .putHeader("content-type", "application/json")
      .end(Json.encodePrettily(itemById))
  }

}
