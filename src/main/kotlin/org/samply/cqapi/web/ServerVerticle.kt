package org.samply.cqapi.web

import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import graphql.GraphQL
import io.vertx.core.Context
import io.vertx.core.Vertx
import io.vertx.core.json.jackson.DatabindCodec
import io.vertx.ext.web.Router
import io.vertx.ext.web.handler.LoggerHandler
import io.vertx.ext.web.handler.graphql.GraphQLHandler
import io.vertx.ext.web.handler.graphql.GraphiQLHandler
import io.vertx.kotlin.core.http.listenAwait
import io.vertx.kotlin.coroutines.CoroutineVerticle
import org.apache.kafka.streams.KafkaStreams
import javax.inject.Inject

class ServerVerticle @Inject constructor(private val kafkaStreams: KafkaStreams,
                                         private val graphQL: GraphQL) : CoroutineVerticle() {

  override fun init(vertx: Vertx, context: Context) {
    super.init(vertx, context)
    kafkaStreams.start()
    DatabindCodec.mapper().registerKotlinModule()
    DatabindCodec.prettyMapper().registerKotlinModule()
  }

  override suspend fun start() {
    val router = Router.router(vertx)
    router.route().handler(LoggerHandler.create())
    router.route("/graphql").handler(GraphQLHandler.create(graphQL))
    router.route("/graphiql/*").handler(GraphiQLHandler.create())

    vertx
      .createHttpServer()
      .requestHandler(router)
      .listenAwait(8888)
  }

}
