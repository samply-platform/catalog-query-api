package org.samply.cqapi

import dagger.Component
import io.vertx.config.ConfigRetriever
import io.vertx.config.ConfigRetrieverOptions
import io.vertx.config.ConfigStoreOptions
import io.vertx.core.json.JsonObject
import io.vertx.kotlin.config.getConfigAwait
import io.vertx.kotlin.core.deployVerticleAwait
import io.vertx.kotlin.core.json.json
import io.vertx.kotlin.core.json.obj
import io.vertx.kotlin.coroutines.CoroutineVerticle
import org.samply.cqapi.domain.KafkaStreamsModule
import org.samply.cqapi.web.ServerVerticle
import javax.inject.Singleton

class MainVerticle : CoroutineVerticle() {

  @Singleton
  @Component(modules = [KafkaStreamsModule::class])
  interface CatalogQueryApi {
    fun server(): ServerVerticle
  }

  override suspend fun start() {
    val cqapi = DaggerMainVerticle_CatalogQueryApi
      .builder()
      .kafkaStreamsModule(KafkaStreamsModule(getConfig()))
      .build()

    vertx.deployVerticleAwait(cqapi.server())
  }

  suspend fun getConfig(): JsonObject {
    val configRetrieverOptions = ConfigRetrieverOptions().setStores(listOf(
      ConfigStoreOptions()
        .setType("file")
        .setFormat("yaml")
        .setConfig(json { obj("path" to "application.yaml") }),
      ConfigStoreOptions().setType("env")
    ))

    val configRetriever = ConfigRetriever.create(vertx, configRetrieverOptions)
    return configRetriever.getConfigAwait()
  }

}
