package org.samply.cqapi.domain

import dagger.Module
import dagger.Provides
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde
import io.vertx.core.json.JsonObject
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.common.utils.Bytes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.kstream.Consumed
import org.apache.kafka.streams.kstream.Materialized
import org.apache.kafka.streams.state.KeyValueStore
import org.samply.catalog.api.domain.model.Item
import java.util.*
import javax.inject.Singleton

@Module
class KafkaModule constructor(private val config: JsonObject) {

  companion object {
    const val ITEM_CREATED_TABLE_STORE = "item-created-table"
  }

  private fun streamsConfig(): Properties {
    val kafkaConfig = config.getJsonObject("kafka")

    val props = Properties()
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "cqapi");
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaConfig.getString("bootstrapServers"));
    props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
    return props
  }

  @Provides
  @Singleton
  fun streams(): KafkaStreams {
    val registryUrl = config.getJsonObject("kafka").getString("registryUrl")

    val streamsBuilder = StreamsBuilder()
    val itemSerde = SpecificAvroSerde<Item>()
    itemSerde.configure(mapOf("schema.registry.url" to registryUrl), false)

    streamsBuilder.table(
      "item-created-log",
      Consumed.with(Serdes.String(), itemSerde),
      Materialized.`as`<String, Item, KeyValueStore<Bytes, ByteArray>>(ITEM_CREATED_TABLE_STORE)
    )

    return KafkaStreams(streamsBuilder.build(), streamsConfig())
  }

}
