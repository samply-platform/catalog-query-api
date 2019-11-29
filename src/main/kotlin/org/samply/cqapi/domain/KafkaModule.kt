package org.samply.cqapi.domain

import dagger.Module
import dagger.Provides
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde
import io.vertx.core.json.JsonObject
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.common.utils.Bytes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.kstream.*
import org.apache.kafka.streams.state.KeyValueStore
import org.samply.catalog.api.domain.model.Item
import java.util.*
import javax.inject.Singleton

@Module
class KafkaModule constructor(private val config: JsonObject) {

  companion object {
    const val ITEM_CREATED_TABLE_STORE = "item-created-table"
    const val SELLER_TO_ITEM_CREATED_STORE = "item-created-by-seller"
  }

  private val itemSerde = SpecificAvroSerde<Item>()

  init {
    val registryUrl = config.getJsonObject("kafka").getString("registryUrl")
    itemSerde.configure(mapOf("schema.registry.url" to registryUrl), false)
  }

  private fun streamsConfig(): Properties {
    val kafkaConfig = config.getJsonObject("kafka")

    val props = Properties()
    props[StreamsConfig.APPLICATION_ID_CONFIG] = "cqapi"
    props[StreamsConfig.BOOTSTRAP_SERVERS_CONFIG] = kafkaConfig.getString("bootstrapServers")
    props[StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG] = 0
    return props
  }

  @Provides
  @Singleton
  fun streams(): KafkaStreams {
    val streamsBuilder = StreamsBuilder()

    val itemCreatedTable = streamsBuilder.itemCreatedTable()
    itemsCreatedBySellerTable(itemCreatedTable)

    return KafkaStreams(streamsBuilder.build(), streamsConfig())
  }

  private fun StreamsBuilder.itemCreatedTable(): KTable<String, Item> {
    return this.table(
      "item-created-log",
      Consumed.with(Serdes.String(), itemSerde),
      Materialized.`as`<String, Item, KeyValueStore<Bytes, ByteArray>>(ITEM_CREATED_TABLE_STORE)
    )
  }

  private fun itemsCreatedBySellerTable(itemCreatedTable: KTable<String, Item>): KTable<SellerId, Collection<Item>> {
    return itemCreatedTable
      .groupBy(
        { _, v -> KeyValue(v.getSellerId(), v) },
        Grouped.with(Serdes.String(), itemSerde)
      )
      .aggregate(
        { hashSetOf() },
        { _, value, aggregate -> aggregate + value },
        { _, value, aggregate -> aggregate - value },
        Materialized
          .`as`<String, Collection<Item>, KeyValueStore<Bytes, ByteArray>>(SELLER_TO_ITEM_CREATED_STORE)
          .withKeySerde(Serdes.String())
          .withValueSerde(collectionSerde(itemSerde))
      )
  }

}
