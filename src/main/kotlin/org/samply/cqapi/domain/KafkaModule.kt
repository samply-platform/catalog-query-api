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
    const val ITEM_LOG_TABLE = "item-log-table"
    const val ITEM_LOG_BY_SELLER_TABLE = "item-log-by-seller-table"
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

    val itemCreatedTable = streamsBuilder.itemLogTable()
    itemLogBySellerTable(itemCreatedTable)

    return KafkaStreams(streamsBuilder.build(), streamsConfig())
  }

  private fun StreamsBuilder.itemLogTable(): KTable<String, Item> {
    return this.table(
      "item-log",
      Consumed.with(Serdes.String(), itemSerde),
      Materialized.`as`<String, Item, KeyValueStore<Bytes, ByteArray>>(ITEM_LOG_TABLE)
    )
  }

  private fun itemLogBySellerTable(itemCreatedTable: KTable<String, Item>): KTable<SellerId, Collection<ItemId>> {
    return itemCreatedTable
      .groupBy(
        { itemId, item -> KeyValue(item.getSellerId(), itemId) },
        Grouped.with(Serdes.String(), Serdes.String())
      )
      .aggregate(
        { hashSetOf() },
        { _, value, aggregate -> aggregate + value },
        { _, value, aggregate -> aggregate - value },
        Materialized
          .`as`<String, Collection<ItemId>, KeyValueStore<Bytes, ByteArray>>(ITEM_LOG_BY_SELLER_TABLE)
          .withKeySerde(Serdes.String())
          .withValueSerde(collectionSerde(Serdes.String()))
      )
  }

}
