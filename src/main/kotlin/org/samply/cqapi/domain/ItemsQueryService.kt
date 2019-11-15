package org.samply.cqapi.domain

import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.state.QueryableStoreTypes
import org.apache.kafka.streams.state.QueryableStoreTypes.keyValueStore
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore
import org.samply.catalog.api.domain.model.Category
import org.samply.catalog.api.domain.model.Item
import org.samply.cqapi.domain.KafkaModule.Companion.ITEM_CREATED_TABLE_STORE
import java.math.BigDecimal
import java.text.NumberFormat
import java.util.*
import javax.inject.Inject
import javax.inject.Singleton

@Singleton
class ItemsQueryService @Inject constructor(private val kafkaStreams: KafkaStreams) {

  private val itemCreateTable by lazy {
    kafkaStreams.store(ITEM_CREATED_TABLE_STORE, keyValueStore<String, Item>())
  }

  suspend fun findById(id: ItemId): ItemDTO? {
    val item: Item? = itemCreateTable.get(id)

    return item?.let { ItemDTO(
      item.getId(),
      item.getTitle(),
      item.getDescription(),
      NumberFormat.getInstance(Locale.GERMANY).format(item.getPrice()),
      item.getCategory()
    )}
  }

}
