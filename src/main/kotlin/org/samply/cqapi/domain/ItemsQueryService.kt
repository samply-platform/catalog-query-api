package org.samply.cqapi.domain

import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.state.QueryableStoreTypes.keyValueStore
import org.samply.catalog.api.domain.model.Item
import org.samply.cqapi.domain.KafkaModule.Companion.ITEM_CREATED_TABLE_STORE
import org.samply.cqapi.domain.KafkaModule.Companion.SELLER_TO_ITEM_CREATED_STORE
import java.text.NumberFormat
import java.util.*
import javax.inject.Inject
import javax.inject.Singleton

@Singleton
class ItemsQueryService @Inject constructor(private val kafkaStreams: KafkaStreams) {

  private val itemCreatedTable by lazy {
    kafkaStreams.store(ITEM_CREATED_TABLE_STORE, keyValueStore<String, Item>())
  }

  private val itemCreatedBySellerTable by lazy {
    kafkaStreams.store(SELLER_TO_ITEM_CREATED_STORE, keyValueStore<String, Collection<Item>>())
  }

  suspend fun findById(id: ItemId): ItemDTO? {
    val item: Item? = itemCreatedTable.get(id)
    return item?.let(this::toDTO)
  }

  suspend fun findBySeller(sellerId: SellerId): List<ItemDTO> {
    val sellerItems: Collection<Item>? = itemCreatedBySellerTable.get(sellerId)
    return sellerItems?.map(this::toDTO) ?: listOf()
  }

  private fun toDTO(item: Item): ItemDTO {
    return ItemDTO(
      item.getId(),
      item.getTitle(),
      item.getDescription(),
      NumberFormat.getInstance(Locale.GERMANY).format(item.getPrice()),
      item.getCategory()
    )
  }

}
