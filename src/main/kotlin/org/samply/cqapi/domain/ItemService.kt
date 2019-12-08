package org.samply.cqapi.domain

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.flow.*
import kotlinx.coroutines.withContext
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.state.QueryableStoreTypes.keyValueStore
import org.samply.catalog.api.domain.model.Item
import org.samply.cqapi.domain.KafkaModule.Companion.ITEM_LOG_TABLE
import org.samply.cqapi.domain.KafkaModule.Companion.ITEM_LOG_BY_SELLER_TABLE
import java.text.NumberFormat
import java.util.*
import javax.inject.Inject
import javax.inject.Singleton

@Singleton
@FlowPreview
class ItemService @Inject constructor(private val kafkaStreams: KafkaStreams) {

  companion object {
    private const val NON_EXISTING_KEY = "non-existing-key"
  }

  private val itemLogTable by lazy {
    kafkaStreams.store(ITEM_LOG_TABLE, keyValueStore<String, Item>())
  }

  private val itemLogBySellerTable by lazy {
    kafkaStreams.store(ITEM_LOG_BY_SELLER_TABLE, keyValueStore<String, Collection<ItemId>>())
  }

  suspend fun findAll(pagination: Pagination): Flow<ItemDTO> {
    return withContext(Dispatchers.Default) {
      when (pagination.after) {
        null -> itemLogTable.all()
        else -> itemLogTable.range(pagination.after, NON_EXISTING_KEY)
      }
    }
      .asFlow()
      .take(pagination.first)
      .map { toDTO(it.value) }
  }

  suspend fun findById(id: ItemId): ItemDTO? {
    return withContext(Dispatchers.Default) { itemLogTable.get(id) }
      ?.let { toDTO(it) }
  }

  suspend fun findBySeller(sellerId: SellerId, pagination: Pagination): Flow<ItemDTO> {
    return withContext(Dispatchers.Default) {
      itemLogBySellerTable.get(sellerId)?.mapNotNull { findById(it) } ?: listOf()
    }
      .asFlow()
      .dropWhile { pagination.after?.let { after -> it.id != after } ?: false }
      .take(pagination.first)
  }

  private fun toDTO(item: Item): ItemDTO {
    return ItemDTO(
      item.getId(),
      item.getSellerId(),
      item.getTitle(),
      item.getDescription(),
      NumberFormat.getInstance(Locale.GERMANY).format(item.getPrice()),
      item.getCategory()
    )
  }

}
