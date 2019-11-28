package org.samply.cqapi.domain

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.flow.*
import kotlinx.coroutines.withContext
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
@FlowPreview
class ItemService @Inject constructor(private val kafkaStreams: KafkaStreams) {

  companion object {
    private const val NON_EXISTING_KEY = "non-existing-key"
  }

  private val itemCreatedTable by lazy {
    kafkaStreams.store(ITEM_CREATED_TABLE_STORE, keyValueStore<String, Item>())
  }

  private val itemCreatedBySellerTable by lazy {
    kafkaStreams.store(SELLER_TO_ITEM_CREATED_STORE, keyValueStore<String, Collection<Item>>())
  }

  suspend fun findAll(pagination: Pagination): Flow<ItemDTO> {
    return withContext(Dispatchers.Default) {
      when (pagination.after) {
        null -> itemCreatedTable.all()
        else -> itemCreatedTable.range(pagination.after, NON_EXISTING_KEY)
      }
    }
      .asFlow()
      .take(pagination.first)
      .map { toDTO(it.value) }
  }

  suspend fun findById(id: ItemId): ItemDTO? {
    return withContext(Dispatchers.Default) { itemCreatedTable.get(id) }
      ?.let { toDTO(it) }
  }

  suspend fun findBySeller(sellerId: SellerId, pagination: Pagination): Flow<ItemDTO> {
    return withContext(Dispatchers.Default) {
      itemCreatedBySellerTable.get(sellerId)?.map { toDTO(it) } ?: listOf()
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
