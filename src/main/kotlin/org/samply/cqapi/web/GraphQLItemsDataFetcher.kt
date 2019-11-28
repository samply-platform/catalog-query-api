package org.samply.cqapi.web

import graphql.schema.DataFetcher
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.future.future
import org.samply.cqapi.domain.*
import java.util.concurrent.CompletableFuture
import javax.inject.Inject
import javax.inject.Singleton

@Singleton
@FlowPreview
internal class GraphQLItemsDataFetcher @Inject constructor(private val itemService: ItemService) {

  internal val itemByIdDataFetcher: DataFetcher<CompletableFuture<ItemDTO?>> = DataFetcher {
    val itemId = it.getArgument<ItemId>("id")
    GlobalScope.future {
      itemService.findById(itemId)
    }
  }

  internal val itemsDataFetcher: DataFetcher<CompletableFuture<List<ItemDTO>>> = DataFetcher {
    val sellerId: SellerId? = it.getArgument<SellerId>("sellerId")
    val pagination = Pagination().apply {
      it.getArgument<Int>("first")?.let { first -> this.first = first }
      it.getArgument<ItemId>("after")?.let { after -> this.after = after }
    }

    when (sellerId) {
      null -> fetchAllItems(pagination)
      else -> fetchBySellerId(sellerId, pagination)
    }
  }

  private fun fetchAllItems(pagination: Pagination): CompletableFuture<List<ItemDTO>> {
    return GlobalScope.future {
      itemService
        .findAll(pagination)
        .toList()
    }
  }

  private fun fetchBySellerId(sellerId: SellerId, pagination: Pagination): CompletableFuture<List<ItemDTO>> {
    return GlobalScope.future {
      itemService
        .findBySeller(sellerId, pagination)
        .toList()
    }
  }

}
