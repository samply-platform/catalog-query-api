package org.samply.cqapi.web

import graphql.schema.DataFetcher
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.future.future
import org.samply.cqapi.domain.ItemDTO
import org.samply.cqapi.domain.ItemId
import org.samply.cqapi.domain.ItemsQueryService
import java.util.concurrent.CompletableFuture
import javax.inject.Inject
import javax.inject.Singleton

@Singleton
internal class GraphQLItemsDataFetcher @Inject constructor(private val itemsQueryService: ItemsQueryService) {

  internal val itemByIdDataFetcher: DataFetcher<CompletableFuture<ItemDTO?>> = DataFetcher {
    val itemId = it.getArgument<ItemId>("id")
    GlobalScope.future {
      itemsQueryService.findById(itemId)
    }
  }

  internal val itemsBySellerDataFetcher: DataFetcher<CompletableFuture<List<ItemDTO>>> = DataFetcher {
    val sellerId = it.getArgument<ItemId>("sellerId")
    GlobalScope.future {
      itemsQueryService.findBySeller(sellerId)
    }
  }

}
