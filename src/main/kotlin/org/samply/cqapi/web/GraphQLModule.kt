package org.samply.cqapi.web

import dagger.Module
import dagger.Provides
import graphql.GraphQL
import graphql.schema.DataFetcher
import graphql.schema.idl.RuntimeWiring
import graphql.schema.idl.SchemaGenerator
import graphql.schema.idl.SchemaParser
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.future.future
import org.samply.cqapi.domain.ItemDTO
import org.samply.cqapi.domain.ItemId
import org.samply.cqapi.domain.ItemsQueryService
import java.io.File
import java.util.concurrent.CompletableFuture
import javax.inject.Singleton

@Module
class GraphQLModule {

  @Provides
  @Singleton
  fun graphQL(wiring: RuntimeWiring): GraphQL {
    val schemaFile = File(this::class.java.getResource("/graphql/schema.graphqls").toURI())
    val schema = SchemaParser().parse(schemaFile)
    val makeExecutableSchema = SchemaGenerator().makeExecutableSchema(schema, wiring)

    return GraphQL.newGraphQL(makeExecutableSchema).build()
  }

  @Provides
  @Singleton
  fun wiring(itemsQueryService: ItemsQueryService): RuntimeWiring {
    return RuntimeWiring.newRuntimeWiring()
      .type("Query") { builder ->
        builder.dataFetcher("item", itemByIdDataFetcher(itemsQueryService))
      }
      .build()
  }

  private fun itemByIdDataFetcher(itemsQueryService: ItemsQueryService): DataFetcher<CompletableFuture<ItemDTO?>> {
    return DataFetcher {
      val itemId = it.getArgument<ItemId>("id")
      GlobalScope.future {
        itemsQueryService.findById(itemId)
      }
    }
  }

}
