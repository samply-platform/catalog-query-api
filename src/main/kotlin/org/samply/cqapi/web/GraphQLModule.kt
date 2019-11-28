package org.samply.cqapi.web

import dagger.Module
import dagger.Provides
import graphql.GraphQL
import graphql.schema.idl.RuntimeWiring
import graphql.schema.idl.SchemaGenerator
import graphql.schema.idl.SchemaParser
import kotlinx.coroutines.FlowPreview
import java.io.File
import javax.inject.Singleton

@Module
@FlowPreview
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
  internal fun wiring(graphQLItemsDataFetcher: GraphQLItemsDataFetcher): RuntimeWiring {
    return RuntimeWiring.newRuntimeWiring()
      .type("Query") { builder ->
        builder.dataFetcher("item", graphQLItemsDataFetcher.itemByIdDataFetcher)
        builder.dataFetcher("items", graphQLItemsDataFetcher.itemsDataFetcher)
      }
      .build()
  }

}
