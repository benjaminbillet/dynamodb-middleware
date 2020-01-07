package fr.benjaminbillet.dynamo.schema;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.CreateTableResult;
import com.amazonaws.services.dynamodbv2.model.GlobalSecondaryIndex;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.LocalSecondaryIndex;
import com.amazonaws.services.dynamodbv2.model.Projection;
import com.amazonaws.services.dynamodbv2.model.ProjectionType;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@AllArgsConstructor
@Slf4j
public class DynamoSchemaBuilder {
  private final DynamoSchema schema;

  public void provision(AmazonDynamoDB amazonDynamoDB) {
    CreateTableRequest request = createMainTable();
    CreateTableResult table = amazonDynamoDB.createTable(createMainTable());
    log.info("Table created: {}", table);
  }

  public void validate(AmazonDynamoDB amazonDynamoDB) {
    // TODO
  }

  private CreateTableRequest createMainTable() {
    List<GlobalSecondaryIndex> globalSecondaryIndexes = schema.getSecondaryKeys().keySet().stream()
      .filter(name -> GlobalSecondaryKeyDefinition.class == schema.getSecondaryKeys().get(name).getClass())
      .map(this::createGlobalSecondaryIndex)
      .collect(Collectors.toList());

    List<LocalSecondaryIndex> localSecondaryIndexes = schema.getSecondaryKeys().keySet().stream()
      .filter(name -> LocalSecondaryKeyDefinition.class == schema.getSecondaryKeys().get(name).getClass())
      .map(this::createLocalSecondaryIndex)
      .collect(Collectors.toList());

    Predicate<Object> notNull = x -> x != null;

    List<AttributeDefinition> attributeDefinitions = new ArrayList<>();
    attributeDefinitions.add(schema.getPrimaryKey().getHashKey());
    attributeDefinitions.add(schema.getPrimaryKey().getRangeKey());
    attributeDefinitions.addAll(schema.getSecondaryKeys().values().stream()
      .flatMap(k -> Stream.of(k.getHashKey(), k.getRangeKey()).filter(notNull))
      .collect(Collectors.toList()));

    List<KeySchemaElement> keySchema = new ArrayList<>();
    keySchema.add(new KeySchemaElement(schema.getPrimaryKey().getHashKeyName(), KeyType.HASH));
    if (schema.getPrimaryKey().hasRangeKey()) {
      keySchema.add(new KeySchemaElement(schema.getPrimaryKey().getRangeKeyName(), KeyType.RANGE));
    }

    CreateTableRequest createTableRequest = new CreateTableRequest()
      .withAttributeDefinitions(attributeDefinitions)
      .withKeySchema(keySchema)
      .withTableName(schema.getTableName());

    if (!globalSecondaryIndexes.isEmpty()) {
      createTableRequest.withGlobalSecondaryIndexes(globalSecondaryIndexes);
    }
    if (!localSecondaryIndexes.isEmpty()) {
      createTableRequest.withLocalSecondaryIndexes(localSecondaryIndexes);
    }

    return createTableRequest;
  }

  private GlobalSecondaryIndex createGlobalSecondaryIndex(String indexName) {
    DynamoKeyDefinition key = schema.getSecondaryKeys().get(indexName);
    List<KeySchemaElement> indexKeySchema = new ArrayList<>();
    indexKeySchema.add(new KeySchemaElement(key.getHashKeyName(), KeyType.HASH));
    if (key.hasRangeKey()) {
      indexKeySchema.add(new KeySchemaElement(key.getRangeKeyName(), KeyType.RANGE));
    }

    return new GlobalSecondaryIndex()
      .withIndexName(indexName)
      .withKeySchema(indexKeySchema)
      .withProjection(new Projection().withProjectionType(ProjectionType.KEYS_ONLY));
  }

  private LocalSecondaryIndex createLocalSecondaryIndex(String indexName) {
    DynamoKeyDefinition key = schema.getSecondaryKeys().get(indexName);
    List<KeySchemaElement> indexKeySchema = Arrays.asList(
      new KeySchemaElement(key.getHashKeyName(), KeyType.HASH),
      new KeySchemaElement(key.getRangeKeyName(), KeyType.RANGE)
    );

    return new LocalSecondaryIndex()
      .withIndexName(indexName)
      .withKeySchema(indexKeySchema)
      .withProjection(new Projection().withProjectionType(ProjectionType.KEYS_ONLY));
  }
}
