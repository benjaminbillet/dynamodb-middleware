package fr.benjaminbillet.dynamo;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.DeleteItemRequest;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.QueryResult;
import com.amazonaws.services.dynamodbv2.model.Select;
import fr.benjaminbillet.dynamo.DynamoDocument.AttributeMap;
import fr.benjaminbillet.dynamo.pagination.DocumentPage;
import fr.benjaminbillet.dynamo.pagination.Pageable;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import static fr.benjaminbillet.dynamo.DynamoConstants.HASH_KEY;
import static fr.benjaminbillet.dynamo.DynamoConstants.RANGE_KEY;

@Slf4j
public abstract class AbstractDynamoRepository {

  protected final AmazonDynamoDB amazonDynamoDB;
  protected final DynamoSchema schema;


  protected AbstractDynamoRepository(AmazonDynamoDB amazonDynamoDB, DynamoSchema schema) {
    this.amazonDynamoDB = amazonDynamoDB;
    this.schema = schema;
  }

  public List<String> listTables() {
    return amazonDynamoDB.listTables().getTableNames();
  }

  protected <T extends DynamoDocument> void save(T document) {
    amazonDynamoDB.putItem(new PutItemRequest()
      .withTableName(schema.getTableName())
      .withItem(document.toAttributeMap())
    );

    log.debug("Document {}={}, {}={} successfully inserted",
      HASH_KEY, document.getHashKey(),
      RANGE_KEY, document.getRangeKey()
    );
  }

  protected <T extends DynamoDocument> Optional<T> findByPrimaryKey(String hashKey, String rangeKey, Function<AttributeMap, T> constructor) {
    Map<String, String> attributeNames = new HashMap<>();
    attributeNames.put("#hk", HASH_KEY);
    attributeNames.put("#rk", RANGE_KEY);

    Map<String, AttributeValue> attributeValues = new HashMap<>();
    attributeValues.put(":hkValue", new AttributeValue(hashKey));
    attributeValues.put(":rkValue", new AttributeValue(rangeKey));

    if (rangeKey == null && schema.getPrimaryKey().hasRangeKey()) {
      throw new IllegalArgumentException("You must specify the range key");
    }

    QueryRequest queryRequest = new QueryRequest()
      .withTableName(schema.getTableName())
      .withKeyConditionExpression("#hk = :hkValue and #rk = :rkValue")
      .withExpressionAttributeNames(attributeNames)
      .withExpressionAttributeValues(attributeValues)
      .withSelect(Select.ALL_ATTRIBUTES);

    QueryResult queryResult = amazonDynamoDB.query(queryRequest);

    List<Map<String, AttributeValue>> results = queryResult.getItems();
    if (results == null || results.isEmpty()) {
      return Optional.empty();
    }
    return Optional.of(constructor.apply(new AttributeMap(results.get(0))));
  }

  protected <T extends DynamoDocument> Optional<T> findBySecondaryKey(String indexName, String hashKey, String rangeKey, Function<AttributeMap, T> constructor) {
    DynamoKeyDefinition indexSchema = schema.getSecondaryKeys().get(indexName);

    Map<String, String> attributeNames = new HashMap<>();
    attributeNames.put("#hk", indexSchema.getHashKeyName());
    attributeNames.put("#rk", indexSchema.getRangeKeyName());

    Map<String, AttributeValue> attributeValues = new HashMap<>();
    attributeValues.put(":hkValue", new AttributeValue(hashKey));
    attributeValues.put(":rkValue", new AttributeValue(rangeKey));

    if (rangeKey == null && indexSchema.hasRangeKey()) {
      throw new IllegalArgumentException("You must specify the range key");
    }

    QueryRequest queryRequest = new QueryRequest()
      .withTableName(schema.getTableName())
      .withIndexName(indexName)
      .withKeyConditionExpression("#hk = :hkValue and #rk = :rkValue")
      .withExpressionAttributeNames(attributeNames)
      .withExpressionAttributeValues(attributeValues)
      .withSelect(Select.ALL_ATTRIBUTES);

    QueryResult queryResult = amazonDynamoDB.query(queryRequest);

    List<Map<String, AttributeValue>> results = queryResult.getItems();
    if (results == null || results.isEmpty()) {
      return Optional.empty();
    }
    return Optional.of(constructor.apply(new AttributeMap(results.get(0))));
  }

  protected <T extends DynamoDocument> DocumentPage<T> findByHashKey(String hashKey, Pageable pageable, Function<AttributeMap, T> constructor) {
    Map<String, String> attributeNames = new HashMap<>();
    attributeNames.put("#hk", HASH_KEY);

    Map<String, AttributeValue> attributeValues = new HashMap<>();
    attributeValues.put(":hkValue", new AttributeValue(hashKey));

    QueryRequest queryRequest = new QueryRequest()
      .withTableName(schema.getTableName())
      .withKeyConditionExpression("#hk = :hkValue")
      .withExpressionAttributeNames(attributeNames)
      .withExpressionAttributeValues(attributeValues)
      .withLimit(pageable.getPageSize())
      .withSelect(Select.ALL_ATTRIBUTES);

    QueryResult queryResult = amazonDynamoDB.query(queryRequest);

    List<Map<String, AttributeValue>> results = queryResult.getItems();
    return new DocumentPage<>(results, pageable, constructor);
  }

  protected <T extends DynamoDocument> List<T> findByHashKey(String hashKey, Function<AttributeMap, T> constructor) {
    Map<String, String> attributeNames = new HashMap<>();
    attributeNames.put("#hk", HASH_KEY);

    Map<String, AttributeValue> attributeValues = new HashMap<>();
    attributeValues.put(":hkValue", new AttributeValue(hashKey));

    QueryRequest queryRequest = new QueryRequest()
      .withTableName(schema.getTableName())
      .withKeyConditionExpression("#hk = :hkValue")
      .withExpressionAttributeNames(attributeNames)
      .withExpressionAttributeValues(attributeValues)
      .withSelect(Select.ALL_ATTRIBUTES);

    QueryResult queryResult = amazonDynamoDB.query(queryRequest);

    List<Map<String, AttributeValue>> results = queryResult.getItems();
    return new DocumentPage<>(results, null, constructor).asList();
  }

  protected <T extends DynamoDocument> DocumentPage<T> findBySecondaryHashKey(String indexName, String hashKey, Pageable pageable, Function<AttributeMap, T> constructor) {
    DynamoKeyDefinition indexSchema = schema.getSecondaryKeys().get(indexName);

    Map<String, String> attributeNames = new HashMap<>();
    attributeNames.put("#hk", indexSchema.getHashKeyName());

    Map<String, AttributeValue> attributeValues = new HashMap<>();
    attributeValues.put(":hkValue", new AttributeValue(hashKey));

    QueryRequest queryRequest = new QueryRequest()
      .withTableName(schema.getTableName())
      .withIndexName(indexName)
      .withKeyConditionExpression("#hk = :hkValue")
      .withExpressionAttributeNames(attributeNames)
      .withExpressionAttributeValues(attributeValues)
      .withLimit(pageable.getPageSize())
      .withSelect(Select.ALL_ATTRIBUTES);

    QueryResult queryResult = amazonDynamoDB.query(queryRequest);

    List<Map<String, AttributeValue>> results = queryResult.getItems();
    return new DocumentPage<>(results, pageable, constructor);
  }

  protected <T extends DynamoDocument> List<T> findBySecondaryHashKey(String indexName, String hashKey, Function<AttributeMap, T> constructor) {
    DynamoKeyDefinition indexSchema = schema.getSecondaryKeys().get(indexName);

    Map<String, String> attributeNames = new HashMap<>();
    attributeNames.put("#hk", indexSchema.getHashKeyName());

    Map<String, AttributeValue> attributeValues = new HashMap<>();
    attributeValues.put(":hkValue", new AttributeValue(hashKey));

    QueryRequest queryRequest = new QueryRequest()
      .withTableName(schema.getTableName())
      .withIndexName(indexName)
      .withKeyConditionExpression("#hk = :hkValue")
      .withExpressionAttributeNames(attributeNames)
      .withExpressionAttributeValues(attributeValues)
      .withSelect(Select.ALL_PROJECTED_ATTRIBUTES);

    QueryResult queryResult = amazonDynamoDB.query(queryRequest);

    List<Map<String, AttributeValue>> results = queryResult.getItems();
    return new DocumentPage<>(results, null, constructor).asList();
  }

  protected <T extends DynamoDocument> void delete(T document) {
    Map<String, AttributeValue> keyAttributes = new HashMap<>();
    keyAttributes.put(HASH_KEY, new AttributeValue(document.getHashKey()));
    if (schema.getPrimaryKey().getRangeKey() != null) {
      keyAttributes.put(RANGE_KEY, new AttributeValue(document.getRangeKey()));
    }

    amazonDynamoDB.deleteItem(new DeleteItemRequest()
      .withTableName(schema.getTableName())
      .withKey(keyAttributes)
    );
  }

  protected <T extends DynamoDocument> void deleteByHashKey(String hashKey) {
    Map<String, String> attributeNames = new HashMap<>();
    attributeNames.put("#hk", HASH_KEY);

    Map<String, AttributeValue> attributeValues = new HashMap<>();
    attributeValues.put(":hkValue", new AttributeValue(hashKey));

    amazonDynamoDB.deleteItem(new DeleteItemRequest()
      .withTableName(schema.getTableName())
      .withConditionExpression("#hk = :hkValue")
      .withExpressionAttributeNames(attributeNames)
      .withExpressionAttributeValues(attributeValues)
    );
  }
}
