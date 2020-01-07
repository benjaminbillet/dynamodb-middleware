package fr.benjaminbillet.dynamo.schema;

import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;

public class LocalSecondaryKeyDefinition extends DynamoKeyDefinition {
  public LocalSecondaryKeyDefinition(PrimaryKeyDefinition primaryKey, String rangeKeyName, ScalarAttributeType rangeKeyType) {
    super(primaryKey.getHashKeyName(), primaryKey.getHashKeyType(), rangeKeyName, rangeKeyType);
  }
}
