package fr.benjaminbillet.dynamo.schema;

import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;

public class GlobalSecondaryKeyDefinition extends DynamoKeyDefinition {
  public GlobalSecondaryKeyDefinition(String hashKeyName, ScalarAttributeType hashKeyType, String rangeKeyName, ScalarAttributeType rangeKeyType) {
    super(hashKeyName, hashKeyType, rangeKeyName, rangeKeyType);
  }

  public GlobalSecondaryKeyDefinition(String hashKeyName, String rangeKeyName) {
    super(hashKeyName, ScalarAttributeType.S, rangeKeyName, ScalarAttributeType.S);
  }

  public GlobalSecondaryKeyDefinition(String hashKeyName) {
    super(hashKeyName, ScalarAttributeType.S, null, null);
  }
}
