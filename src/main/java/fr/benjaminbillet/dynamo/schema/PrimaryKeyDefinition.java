package fr.benjaminbillet.dynamo.schema;

import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;

public class PrimaryKeyDefinition extends DynamoKeyDefinition {
  public PrimaryKeyDefinition(String hashKeyName, String rangeKeyName) {
    super(hashKeyName, ScalarAttributeType.S, rangeKeyName, ScalarAttributeType.S);
  }

  public PrimaryKeyDefinition(String hashKeyName) {
    super(hashKeyName, ScalarAttributeType.S, null, null);
  }
}
