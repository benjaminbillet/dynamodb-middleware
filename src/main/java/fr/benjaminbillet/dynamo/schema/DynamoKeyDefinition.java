package fr.benjaminbillet.dynamo.schema;

import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class DynamoKeyDefinition {
  private String hashKeyName;
  private ScalarAttributeType hashKeyType;
  private String rangeKeyName;
  private ScalarAttributeType rangeKeyType;

  public boolean hasRangeKey() {
    return rangeKeyName != null && rangeKeyType != null;
  }

  public AttributeDefinition getHashKey() {
    return new AttributeDefinition(hashKeyName, hashKeyType);
  }

  public AttributeDefinition getRangeKey() {
    if (hasRangeKey()) {
      return new AttributeDefinition(rangeKeyName, rangeKeyType);
    }
    return null;
  }
}
