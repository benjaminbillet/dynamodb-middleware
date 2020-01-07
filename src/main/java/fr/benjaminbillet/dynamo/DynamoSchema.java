package fr.benjaminbillet.dynamo;

import lombok.Builder;
import lombok.Getter;

import java.util.HashMap;
import java.util.Map;

@Getter
@Builder
public class DynamoSchema {
  private String tableName;
  private PrimaryKeyDefinition primaryKey;

  @Builder.Default
  private Map<String, DynamoKeyDefinition> secondaryKeys = new HashMap<>();
}
