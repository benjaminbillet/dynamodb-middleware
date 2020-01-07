package fr.benjaminbillet.dynamo;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import lombok.Data;
import lombok.ToString;

import java.time.ZonedDateTime;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static fr.benjaminbillet.dynamo.DynamoConstants.CREATED;
import static fr.benjaminbillet.dynamo.DynamoConstants.HASH_KEY;
import static fr.benjaminbillet.dynamo.DynamoConstants.RANGE_KEY;
import static fr.benjaminbillet.dynamo.DynamoConstants.UPDATED;
import static fr.benjaminbillet.dynamo.util.AttributeUtils.dateFromString;
import static fr.benjaminbillet.dynamo.util.AttributeUtils.dateToString;

@Data
public abstract class DynamoDocument {

  private String hashKey;
  private String rangeKey;

  private ZonedDateTime created;
  private ZonedDateTime updated;

  protected DynamoDocument() {
    created = ZonedDateTime.now();
    updated = created;
  }

  public DynamoDocument(AttributeMap map) {
    this.hashKey = map.getString(HASH_KEY);
    this.rangeKey = map.getString(RANGE_KEY);
    this.created = map.getDate(CREATED);
    this.updated = map.getDate(UPDATED);
  }

  public AttributeMap toAttributeMap() {
    AttributeMap map = new AttributeMap();
    map.put(HASH_KEY, new AttributeValue(getHashKey()));
    if (getRangeKey() != null) {
      map.put(RANGE_KEY, new AttributeValue(getRangeKey()));
    }
    map.put(CREATED, new AttributeValue(dateToString(getCreated())));
    map.put(UPDATED, new AttributeValue(dateToString(getUpdated())));
    return map;
  }

  @ToString
  public static class AttributeMap implements Map<String, AttributeValue> {
    private Map<String, AttributeValue> attributes;

    public AttributeMap() {
      this.attributes = new HashMap<>();
    }

    public AttributeMap(Map<String, AttributeValue> attributes) {
      this.attributes = attributes;
    }

    @Override
    public int size() {
      return attributes.size();
    }

    @Override
    public boolean isEmpty() {
      return attributes.isEmpty();
    }

    @Override
    public boolean containsKey(Object key) {
      return attributes.containsKey(key);
    }

    @Override
    public boolean containsValue(Object value) {
      return attributes.containsValue(value);
    }

    @Override
    public AttributeValue get(Object key) {
      return attributes.get(key);
    }

    @Override
    public AttributeValue put(String key, AttributeValue value) {
      return attributes.put(key, value);
    }

    @Override
    public AttributeValue remove(Object key) {
      return attributes.remove(key);
    }

    @Override
    public void putAll(Map<? extends String, ? extends AttributeValue> m) {
      attributes.putAll(m);
    }

    @Override
    public void clear() {
      attributes.clear();
    }

    @Override
    public Set<String> keySet() {
      return attributes.keySet();
    }

    @Override
    public Collection<AttributeValue> values() {
      return attributes.values();
    }

    @Override
    public Set<Entry<String, AttributeValue>> entrySet() {
      return attributes.entrySet();
    }

    public String getString(String key) {
      if (attributes.containsKey(key)) {
        return attributes.get(key).getS();
      }
      return null;
    }

    public Long getLong(String key) {
      if (attributes.containsKey(key)) {
        try {
          return Long.valueOf(attributes.get(key).getS());
        } catch (NumberFormatException | NullPointerException e) {
        }
      }
      return null;
    }

    public Double getDouble(String key) {
      if (attributes.containsKey(key)) {
        try {
          return Double.valueOf(attributes.get(key).getS());
        } catch (NumberFormatException | NullPointerException e) {
        }
      }
      return null;
    }

    public ZonedDateTime getDate(String key) {
      if (attributes.containsKey(key)) {
        try {
          return dateFromString(attributes.get(key).getS());
        } catch (NumberFormatException | NullPointerException e) {
        }
      }
      return null;
    }
  }
}
