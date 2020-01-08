package fr.benjaminbillet.dynamo.util;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import fr.benjaminbillet.dynamo.DynamoDocument;

import java.text.Normalizer;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.Map;

import static fr.benjaminbillet.dynamo.DynamoConstants.HASH_KEY;
import static fr.benjaminbillet.dynamo.DynamoConstants.RANGE_KEY;

public class AttributeUtils {

  public static String dateToString(ZonedDateTime zonedDateTime) {
    if (zonedDateTime == null) {
      return null;
    }
    return zonedDateTime.toOffsetDateTime().toString();
  }

  public static ZonedDateTime dateFromString(String string) {
    if (string == null) {
      return null;
    }
    return ZonedDateTime.parse(string);
  }

  public static String toNullableString(Object o) {
    if (o == null) {
      return null;
    }
    return String.valueOf(o);
  }

  public static Map<String, AttributeValue> keyToAttributes(String hashKey, String rangeKey) {
    DynamoDocument.AttributeMap map = new DynamoDocument.AttributeMap();
    map.put(HASH_KEY, new AttributeValue(hashKey));
    map.put(RANGE_KEY, new AttributeValue(rangeKey));
    return map;
  }

  public static String normalize(String input) {
    return Normalizer.normalize(input, Normalizer.Form.NFD).replaceAll("[^A-Za-z0-9-_:]", "");
  }

  public static String buildPrefixedKey(String prefix, String value) {
    if (value != null && value.startsWith(prefix)) {
      return normalize(value.toLowerCase().trim());
    }
    return String.format("%s:%s", prefix, normalize(value)).toLowerCase();
  }

  public static <K, V> Map<K, V> hashmap(K key, V value) {
    Map<K, V> map = new HashMap<>();
    map.put(key, value);
    return map;
  }
}
