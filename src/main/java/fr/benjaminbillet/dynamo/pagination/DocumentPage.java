package fr.benjaminbillet.dynamo.pagination;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import fr.benjaminbillet.dynamo.DynamoDocument;
import fr.benjaminbillet.dynamo.DynamoDocument.AttributeMap;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class DocumentPage<T extends DynamoDocument> {

  private List<Map<String, AttributeValue>> results;
  private Function<AttributeMap, T> constructor;
  private Pageable pageable;
  private String rangeKeyName;
  private boolean hasLastEvaluatedKey;

  public DocumentPage(List<Map<String, AttributeValue>> results, String rangeKeyName, boolean hasLastEvaluatedKey, Pageable pageable, Function<AttributeMap, T> constructor) {
    this.pageable = pageable;
    this.constructor = constructor;
    this.rangeKeyName = rangeKeyName;
    this.hasLastEvaluatedKey = hasLastEvaluatedKey;
    if (results == null) {
      this.results = Collections.emptyList();
    } else {
      this.results = results;
    }
  }

  public int size() {
    return results.size();
  }

  public String getLastRangeKey() {
    if (isEmpty()) {
      return null;
    }
    return results.get(results.size() - 1).get(rangeKeyName).getS();
  }

  public String getFirstRangeKey() {
    if (isEmpty()) {
      return null;
    }
    return results.get(0).get(rangeKeyName).getS();
  }

  public boolean isEmpty() {
    return results.isEmpty();
  }

  public Stream<T> stream() {
    return results.stream().map(AttributeMap::new).map(constructor);
  }

  public List<T> asList() {
    return stream().collect(Collectors.toList());
  }

  public Pageable nextPage() {
    if (hasNext()) {
      return new Pageable(pageable, getLastRangeKey(), null);
    }
    throw new NoSuchElementException();
  }

  public Pageable previousPage() {
    if (hasPrevious()) {
      return new Pageable(pageable, null, getFirstRangeKey());
    }
    throw new NoSuchElementException();
  }

  public boolean hasNext() {
    if (isEmpty()) {
      return false;
    }
    // return !getLastRangeKey().equals(pageable.getFirst());
    return hasLastEvaluatedKey;
  }

  public boolean hasPrevious() {
    if (pageable.isFirstPage() || isEmpty()) {
      return false;
    }
    return !getFirstRangeKey().equals(pageable.getLast());
  }
}
