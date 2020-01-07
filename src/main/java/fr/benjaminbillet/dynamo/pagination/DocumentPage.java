package fr.benjaminbillet.dynamo.pagination;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import fr.benjaminbillet.dynamo.DynamoDocument;
import fr.benjaminbillet.dynamo.DynamoDocument.AttributeMap;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class DocumentPage<T extends DynamoDocument> {

  private List<Map<String, AttributeValue>> results;
  private Function<AttributeMap, T> constructor;
  private Pageable pageable;

  public DocumentPage(List<Map<String, AttributeValue>> results, Pageable pageable, Function<AttributeMap, T> constructor) {
    this.pageable = pageable;
    this.constructor = constructor;
    if (results == null) {
      this.results = Collections.emptyList();
    } else {
      this.results = results;
    }
  }

  public String getLastRangeKey() {
    return results.get(results.size()).get(pageable.getRangeKeyName()).getS();
  }

  public String getFirstRangeKey() {
    return results.get(0).get(pageable.getRangeKeyName()).getS();
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
    return new Pageable(pageable, getLastRangeKey(), null);
  }

  public Pageable previousPage() {
    return new Pageable(pageable, null, getFirstRangeKey());
  }

  public boolean hasNext() {
    return getLastRangeKey().equals(pageable.getFirst());
  }

  public boolean hasPrevious() {
    if (pageable.isFirstPage()) {
      return false;
    }
    return getFirstRangeKey().equals(pageable.getLast());
  }
}
