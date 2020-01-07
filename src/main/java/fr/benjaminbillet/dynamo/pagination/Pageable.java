package fr.benjaminbillet.dynamo.pagination;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor(access = AccessLevel.PACKAGE)
public class Pageable {
  private String rangeKeyName;
  private String first;
  private String last;
  // private boolean isFull; // TODO deal with last_evaluated_key
  private int pageSize;
  private Traversal traversal; // TODO deal with traversal

  Pageable(Pageable pageable, String first, String last) {
    this.rangeKeyName = pageable.rangeKeyName;
    this.pageSize = pageable.pageSize;
    this.traversal = pageable.traversal;
    this.first = first;
    this.last = last;
  }

  public static Pageable first(String rangeKeyName, int pageSize, Traversal traversal) {
    return new Pageable(rangeKeyName, null, null, pageSize, traversal);
  }

  public boolean isFirstPage() {
    return first == null && last == null;
  }
}
