package StorageManager.Objects;

import java.util.List;

public abstract class Node<K extends Comparable<K>> extends BufferPage {
  protected boolean isLeaf;
  protected int parentPageNumber;
  protected List<K> primaryKeys;

  public Node(int tableNumber, int pageNumber) {
    super(tableNumber, pageNumber);
  }

  public abstract boolean isLeaf();
}

