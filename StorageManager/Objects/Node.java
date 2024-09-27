package StorageManager.Objects;

import java.util.ArrayList;
import java.util.List;

public abstract class Node extends BufferPage {
  protected boolean isLeaf;
  protected int numPrimaryKeys;
  protected List<Object> primaryKeys;
  protected int parentPageNumber;

  public Node(int tableNumber, int pageNumber, boolean isLeaf, int parentPageNumber) {
      super(tableNumber, pageNumber);
      this.isLeaf = isLeaf;
      this.numPrimaryKeys = 0;
      this.primaryKeys = new ArrayList<>();
      this.changed = false;
      this.parentPageNumber = parentPageNumber;
  }

  public boolean isLeaf() {
      return isLeaf;
  }

  public int getNumKeys() {
      return numPrimaryKeys;
  }

  public List<Object> getPrimaryKeys() {
    return primaryKeys;
  }

  public int getParentPageNumber() {
    return parentPageNumber;
  }

  public void setParentPageNumber(int parentPageNumber) {
    this.parentPageNumber = parentPageNumber;
  }

  public abstract void insertKey(Object primaryKey, Object value, BPlusTree tree) throws Exception;
  public abstract void deleteKey(Object primaryKey, BPlusTree tree) throws Exception;


  @SuppressWarnings({ "rawtypes", "unchecked" })
public int findInsertPosition(Object primaryKey) {
    int i = 0;
    while (i < numPrimaryKeys && ((Comparable) primaryKeys.get(i)).compareTo(primaryKey) < 0) {
        i++;
    }
    return i;
}
}

