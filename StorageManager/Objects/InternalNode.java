package StorageManager.Objects;

import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;

import StorageManager.StorageManager;
import StorageManager.TableSchema;

public class InternalNode<K extends Comparable<K>> extends Node<K> {
  List<Integer> childrenPageNumbers;

  public InternalNode(int tableNumber, int pageNumber) {
    super(tableNumber, pageNumber);
    this.isLeaf = false;
    this.primaryKeys = new ArrayList<>();
    this.childrenPageNumbers = new ArrayList<>();
  }

  @Override
  public boolean isLeaf() {
    return false;
  }

  public void insertKey(K primaryKey) {
    int index = 0;
    while (index < primaryKeys.size() && primaryKeys.get(index).compareTo(primaryKey) < 0) {
      ++index;
    }
    primaryKeys.add(index, primaryKey);
  }

  @SuppressWarnings("unchecked")
  public void addChild(int childPageNumber) throws Exception {
    int index = 0;
    StorageManager storageManager = StorageManager.getStorageManager();
    Node<K> child = storageManager.getNodePage(this.tableNumber, childPageNumber);
    while (index < childrenPageNumbers.size() && ((Comparable<K>) storageManager.getNodePage(this.tableNumber, childrenPageNumbers.get(index)).primaryKeys.get(0)).compareTo(child.primaryKeys.get(0)) < 0) {
      ++index;
    }
    childrenPageNumbers.add(index, childPageNumber);
    child.parentPageNumber = this.pageNumber;
  }

  @Override
  public void readFromHardware(RandomAccessFile tableAccessFile, TableSchema tableSchema) throws Exception {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("Unimplemented method 'readFromHardware'");
  }

  @Override
  public void writeToHardware(RandomAccessFile tableAccessFile) throws Exception {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("Unimplemented method 'writeToHardware'");
  }

}
