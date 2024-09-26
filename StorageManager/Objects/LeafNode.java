package StorageManager.Objects;

import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;

import StorageManager.TableSchema;

public class LeafNode<K extends Comparable<K>> extends Node<K> {
  List<Bucket> buckets;
  LeafNode<K> next;

  public LeafNode(int tableNumber, int pageNumber) {
    super(tableNumber, pageNumber);
    this.isLeaf = true;
    this.primaryKeys = new ArrayList<>();
    this.buckets = new ArrayList<>();
    this.next = null;
  }

  @Override
  public boolean isLeaf() {
    return true;
  }

  public void insert(K primaryKey, Bucket bucket) {
    int index = 0;
    while (index < primaryKeys.size() && primaryKeys.get(index).compareTo(primaryKey) < 0) {
      ++index;
    }
    primaryKeys.add(primaryKey);
    buckets.add(index, bucket);
  }

  public Bucket search(K primaryKey) {
    for (int i=0; i < primaryKeys.size(); ++i) {
      if (primaryKeys.get(i).compareTo(primaryKey) == 0) {
        return buckets.get(i);
      }
    }

    return null;
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
