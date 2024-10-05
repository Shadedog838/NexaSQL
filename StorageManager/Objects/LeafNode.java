package StorageManager.Objects;

import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;

import StorageManager.StorageManager;
import StorageManager.TableSchema;

public class LeafNode extends Node {
  List<Bucket> buckets;
  private int nextLeafPageNumber;


  public LeafNode(int tableNumber, int pageNumber, int parentPageNumber) {
    super(tableNumber, pageNumber, true, parentPageNumber);
    this.buckets = new ArrayList<>();
    this.nextLeafPageNumber = -1;
  }

  public List<Bucket> getBuckets() {
    return buckets;
  }

  public int getNextLeafPageNumber() {
    return nextLeafPageNumber;
  }

  public void setNextLeafPageNumber(int nextLeafPageNumber) {
    this.nextLeafPageNumber = nextLeafPageNumber;
  }

  public LeafNode getNextLeaf() throws Exception {
    if (nextLeafPageNumber == -1) {
      return null;
    }

    return (LeafNode) StorageManager.getStorageManager().getNodePage(tableNumber, nextLeafPageNumber);
  }

  @Override
  public void insertKey(Object primaryKey, Object value, BPlusTree tree) throws Exception {
    int pos = findInsertPosition(primaryKey);

    primaryKeys.add(pos, primaryKey);
    buckets.add(pos, (Bucket) value);
    ++numPrimaryKeys;

    if (numPrimaryKeys > tree.getMaxKeysPerNode()) {
      splitLeaf(tree);
    }
    this.setChanged();
    this.setPriority();
  }

  private void splitLeaf(BPlusTree tree) throws Exception {
    int newRightPageNumber = Catalog.getCatalog().getSchema(tableNumber).getNumIndexPages() + 1;
    Catalog.getCatalog().getSchema(tableNumber).incrementNumIndexPages();
    LeafNode newRightNode = new LeafNode(tableNumber, newRightPageNumber, parentPageNumber);
    newRightNode.setChanged();
    StorageManager.getStorageManager().addPageToBuffer(newRightNode);


    int midIndex = numPrimaryKeys / 2;
    for (int i=midIndex; i < numPrimaryKeys; ++i) {
      newRightNode.primaryKeys.add(primaryKeys.get(i));
      newRightNode.buckets.add(buckets.get(i));
    }
    newRightNode.numPrimaryKeys = newRightNode.primaryKeys.size();

    for (int i=numPrimaryKeys - 1; i >= midIndex; --i) {
      primaryKeys.remove(i);
      buckets.remove(i);
    }

    numPrimaryKeys = primaryKeys.size();

    newRightNode.setNextLeafPageNumber(this.nextLeafPageNumber);
    this.setNextLeafPageNumber(newRightPageNumber);

    tree.insertInParent(this, newRightNode, newRightNode.primaryKeys.get(0));
  }


  @SuppressWarnings({ "rawtypes", "unchecked" })
  @Override
  public void deleteKey(Object primaryKey, BPlusTree tree) throws Exception {
    int pos = findInsertPosition(primaryKey);
    if (pos < numPrimaryKeys && ((Comparable) primaryKeys.get(pos)).compareTo(primaryKey) == 0) {
      primaryKeys.remove(pos);
      buckets.remove(pos);
      --numPrimaryKeys;
    }

    if (numPrimaryKeys < tree.getMinKeysForLeafNode(this)) {
      handleUnderflow(tree);
    }
    this.setChanged();
    this.setPriority();
  }


  private void handleUnderflow(BPlusTree tree) throws Exception {
    LeafNode leftSibling = tree.getLeftSibling(this);
    LeafNode rightSibling = tree.getRightSibling(this);

    if (leftSibling != null && leftSibling.numPrimaryKeys > tree.getMinKeysForLeafNode(leftSibling)) {
      redistributionFromLeft(leftSibling);
    } else if (rightSibling != null && rightSibling.numPrimaryKeys > tree.getMinKeysForLeafNode(rightSibling)) {
      redistributionFromRight(rightSibling);
    } else if (leftSibling != null) {
      mergeWithLeftSibling(leftSibling, tree);
    } else if (rightSibling != null) {
      mergeWithRightSibling(rightSibling, tree);
    }
  }

  private void redistributionFromLeft(LeafNode leftSibling) {
    primaryKeys.add(0, leftSibling.primaryKeys.remove(leftSibling.numPrimaryKeys - 1));
    buckets.add(0, leftSibling.buckets.remove(leftSibling.numPrimaryKeys - 1));
    leftSibling.numPrimaryKeys--;
    numPrimaryKeys++;
  }

  private void redistributionFromRight(LeafNode rightSibling) throws Exception {
    primaryKeys.add(numPrimaryKeys, rightSibling.primaryKeys.remove(0));
    buckets.add(numPrimaryKeys, rightSibling.buckets.remove(0));
    rightSibling.numPrimaryKeys--;
    numPrimaryKeys++;
  }

  private void mergeWithLeftSibling(LeafNode leftSibling, BPlusTree tree) throws Exception {
    for (int i=0; i < numPrimaryKeys; ++i) {
      leftSibling.primaryKeys.add(primaryKeys.get(i));
      leftSibling.buckets.add(buckets.get(i));
    }
    leftSibling.numPrimaryKeys += numPrimaryKeys;
    leftSibling.setNextLeafPageNumber(this.nextLeafPageNumber);
    tree.deleteInParent(this);
  }

  private void mergeWithRightSibling(LeafNode rightSibling, BPlusTree tree) throws Exception {
    for (int i=0; i < rightSibling.numPrimaryKeys; ++i) {
      primaryKeys.add(rightSibling.primaryKeys.get(i));
      buckets.add(rightSibling.buckets.get(i));
    }
    numPrimaryKeys += rightSibling.numPrimaryKeys;
    this.setNextLeafPageNumber(rightSibling.getNextLeafPageNumber());
    tree.deleteInParent(rightSibling);
  }


  @Override
  public void readFromHardware(RandomAccessFile tableAccessFile, TableSchema tableSchema) throws Exception {
    this.numPrimaryKeys = tableAccessFile.readInt();
    int numOfBuckets = tableAccessFile.readInt();
    this.nextLeafPageNumber = tableAccessFile.readInt();
    int primaryIndex = tableSchema.getPrimaryIndex();
    String primaryKeyDataType = tableSchema.getAttributes().get(primaryIndex).getDataType();

    for (int i=0; i < numPrimaryKeys; ++i) {
      if (primaryKeyDataType.equalsIgnoreCase("integer")) {
        int primaryKey = tableAccessFile.readInt();
        this.primaryKeys.add(primaryKey);
      } else if (primaryKeyDataType.equalsIgnoreCase("double")) {
        double primaryKey = tableAccessFile.readDouble();
        this.primaryKeys.add(primaryKey);
      } else if (primaryKeyDataType.equalsIgnoreCase("boolean")) {
        boolean primaryKey = tableAccessFile.readBoolean();
        this.primaryKeys.add(primaryKey);
      } else if (primaryKeyDataType.contains("char") || primaryKeyDataType.contains("varchar")) {
        String primaryKey = tableAccessFile.readUTF();
        this.primaryKeys.add(primaryKey);
      }
    }

    for (int i=0; i < numOfBuckets; ++i) {
      int pageNumber = tableAccessFile.readInt();
      int index = tableAccessFile.readInt();
      Bucket bucket = new Bucket(pageNumber, index);
      this.buckets.add(bucket);
    }
  }

  @Override
  public void writeToHardware(RandomAccessFile tableAccessFile) throws Exception {
    tableAccessFile.writeInt(pageNumber);
    tableAccessFile.writeBoolean(this.isLeaf);
    tableAccessFile.writeInt(parentPageNumber);
    tableAccessFile.writeInt(this.numPrimaryKeys);
    tableAccessFile.writeInt(this.buckets.size());
    tableAccessFile.writeInt(this.nextLeafPageNumber);

    for (Object key : primaryKeys) {
      if (key instanceof Integer) {
        tableAccessFile.writeInt((Integer) key);
      } else if (key instanceof String) {
          tableAccessFile.writeUTF((String) key);
      } else if (key instanceof Double) {
          tableAccessFile.writeDouble((Double) key);
      } else if (key instanceof Boolean) {
          tableAccessFile.writeBoolean((Boolean) key);
      }
    }

    for (Bucket bucket : buckets) {
      tableAccessFile.writeInt(bucket.getPageNumber());
      tableAccessFile.writeInt(bucket.getIndex());
    }
  }
}
