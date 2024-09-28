package StorageManager.Objects;

import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;

import StorageManager.StorageManager;
import StorageManager.TableSchema;

public class InternalNode extends Node {
  private List<Integer> childrenPointers;

  public InternalNode(int tableNumber, int pageNumber, int parentPageNumber) {
    super(tableNumber, pageNumber, false, parentPageNumber);
    this.childrenPointers = new ArrayList<>();
  }

  public List<Integer> getChildrenPointers() {
    return childrenPointers;
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  @Override
  public void insertKey(Object primaryKey, Object value, BPlusTree tree) throws Exception {
    int pos = findInsertPosition(primaryKey);

    if (value instanceof Integer) {
      primaryKeys.add(pos, primaryKey);
      this.setChanged();
      this.setPriority();
      return;
    }

    if (((Comparable) primaryKey).compareTo(this.getPrimaryKeys().get(pos)) > 0) {
      ++pos; // got to the right child
    }

    int childPageNumber = childrenPointers.get(pos);
    Node childNode = StorageManager.getStorageManager().getNodePage(tableNumber, childPageNumber);


    childNode.insertKey(primaryKey, value, tree);

    if (childNode.numPrimaryKeys > tree.getMaxKeysPerNode()) {
      splitChild(pos, childNode, tree);
    }
    this.setChanged();
    this.setPriority();
  }

  private void splitChild(int index, Node child, BPlusTree tree) throws Exception {
    int midIndex = child.numPrimaryKeys / 2;
    Object midKey = child.getPrimaryKeys().get(midIndex);

    InternalNode newRightNode = new InternalNode(tableNumber, Catalog.getCatalog().getSchema(tableNumber).getNumIndexPages() + 1, parentPageNumber);
    Catalog.getCatalog().getSchema(tableNumber).incrementNumIndexPages();
    newRightNode.setChanged();
    StorageManager.getStorageManager().addPageToBuffer(newRightNode);

    for (int i = midIndex + 1; i < child.numPrimaryKeys; ++i) {
      newRightNode.primaryKeys.add(child.primaryKeys.get(i));
    }

    newRightNode.numPrimaryKeys = newRightNode.primaryKeys.size();
    child.numPrimaryKeys = midIndex;

    primaryKeys.add(index, midKey);
    childrenPointers.add(index + 1, newRightNode.getPageNumber());
    ++numPrimaryKeys;

    newRightNode.setParentPageNumber(this.getPageNumber());
    child.setParentPageNumber(this.getParentPageNumber());

    if (numPrimaryKeys > tree.getMaxKeysPerNode()) {
      tree.splitRoot(this);
    }
  }


  @SuppressWarnings({ "rawtypes", "unchecked" })
  @Override
  public void deleteKey(Object primaryKey, BPlusTree tree) throws Exception {
    int pos = findInsertPosition(primaryKey);

    if (pos < numPrimaryKeys && ((Comparable) primaryKeys.get(pos)).compareTo(primaryKey) == 0) {
      primaryKeys.remove(pos);
      childrenPointers.remove(pos + 1); // Remove corresponding child pointer
      numPrimaryKeys--;

      if (numPrimaryKeys < tree.getMinKeysPerNode()) {
        handleUnderflow(tree);
      }
    } else {
      if (((Comparable) primaryKey).compareTo(this.getPrimaryKeys().get(pos)) > 0 || ((Comparable) primaryKey).compareTo(this.getPrimaryKeys().get(pos)) == 0) {
        ++pos; // got to the right child
      }
      int childPageNumber = childrenPointers.get(pos);
      Node childNode = StorageManager.getStorageManager().getNodePage(tableNumber, childPageNumber);
      childNode.deleteKey(primaryKey, tree);

      if (childNode.getNumKeys() < tree.getMinKeysPerNode()) {
        handleChildUnderflow(pos, childNode, tree);
      }
    }
    this.setChanged();
    this.setPriority();
  }

  private void handleUnderflow(BPlusTree tree) throws Exception {
    InternalNode leftSibling = tree.getLeftInternalSibling(this);
    InternalNode rightSibling = tree.getRightInternalSibling(this);

    if (leftSibling != null && leftSibling.numPrimaryKeys > tree.getMinKeysPerNode()) {
      redistributeFromLeft(leftSibling);
    } else if (rightSibling != null && rightSibling.numPrimaryKeys > tree.getMinKeysPerNode()) {
      redistributeFromRight(rightSibling);
    } else if (leftSibling != null) {
      mergeWithLeftSibling(leftSibling, tree);
    } else if (rightSibling != null) {
      mergeWithRightSibling(rightSibling, tree);
    }
  }

  private void redistributeFromLeft(InternalNode leftSibling) {
    Object borrowedKey = leftSibling.primaryKeys.remove(leftSibling.numPrimaryKeys - 1);
    primaryKeys.add(0, borrowedKey);

    int borrowedPointer = leftSibling.getChildrenPointers().remove(leftSibling.numPrimaryKeys);
    childrenPointers.add(0, borrowedPointer);

    leftSibling.numPrimaryKeys--;
    numPrimaryKeys++;
  }


  private void redistributeFromRight(InternalNode rightSibling) {
    Object borrowedKey = rightSibling.primaryKeys.remove(0);
    primaryKeys.add(numPrimaryKeys, borrowedKey);

    // Borrow the corresponding child pointer from the right sibling
    int borrowedPointer = rightSibling.getChildrenPointers().remove(0);
    childrenPointers.add(numPrimaryKeys + 1, borrowedPointer);

    rightSibling.numPrimaryKeys--;
    numPrimaryKeys++;
  }

  private void mergeWithLeftSibling(InternalNode leftSibling, BPlusTree tree) throws Exception {
    for (int i = 0; i < numPrimaryKeys; i++) {
        leftSibling.primaryKeys.add(primaryKeys.get(i));
        leftSibling.childrenPointers.add(childrenPointers.get(i + 1));
    }
    leftSibling.numPrimaryKeys += numPrimaryKeys;

    tree.deleteInParent(this);
  }

  private void mergeWithRightSibling(InternalNode rightSibling, BPlusTree tree) throws Exception {
      for (int i = 0; i < rightSibling.numPrimaryKeys; i++) {
          primaryKeys.add(rightSibling.primaryKeys.get(i));
          childrenPointers.add(rightSibling.childrenPointers.get(i + 1));
      }
      numPrimaryKeys += rightSibling.numPrimaryKeys;

      tree.deleteInParent(rightSibling);
  }

  private void handleChildUnderflow(int pos, Node childNode, BPlusTree tree) throws Exception {
    InternalNode leftSibling = tree.getLeftInternalSibling(this);
    InternalNode rightSibling = tree.getRightInternalSibling(this);

    if (leftSibling != null && leftSibling.numPrimaryKeys > tree.getMinKeysPerNode()) {
        redistributeFromLeft(leftSibling);
    } else if (rightSibling != null && rightSibling.numPrimaryKeys > tree.getMinKeysPerNode()) {
        redistributeFromRight(rightSibling);
    } else if (leftSibling != null) {
        mergeWithLeftSibling(leftSibling, tree);
    } else if (rightSibling != null) {
        mergeWithRightSibling(rightSibling, tree);
    }
  }


  @Override
  public void readFromHardware(RandomAccessFile tableAccessFile, TableSchema tableSchema) throws Exception {
    this.numPrimaryKeys = tableAccessFile.readInt();
    int numOfChildren = tableAccessFile.readInt();

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

    for (int i=0; i < numOfChildren; ++i) {
      int childPageNumber = tableAccessFile.readInt();
      this.childrenPointers.add(childPageNumber);
    }
  }

  @Override
  public void writeToHardware(RandomAccessFile tableAccessFile) throws Exception {
    tableAccessFile.writeInt(pageNumber);
    tableAccessFile.writeBoolean(this.isLeaf);
    tableAccessFile.writeInt(parentPageNumber);
    tableAccessFile.writeInt(this.numPrimaryKeys);
    tableAccessFile.writeInt(this.childrenPointers.size());

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

    for (int childPageNumber : this.childrenPointers) {
      tableAccessFile.writeInt(childPageNumber);
    }
  }
}
