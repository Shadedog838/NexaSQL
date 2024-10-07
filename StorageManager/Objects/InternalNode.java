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

  @Override
  public void insertKey(Object primaryKey, Object value, BPlusTree tree) throws Exception {
    int pos = findInsertPosition(primaryKey);

    if (value instanceof Integer) {
      primaryKeys.add(pos, primaryKey);
      ++numPrimaryKeys;

      if (numPrimaryKeys > tree.getMaxKeysPerNode()) {
        splitInternalNode(tree);
      }
      this.setChanged();
      this.setPriority();
      return;
    }

    int childPageNumber = childrenPointers.get(pos);
    Node childNode = StorageManager.getStorageManager().getNodePage(tableNumber, childPageNumber);

    childNode.insertKey(primaryKey, value, tree);

    this.setChanged();
    this.setPriority();
  }

  private void splitInternalNode(BPlusTree tree) throws Exception {
    int midIndex = numPrimaryKeys / 2;
    InternalNode newRightNode = new InternalNode(tableNumber,
        Catalog.getCatalog().getSchema(tableNumber).getNumIndexPages() + 1, parentPageNumber);
    Catalog.getCatalog().getSchema(tableNumber).incrementNumIndexPages();
    newRightNode.setChanged();
    StorageManager.getStorageManager().addPageToBuffer(newRightNode);

    for (int i = midIndex + 1; i < numPrimaryKeys; i++) {
      newRightNode.primaryKeys.add(primaryKeys.get(i));
    }
    newRightNode.childrenPointers.addAll(childrenPointers.subList(midIndex, childrenPointers.size()));

    // update parent for new right node child pointers
    for (int childPageNumber : newRightNode.getChildrenPointers()) {
      Node childNode = StorageManager.getStorageManager().getNodePage(tableNumber, childPageNumber);
      childNode.setParentPageNumber(newRightNode.getPageNumber());
    }

    newRightNode.numPrimaryKeys = newRightNode.primaryKeys.size();

    primaryKeys = primaryKeys.subList(0, midIndex + 1); // Retain only the left half
    childrenPointers = childrenPointers.subList(0, midIndex);
    numPrimaryKeys = primaryKeys.size();

    // Promote the middle key to the parent
    Object middleKey = primaryKeys.get(midIndex);
    if (this == tree.getRoot()) {
      // If this node is the root, we need to create a new root
      InternalNode newRoot = new InternalNode(tableNumber,
          Catalog.getCatalog().getSchema(tableNumber).getNumIndexPages() + 1, -1);
      Catalog.getCatalog().getSchema(tableNumber).incrementNumIndexPages();
      newRoot.setChanged();
      StorageManager.getStorageManager().addPageToBuffer(newRoot);
      newRoot.primaryKeys.add(middleKey);
      primaryKeys.remove(middleKey);
      --numPrimaryKeys;
      newRoot.numPrimaryKeys++;
      newRoot.childrenPointers.add(this.pageNumber);
      newRoot.childrenPointers.add(newRightNode.getPageNumber());
      this.setParentPageNumber(newRoot.getPageNumber());
      newRightNode.setParentPageNumber(newRoot.getPageNumber());

      tree.setRoot(newRoot); // Set the new root
      Catalog.getCatalog().getSchema(tableNumber).setRoot(newRoot.pageNumber);
    } else {
      // Otherwise, insert the middle key into the parent node
      InternalNode parent = (InternalNode) StorageManager.getStorageManager().getNodePage(tableNumber,
          parentPageNumber);
      parent.insertKey(middleKey, newRightNode.getPageNumber(), tree);
      if (parent.getPrimaryKeys().indexOf(middleKey) == parent.numPrimaryKeys - 1) {
        parent.getChildrenPointers().add(newRightNode.getPageNumber());
      } else {
        parent.getChildrenPointers().add(parent.getPrimaryKeys().indexOf(middleKey) + 1, newRightNode.getPageNumber());
      }
    }

    // Mark the current node and new right node as changed
    this.setChanged();
    newRightNode.setChanged();
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  @Override
  public void deleteKey(Object primaryKey, BPlusTree tree) throws Exception {
    int pos = findInsertPosition(primaryKey);

    if (pos < numPrimaryKeys && ((Comparable) primaryKeys.get(pos)).compareTo(primaryKey) == 0) {
      int rightChildPageNumber = childrenPointers.get(pos + 1);
      Node rightChild = StorageManager.getStorageManager().getNodePage(tableNumber, rightChildPageNumber);

      rightChild.deleteKey(primaryKey, tree);

      if (rightChild.numPrimaryKeys != 0) {
        Object successorKey = findSuccessor(rightChild, tree);
        primaryKeys.set(pos, successorKey);
      }
    } else {
      int childPageNumber = childrenPointers.get(pos);
      Node childNode = StorageManager.getStorageManager().getNodePage(tableNumber, childPageNumber);
      childNode.deleteKey(primaryKey, tree);
      if (childNode.numPrimaryKeys - 1 < tree.getMinKeysForLeafNode(childNode)
          && pos < this.getChildrenPointers().size() && childNode.numPrimaryKeys != 0) {
        // child was underfull
        if (pos > 0) {
          Object successorKey = findSuccessor(childNode, tree);
          primaryKeys.set(pos - 1, successorKey);
        } else {
          if (childNode.isLeaf()) {
            LeafNode rightChildNode = tree.getRightSibling((LeafNode) childNode);
            Object successorKey = findSuccessor(rightChildNode, tree);
            primaryKeys.set(pos, successorKey);

          } else {
            InternalNode rightChildNode = tree.getRightInternalSibling((InternalNode) childNode);
            Object successorKey = findSuccessor(rightChildNode, tree);
            primaryKeys.set(pos, successorKey);
          }
        }
      }
    }

    if (numPrimaryKeys < tree.getMinKeysForNode(this) && this.numPrimaryKeys != 0) {
      handleUnderflow(tree);
    }

    this.setChanged();
    this.setPriority();
  }

  private Object findSuccessor(Node node, BPlusTree tree) throws Exception {
    while (!node.isLeaf()) {
      InternalNode internalNode = (InternalNode) node;
      int leftmostChildPageNumber = internalNode.getChildrenPointers().get(0);
      node = StorageManager.getStorageManager().getNodePage(tableNumber, leftmostChildPageNumber);
    }
    LeafNode leafNode = (LeafNode) node;
    return leafNode.getPrimaryKeys().get(0); // The smallest key in the leaf node
  }

  private void handleUnderflow(BPlusTree tree) throws Exception {
    InternalNode leftSibling = tree.getLeftInternalSibling(this);
    InternalNode rightSibling = tree.getRightInternalSibling(this);

    if (leftSibling != null && leftSibling.numPrimaryKeys > tree.getMinKeysForNode(leftSibling)) {
      redistributeFromLeft(leftSibling);
    } else if (rightSibling != null && rightSibling.numPrimaryKeys > tree.getMinKeysForNode(rightSibling)) {
      redistributeFromRight(rightSibling);
    } else if (leftSibling != null) {
      mergeWithLeftSibling(leftSibling, tree);
    } else if (rightSibling != null) {
      mergeWithRightSibling(rightSibling, tree);
    }

    // Special case: if this was the root and it became underfull
    if (this == tree.getRoot() && numPrimaryKeys == 0) {
      if (!childrenPointers.isEmpty()) {
        // If there's only one child left, make it the new root
        tree.setRoot(StorageManager.getStorageManager().getNodePage(tableNumber, childrenPointers.get(0)));
        Catalog.getCatalog().getSchema(tableNumber).setRoot(StorageManager.getStorageManager().getNodePage(tableNumber, childrenPointers.get(0)).pageNumber);
      } else {
        tree.setRoot(null); // Empty tree
        Catalog.getCatalog().getSchema(tableNumber).setRoot(1);
        Catalog.getCatalog().getSchema(tableNumber).setNumIndexPages(0);
      }
    }
  }

  private void redistributeFromLeft(InternalNode leftSibling) throws Exception {
    Object borrowedKey = leftSibling.primaryKeys.remove(leftSibling.numPrimaryKeys - 1);
    InternalNode parent = (InternalNode) StorageManager.getStorageManager().getNodePage(tableNumber, this.getParentPageNumber());
    Object keyBeingReplaced = parent.getPrimaryKeys().get(parent.numPrimaryKeys - 1); // get the last key in parent
    parent.getPrimaryKeys().set(parent.numPrimaryKeys - 1, borrowedKey);


    this.getPrimaryKeys().add(0, keyBeingReplaced); // bring the parent key down to the child
    int borrowedPointer = leftSibling.getChildrenPointers().remove(leftSibling.numPrimaryKeys);
    childrenPointers.add(0, borrowedPointer);

    Node node = StorageManager.getStorageManager().getNodePage(tableNumber, borrowedPointer);
    node.setParentPageNumber(this.getPageNumber());

    leftSibling.numPrimaryKeys--;
    numPrimaryKeys++;
    leftSibling.setChanged();
  }

  private void redistributeFromRight(InternalNode rightSibling) throws Exception {
    Object borrowedKey = rightSibling.primaryKeys.remove(0);

    InternalNode parent = (InternalNode) StorageManager.getStorageManager().getNodePage(tableNumber, this.getParentPageNumber());
    Object keyBeingReplaced = parent.getPrimaryKeys().get(parent.getChildrenPointers().indexOf(this.getPageNumber())); // Get the key that separates this node from the right sibling
    parent.getPrimaryKeys().set(parent.getChildrenPointers().indexOf(this.getPageNumber()), borrowedKey);

    // Bring the parent's key down to the current node
    this.getPrimaryKeys().add(keyBeingReplaced);

    int borrowedPointer = rightSibling.getChildrenPointers().remove(0);
    childrenPointers.add(borrowedPointer);

    Node node = StorageManager.getStorageManager().getNodePage(tableNumber, borrowedPointer);
    node.setParentPageNumber(this.getPageNumber());

    rightSibling.numPrimaryKeys--;
    numPrimaryKeys++;

    rightSibling.setChanged();
    this.setChanged();
  }

  private void mergeWithLeftSibling(InternalNode leftSibling, BPlusTree tree) throws Exception {
    // Move all keys and pointers from this node into the left sibling
    leftSibling.getPrimaryKeys().add(findSuccessor(this, tree));
    leftSibling.numPrimaryKeys ++;

    leftSibling.primaryKeys.addAll(primaryKeys);
    leftSibling.childrenPointers.addAll(childrenPointers);
    // update the pointers parent
    for (int childpageNumber : childrenPointers) {
      Node child = StorageManager.getStorageManager().getNodePage(tableNumber, childpageNumber);
      child.setParentPageNumber(leftSibling.getPageNumber());
    }
    leftSibling.numPrimaryKeys += numPrimaryKeys;

    // Delete this node from the parent
    tree.deleteInParent(this);

    InternalNode parent = (InternalNode) StorageManager.getStorageManager().getNodePage(tableNumber, this.getParentPageNumber());
    if (parent.getChildrenPointers().size() < tree.getMinChildrenPerNode(parent)) {
      tree.setRoot(leftSibling); // left sibling becomes new root
      Catalog.getCatalog().getSchema(tableNumber).setRoot(leftSibling.pageNumber);
    }

    leftSibling.setChanged();
}

private void mergeWithRightSibling(InternalNode rightSibling, BPlusTree tree) throws Exception {
    // Move all keys and pointers from the right sibling into this node
    primaryKeys.add(findSuccessor(rightSibling, tree));
    numPrimaryKeys++;

    primaryKeys.addAll(rightSibling.primaryKeys);
    childrenPointers.addAll(rightSibling.childrenPointers);
    // update the pointers parent
    for (int childpageNumber : rightSibling.getChildrenPointers()) {
      Node child = StorageManager.getStorageManager().getNodePage(tableNumber, childpageNumber);
      child.setParentPageNumber(this.getPageNumber());
    }
    numPrimaryKeys += rightSibling.numPrimaryKeys;

    // Delete the right sibling from the parent
    tree.deleteInParent(rightSibling);

    InternalNode parent = (InternalNode) StorageManager.getStorageManager().getNodePage(tableNumber, rightSibling.getParentPageNumber());
    if (parent.getChildrenPointers().size() < tree.getMinChildrenPerNode(parent)) {
      tree.setRoot(this);
      Catalog.getCatalog().getSchema(tableNumber).setRoot(this.pageNumber);
    }

    this.setChanged();
}

  @Override
  public void readFromHardware(RandomAccessFile tableAccessFile, TableSchema tableSchema) throws Exception {
    this.numPrimaryKeys = tableAccessFile.readInt();
    int numOfChildren = tableAccessFile.readInt();

    int primaryIndex = tableSchema.getPrimaryIndex();
    String primaryKeyDataType = tableSchema.getAttributes().get(primaryIndex).getDataType();

    for (int i = 0; i < numPrimaryKeys; ++i) {
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

    for (int i = 0; i < numOfChildren; ++i) {
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
