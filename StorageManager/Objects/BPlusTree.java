package StorageManager.Objects;

import StorageManager.StorageManager;
import StorageManager.TableSchema;
import StorageManager.Objects.MessagePrinter.MessageType;

public class BPlusTree {
  private Node root;
  private int N;
  private TableSchema tableSchema;
  private int tableNumber;

  public BPlusTree(TableSchema tableSchema, Node root) throws Exception {
    this.tableSchema = tableSchema;
    this.tableNumber = tableSchema.getTableNumber();
    this.N = tableSchema.computeN(Catalog.getCatalog());
    this.root = root;
  }

  public int getMaxKeysPerNode() {
    return N - 1;
  }

  public int getMinKeysPerNode() {
    return (N - 1) / 2;
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  public Bucket canInsert(Object primaryKey) throws Exception {
    Node currentNode = root;

    while (!currentNode.isLeaf()) {
      InternalNode internalNode = (InternalNode) currentNode;
      int pos = internalNode.findInsertPosition(primaryKey);
      if (((Comparable) primaryKey).compareTo(internalNode.getPrimaryKeys().get(pos)) > 0) {
        ++pos; // got to the right child
      }
      int childPageNumber = internalNode.getChildrenPointers().get(pos);
      currentNode = StorageManager.getStorageManager().getNodePage(tableNumber, childPageNumber);
    }

    LeafNode leafNode = (LeafNode) currentNode;
    int pos = leafNode.findInsertPosition(primaryKey);

    if (pos < leafNode.getNumKeys() && ((Comparable) leafNode.getPrimaryKeys().get(pos)).compareTo(primaryKey) == 0) {
       MessagePrinter.printMessage(MessageType.ERROR, String.format("primaryKey: (%d) already exist", primaryKey.toString()));
    }

    if (pos - 1 < 0) {
      return leafNode.getBuckets().get(0);
    } else {
      return leafNode.getBuckets().get(pos - 1);
    }
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  public Bucket search(Object primaryKey) throws Exception {
    Node currentNode = root;

    while (!currentNode.isLeaf()) {
      InternalNode internalNode = (InternalNode) currentNode;
      int pos = internalNode.findInsertPosition(primaryKey);
      int childPageNumber = internalNode.getChildrenPointers().get(pos);
      currentNode = StorageManager.getStorageManager().getNodePage(tableNumber, childPageNumber);
    }

    LeafNode leafNode = (LeafNode) currentNode;
    int pos = leafNode.findInsertPosition(primaryKey);

    if (pos < leafNode.getNumKeys() && ((Comparable) leafNode.getPrimaryKeys().get(pos)).compareTo(primaryKey) == 0) {
      return leafNode.getBuckets().get(pos);
    }

    return null;
  }

  public void insert(Object primaryKey, Object value) throws Exception {
    root.insertKey(primaryKey, value, this);
  }

  public void update(Object oldPrimaryKey, Object newPrimaryKey, Object newValue) throws Exception {
    delete(oldPrimaryKey);
    insert(newPrimaryKey, newValue);
  }

  public void delete(Object primaryKey) throws Exception {
    root.deleteKey(primaryKey, this);
  }

  public void insertInParent(Node leftNode, Node rightNode, Object middleKey) throws Exception {
    if (leftNode == root) {
      InternalNode newRoot = new InternalNode(tableNumber, tableSchema.getNumIndexPages() + 1, -1);
      tableSchema.incrementNumIndexPages();
      newRoot.setChanged();
      StorageManager.getStorageManager().addPageToBuffer(newRoot);
      newRoot.primaryKeys.add(middleKey);
      newRoot.getChildrenPointers().add(leftNode.getPageNumber());
      newRoot.getChildrenPointers().add(rightNode.getPageNumber());
      leftNode.setParentPageNumber(newRoot.getPageNumber());
      rightNode.setParentPageNumber(newRoot.getPageNumber());
      root = newRoot;
      tableSchema.setRoot(root.pageNumber);
    } else {
      InternalNode parent = (InternalNode) StorageManager.getStorageManager().getNodePage(tableNumber, leftNode.getParentPageNumber());
      parent.insertKey(middleKey, rightNode.getParentPageNumber(), this);
    }
  }

  public void deleteInParent(Node node) throws Exception {
    if (node == root && node.numPrimaryKeys == 0) {
      root = null;
    } else {
      InternalNode parent = (InternalNode) StorageManager.getStorageManager().getNodePage(tableNumber, node.getParentPageNumber());
      parent.deleteKey(node.getPrimaryKeys().get(0), this);
    }
  }

  public LeafNode getLeftSibling(LeafNode node) throws Exception {
    if (node.getParentPageNumber() == -1) {
      return null;
    }

    InternalNode parent = (InternalNode) StorageManager.getStorageManager().getNodePage(tableNumber, node.getParentPageNumber());
    int index = parent.getChildrenPointers().indexOf(node.getPageNumber());
    if (index > 0) {
      return (LeafNode) StorageManager.getStorageManager().getNodePage(tableNumber, parent.getChildrenPointers().get(index - 1));
    }

    return null;
  }

  public LeafNode getRightSibling(LeafNode node) throws Exception {
    if (node.getParentPageNumber() == -1) {
      return null;
    }

    InternalNode parent = (InternalNode) StorageManager.getStorageManager().getNodePage(tableNumber, node.getParentPageNumber());
    int index = parent.getChildrenPointers().indexOf(node.getPageNumber());
    if (index < parent.getChildrenPointers().size() - 1) {
      return (LeafNode) StorageManager.getStorageManager().getNodePage(tableNumber, parent.getChildrenPointers().get(index + 1));
    }

    return null;
  }

  public InternalNode getLeftInternalSibling(InternalNode node) throws Exception {
    if (node.getParentPageNumber() == -1) {
        return null;
    }
    InternalNode parent = (InternalNode) StorageManager.getStorageManager().getNodePage(tableNumber, node.getParentPageNumber());
    int index = parent.getChildrenPointers().indexOf(node.getPageNumber());
    if (index > 0) {
        return (InternalNode) StorageManager.getStorageManager().getNodePage(tableNumber, parent.getChildrenPointers().get(index - 1));
    }
    return null;
}

  public InternalNode getRightInternalSibling(InternalNode node) throws Exception {
    if (node.getParentPageNumber() == -1) {
        return null;
    }
    InternalNode parent = (InternalNode) StorageManager.getStorageManager().getNodePage(tableNumber, node.getParentPageNumber());
    int index = parent.getChildrenPointers().indexOf(node.getPageNumber());
    if (index < parent.getChildrenPointers().size() - 1) {
        return (InternalNode) StorageManager.getStorageManager().getNodePage(tableNumber, parent.getChildrenPointers().get(index + 1));
    }
    return null;
  }

  public void splitRoot(InternalNode rootNode) throws Exception {
    int midIndex = rootNode.numPrimaryKeys / 2;
    Object midKey = rootNode.primaryKeys.get(midIndex);


    InternalNode newRoot = new InternalNode(tableNumber, tableSchema.getNumIndexPages() + 1, -1);
    tableSchema.incrementNumIndexPages();
    newRoot.setChanged();
    StorageManager.getStorageManager().addPageToBuffer(newRoot);
    InternalNode newRightNode = new InternalNode(tableNumber, tableSchema.getNumIndexPages() + 1, newRoot.getPageNumber());
    tableSchema.incrementNumIndexPages();
    newRightNode.setChanged();
    StorageManager.getStorageManager().addPageToBuffer(newRightNode);

    for (int i=midIndex + 1; i < rootNode.numPrimaryKeys; ++i) {
      newRightNode.primaryKeys.add(rootNode.primaryKeys.get(midIndex));
      newRightNode.getChildrenPointers().add(rootNode.getChildrenPointers().get(i));
    }
    newRightNode.numPrimaryKeys = newRightNode.primaryKeys.size();

    rootNode.numPrimaryKeys = midIndex;

    newRoot.primaryKeys.add(midKey);
    newRoot.getChildrenPointers().add(rootNode.getPageNumber());
    newRoot.getChildrenPointers().add(newRightNode.getPageNumber());
    rootNode.setParentPageNumber(newRoot.getPageNumber());
    newRightNode.setParentPageNumber(newRoot.getPageNumber());

    this.root = newRoot;
    tableSchema.setRoot(this.root.pageNumber);
  }

}