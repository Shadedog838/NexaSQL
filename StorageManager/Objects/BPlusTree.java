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

  public int getMinKeysForNode(Node node) {
    if (node == root) {
      return 1;
    }
    return getMinKeysPerNode();
  }

  public int getMinKeysForLeafNode(Node node) {
    if (node == root) {
      return 1;
    }
    return (N - 1) / 2;
  }

  public int getMaxChildrenPerNode() {
      return N;
  }

  public int getMinChildrenPerNode(Node node) {
    if (node == root) return 2;
    return (int) Math.ceil((double) N / 2);
  }

  public Node getRoot() {
    return this.root;
  }

  public void setRoot(Node newRoot) {
    this.root = newRoot;
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  public Bucket canInsert(Object primaryKey) throws Exception {
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
      if (pos < internalNode.numPrimaryKeys && ((Comparable) internalNode.getPrimaryKeys().get(pos)).compareTo(primaryKey) == 0) {
        // go to the right if key is equal;
        int childPageNumber = internalNode.getChildrenPointers().get(pos + 1);
        currentNode = StorageManager.getStorageManager().getNodePage(tableNumber, childPageNumber);
      } else {
        int childPageNumber = internalNode.getChildrenPointers().get(pos);
        currentNode = StorageManager.getStorageManager().getNodePage(tableNumber, childPageNumber);
      }
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
      ++newRoot.numPrimaryKeys;
      newRoot.getChildrenPointers().add(leftNode.getPageNumber());
      newRoot.getChildrenPointers().add(rightNode.getPageNumber());
      leftNode.setParentPageNumber(newRoot.getPageNumber());
      rightNode.setParentPageNumber(newRoot.getPageNumber());
      root = newRoot;
      tableSchema.setRoot(root.pageNumber);
    } else {
      InternalNode parent = (InternalNode) StorageManager.getStorageManager().getNodePage(tableNumber, leftNode.getParentPageNumber());
      parent.insertKey(middleKey, rightNode.getParentPageNumber(), this);
      if(!parent.getPrimaryKeys().contains(middleKey)) {
        leftNode.setParentPageNumber(parent.getPageNumber());
        parent.getChildrenPointers().add(leftNode.getPageNumber());
        InternalNode rightSibling = this.getRightInternalSibling(parent);
        rightSibling.getChildrenPointers().set(0, rightNode.getPageNumber());
        rightNode.setParentPageNumber(rightSibling.getPageNumber());
        return;
      }
      if (parent.getPrimaryKeys().indexOf(middleKey) == parent.numPrimaryKeys - 1) {
        parent.getChildrenPointers().add(rightNode.getPageNumber());
      } else {
        parent.getChildrenPointers().add(parent.getPrimaryKeys().indexOf(middleKey) + 1, rightNode.getPageNumber());
      }
    }
  }

  public void deleteInParent(Node node) throws Exception {
    if (node == root && node.numPrimaryKeys == 0) {
      root = null;
    } else {
      InternalNode parent = (InternalNode) StorageManager.getStorageManager().getNodePage(tableNumber, node.getParentPageNumber());
      int pointerIndex = parent.getChildrenPointers().indexOf(node.getPageNumber());
      parent.getChildrenPointers().remove(pointerIndex);
      if (pointerIndex == 0) {
        parent.getPrimaryKeys().remove(pointerIndex);
      } else {
        parent.getPrimaryKeys().remove(pointerIndex - 1);
      }
      parent.numPrimaryKeys--;
      node.getPrimaryKeys().clear(); // remove all keys to indicate dead node
      node.numPrimaryKeys = 0;

      if (parent.numPrimaryKeys == 0 && parent == this.getRoot()) {
        if (!parent.getChildrenPointers().isEmpty()) {
          // If there's only one child left, make it the new root
          this.setRoot(StorageManager.getStorageManager().getNodePage(tableNumber, parent.getChildrenPointers().get(0)));
          Catalog.getCatalog().getSchema(tableNumber).setRoot(StorageManager.getStorageManager().getNodePage(tableNumber, parent.getChildrenPointers().get(0)).pageNumber);
          this.root.parentPageNumber = -1;
        } else {
          this.setRoot(null); // Empty tree
          Catalog.getCatalog().getSchema(tableNumber).setRoot(1);
          Catalog.getCatalog().getSchema(tableNumber).setNumIndexPages(0);
        }
      }
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


}