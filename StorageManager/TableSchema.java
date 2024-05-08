package StorageManager;

import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import Parser.Type;
import StorageManager.Objects.AttributeSchema;
import StorageManager.Objects.Catalog;
import StorageManager.Objects.MessagePrinter;
import StorageManager.Objects.MessagePrinter.MessageType;
import StorageManager.Objects.SchemaInterface;

public class TableSchema implements SchemaInterface {
  private int tableNumber;
  private String tableName;
  private List<AttributeSchema> attributes;
  private int numPages;
  private List<Integer> pageOrder;
  private int numRecords;
  private int indexRootNumber;
  private int numIndexPages;

  public TableSchema(String tableName, int tableNumber) {
    this.tableName = tableName;
    this.tableNumber = tableNumber;
    this.numPages = 0;
    this.numIndexPages = 0;
    this.pageOrder = new ArrayList<Integer>();
    this.numRecords = 0;
    this.attributes = new ArrayList<AttributeSchema>();
    this.indexRootNumber = -1; // initialize to -1 in the case that there is no current B+ tree
  }

  public TableSchema(String tableName) {
    this.tableName = tableName;
    this.tableNumber = this.hashName();
    this.numPages = 0;
    this.numIndexPages = 0;
    this.pageOrder = new ArrayList<Integer>();
    this.numRecords = 0;
    this.attributes = new ArrayList<AttributeSchema>();
    this.indexRootNumber = -1; // initialize to -1 in the case that there is no current B+ tree
  }

  public int getTableNumber() {
    return tableNumber;
  }

  public String getTableName() {
    return tableName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
    this.tableNumber = this.hashName();
  }

  public List<AttributeSchema> getAttributes() {
    return attributes;
  }

  public void setAttributes(List<AttributeSchema> attributes) {
    this.attributes = attributes;
  }

  public boolean hasAttribute(String name) {
    for (AttributeSchema attr : this.attributes) {
      if (attr.getAttributeName().equals(name)) {
        return true;
      }
    }
    return false;
  }

  public int getNumPages() {
    return numPages;
  }

  public void setNumPages() {
    this.numPages = this.pageOrder.size();
  }

  public int getNumIndexPages() {
    return this.numIndexPages;
  }

  public int incrementNumIndexPages() {
    this.numIndexPages++;
    return this.numIndexPages;
  }

  public int decrementNumIndexPages() {
    this.numIndexPages--;
    return this.numIndexPages;
  }

  public int incrementNumRecords() {
    this.numRecords += 1;
    return this.numRecords;
  }

  public int decrementNumRecords() {
    this.numRecords -= 1;
    return this.numRecords;
  }

  public void setRoot(int rootNumber) {
    this.indexRootNumber = rootNumber;
  }

  public int getRootNumber() {
    return this.indexRootNumber;
  }

  public List<Integer> getPageOrder() {
    return this.pageOrder;
  }

  public void setPageOrder(List<Integer> pageOrder) {
    this.pageOrder = pageOrder;
  }

  public void addPageNumber(int pageNumber) {
    this.pageOrder.add(pageNumber);
    this.setNumPages();
  }

  public void addPageNumber(int numberBefore, int pageNumber) {
    int index = this.pageOrder.indexOf(numberBefore);
    this.pageOrder.add(index + 1, pageNumber);
    this.setNumPages();
  }

  /**
   * Decrements all pageNumbers that are greater than the input
   * @param pageNum   The pageNum that was deleted
   */
  public void decrementPageOrder(int pageNum) {
    for (int i = 0; i < this.pageOrder.size(); i++) {
      if (this.pageOrder.get(i) > pageNum) {
        this.pageOrder.set(i, this.pageOrder.get(i)-1);
      }
    }
  }

  /**
  * deletes a page from the tableSchema
  * then decrement all pageNumber that appear after
  * the deleted page
  */
  public void deletePageNumber(Integer pageNumber) {
    this.pageOrder.remove(pageNumber);
    for (int i = 0; i < this.pageOrder.size(); i++) {
      int pNum = this.pageOrder.get(i);
      if (pNum > pageNumber) {
        this.pageOrder.remove(i);
        this.pageOrder.add(i, pNum - 1);
      }
    }
    this.setNumPages();
  }

  public int getRecords() {
    return numRecords;
  }

  public Type getAttributeType(int index) throws Exception {
    String type = this.attributes.get(index).getDataType().toLowerCase();
    Type pkType = null;
    switch (type) {
      case "integer":
          pkType = Type.INTEGER;
          break;
      case "double":
          pkType = Type.DOUBLE;
          break;
      case "boolean":
          pkType = Type.BOOLEAN;
          break;
      default:
          if (type.contains("char")) {
              pkType = Type.STRING;
          } else {
              MessagePrinter.printMessage(MessageType.ERROR, String.format("Invalid data type: %s", type));
          }
          // should never reach the break because of the error message, just in case, test this
          break;
    }
    return pkType;
  }

  public int getSizeofDataType(String dataType) throws Exception {
    int dataSize = 0;
    switch (dataType) {
      case "integer":
        dataSize = Integer.BYTES;
        break;
      case "double":
        dataSize = Double.BYTES;
        break;
      case "boolean":
        dataSize = 1;
        break;
      default:
        if (dataType.contains("char") || dataType.contains("varchar")) {
          Pattern pattern = Pattern.compile("\\((\\d+)\\)");
          Matcher matcher = pattern.matcher(dataType);
          while (matcher.find()) {
            dataSize = Integer.parseInt(matcher.group(1));
          }
        } else {
            MessagePrinter.printMessage(MessageType.ERROR, String.format("Invalid data type: %s", dataType));
        }
    }

    return dataSize;
  }

  public int computeN(Catalog catalog) throws Exception {
    int pageSize = catalog.getPageSize();
    int dataSize = 0;
    String dataType = this.attributes.get(this.getPrimaryIndex()).getDataType();

    dataSize = getSizeofDataType(dataType);
    return Math.floorDiv(pageSize, dataSize+8) - 1;
  }

  public int computeSizeOfNode(Catalog catalog) throws Exception {
    int N = computeN(catalog);
    String dataType = this.attributes.get(this.getPrimaryIndex()).getDataType();
    int size = 1 + (Integer.BYTES * 4) + ((N-1) * getSizeofDataType(dataType)) + (N * (Integer.BYTES * 2));
    return size;
  }

  private int hashName() {
    char[] chars = this.tableName.toCharArray();
    int hash = 0;
    int index = 0;
    for (char c : chars) {
        hash += Character.hashCode(c) + index;
        index++;
    }
    return hash;
  }

  public static int hashName(String name) {
    char[] chars = name.toCharArray();
    int hash = 0;
    int index = 0;
    for (char c : chars) {
        hash += Character.hashCode(c) + index;
        index++;
    }
    return hash;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void saveSchema(RandomAccessFile catalogAccessFile) throws Exception {
    // Write table name to the catalog file as UTF string
    catalogAccessFile.writeUTF(this.tableName);

    // Write table number to the catalog file
    catalogAccessFile.writeInt(this.tableNumber);

    // Write the pageNumber of the root of the B+ tree
    catalogAccessFile.writeInt(this.indexRootNumber);

    // Write number of index pages to the catalog file
    catalogAccessFile.writeInt(this.numIndexPages);

    // Write the number of pages to the catalog file
    catalogAccessFile.writeInt(this.numPages);

    // Write page order to the catalog file
    for (int i = 0; i < this.numPages; ++i) {
        catalogAccessFile.writeInt(this.pageOrder.get(i));
    }

    // Write the number of records to the catalog file
    catalogAccessFile.writeInt(this.numRecords);

    // Write the number of attributes to the catalog file
    catalogAccessFile.writeInt(this.attributes.size());

    // Iterate over each attribute and save its schema to the catalog file
    for (int i = 0; i < this.attributes.size(); ++i) {
        this.attributes.get(i).saveSchema(catalogAccessFile);
    }
  }

  public void addAttribute(AttributeSchema attributeSchema) {
    this.attributes.add(attributeSchema);
  }

  /**
    * {@inheritDoc}
   */
  @Override
  public void loadSchema(RandomAccessFile catalogAccessFile) throws Exception {
    // at this point both table name and table number have already been read

    // Read the pageNumber of the root of the B+ Tree
    this.indexRootNumber = catalogAccessFile.readInt();

    // Read the number of index pages from the catalog file
    this.numIndexPages = catalogAccessFile.readInt();

    // Read the number of pages from the catalog file
    this.numPages = catalogAccessFile.readInt();

    // Read page order from the catalog file
    for (int i = 0; i < this.numPages; i++) {
        this.pageOrder.add(catalogAccessFile.readInt());
    }

    // Read the number of records from the catalog file
    this.numRecords = catalogAccessFile.readInt();

    // Read the number of attributes from the catalog file
    int numOfAttributes = catalogAccessFile.readInt();

    // Iterate over each attribute and load its schema from the catalog file
    for (int i = 0; i < numOfAttributes; ++i) {
        // Create a new AttributeSchema instance
        AttributeSchema attributeSchema = new AttributeSchema();

        // Load attribute schema from the catalog file
        attributeSchema.loadSchema(catalogAccessFile);

        // Add the loaded attribute schema to the list of attributes
        this.attributes.add(attributeSchema);
    }
  }

    public int getPrimaryIndex() {
      // determine index of the primary key
      int primaryIndex = -1;
      for (int i = 0; i < this.attributes.size(); i++) {
          if (this.attributes.get(i).isPrimaryKey()) {
              primaryIndex = i;
          }
      }
      return primaryIndex;
    }

}
