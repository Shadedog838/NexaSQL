package StorageManager.Objects;

import java.io.File;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import QueryExecutor.InsertQueryExcutor;
import StorageManager.StorageManager;
import StorageManager.TableSchema;
import StorageManager.Objects.MessagePrinter.MessageType;

public class Catalog implements java.io.Serializable, CatalogInterface {
    private static Catalog catalog;
    private Map<Integer, TableSchema> schemas;
    private String dbLocation;
    private String catalogLocation;
    private int pageSize;
    private int bufferSize;
    private boolean indexing;

    private Catalog(String catalogLocation, String dbLocation, int pageSize, int bufferSize, boolean indexing) throws Exception {
        this.catalogLocation = catalogLocation;
        this.dbLocation = dbLocation;
        this.bufferSize = bufferSize;
        this.schemas = new HashMap<>();
        if (pageSize == -1) {
            loadCatalog();
        } else {
            this.pageSize = pageSize;
            this.indexing = indexing;
        }
    }

    public static void createCatalog(String dbLocation, String catalogLocation, int pageSize, int bufferSize, boolean indexing)
            throws Exception {
        catalog = new Catalog(catalogLocation, dbLocation, pageSize, bufferSize, indexing);
    }

    public static Catalog getCatalog() {
        return catalog;
    }

    /**
     * {@inheritDoc}
     *
     * @throws Exception
     */
    public void saveCatalog() throws Exception {
        // Save catalog to hardware and obtain a random access file
        File schemaFile = new File(this.catalogLocation);
        RandomAccessFile catalogAccessFile = new RandomAccessFile(schemaFile, "rw");

        // Write page size to the catalog file
        catalogAccessFile.writeInt(this.pageSize);

        // Write indexing on:off to catalog file
        catalogAccessFile.writeBoolean(this.indexing);

        // Write the number of schemas to the catalog file
        catalogAccessFile.writeInt(this.schemas.size());

        // Iterate over each table schema and save it to the catalog file
        for (int tableNum : this.schemas.keySet()) {
            this.schemas.get(tableNum).saveSchema(catalogAccessFile);
        }

        catalogAccessFile.close();

    }

    /**
     * {@inheritDoc}
     *
     * @throws Exception
     */
    @Override
    public void loadCatalog() throws Exception {
        // Load catalog from hardware and obtain a random access file
        File schemaFile = new File(this.catalogLocation);
        RandomAccessFile catalogAccessFile = new RandomAccessFile(schemaFile, "r");

        // Read page size from the catalog file
        this.pageSize = catalogAccessFile.readInt();

        // Read indexing on:off from catalog file
        this.indexing = catalogAccessFile.readBoolean();

        // Read the number of tables from the catalog file
        int numOfTables = catalogAccessFile.readInt();

        // Iterate over each table schema and load it from the catalog file
        for (int i = 0; i < numOfTables; ++i) {
            // Read table Name
            String tableName = catalogAccessFile.readUTF();

            // Read the table number
            int tableNumber = catalogAccessFile.readInt();

            // Create a new table schema instance and load it from the catalog file
            TableSchema tableSchema = new TableSchema(tableName, tableNumber);
            tableSchema.loadSchema(catalogAccessFile);

            // Add the loaded table schema to the schemas map
            this.schemas.put(tableNumber, tableSchema);
        }

        catalogAccessFile.close();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void dropTableSchema(int tableNumber) {
        try {
            schemas.remove(tableNumber);
            StorageManager.getStorageManager().dropTable(tableNumber);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void alterTableSchema(int tableNumber,String op, String attrName, String attrType, Object val,String isDeflt ,boolean notNull,
                                boolean pKey, boolean unique) throws Exception {
        TableSchema table = schemas.get(tableNumber);

        if(isDeflt.equals("true")) {
            if (attrType.equals("integer")) {
                try {
                    val = Integer.parseInt((String) val);
                } catch (Exception e) {
                    MessagePrinter.printMessage(MessageType.ERROR, String.format("Invalid data type: expected (%s)", attrType));
                }

            } else if (attrType.equals("double")) {
                try {
                    val = Double.parseDouble((String) val);
                } catch (Exception e) {
                    MessagePrinter.printMessage(MessageType.ERROR, String.format("Invalid data type: expected (%s)", attrType));
                }
            } else if (attrType.equals("boolean")) {
                try {
                    val = Boolean.parseBoolean((String) val);
                } catch (Exception e) {
                    MessagePrinter.printMessage(MessageType.ERROR, String.format("Invalid data type: expected (%s)", attrType));
                }
            }
        }



        //List<AttributeSchema> attrList = table.getAttributes();
        List<AttributeSchema> attrList = new ArrayList<>();
        attrList.addAll(table.getAttributes());
        if(op.equals("drop")){
            for(int i=0; i<attrList.size(); i++) {
                if(attrList.get(i).getAttributeName().equals(attrName)){
                    if (attrList.get(i).isPrimaryKey()) {
                        MessagePrinter.printMessage(MessageType.ERROR, String.format("Attribute name %s cannot be droped because its a primary key", attrName));
                    }
                    attrList.remove(i);
                }
            }
        } else if (op.equals("add")) {
            for(int i=0; i<attrList.size(); i++){
                AttributeSchema currentAttr = attrList.get(i);
                if(currentAttr.getAttributeName().equals(attrName)){
                    MessagePrinter.printMessage(MessageType.ERROR, String.format("Atrribute name %s already exist", attrName));
                }
            }

            // validate default data type
            if (isDeflt.equals("true")) {
                String valDataType = InsertQueryExcutor.getDataType(val, attrType);
                if (attrType.contains("char") || attrType.contains("varchar")) {
                    if (!(val instanceof String)) {
                        MessagePrinter.printMessage(MessageType.ERROR, String.format("Invalid data type: expected (%s) got (%s)", attrList, valDataType));
                    }
                    Pattern pattern = Pattern.compile("\\((\\d+)\\)");
                    Matcher matcher = pattern.matcher(attrType);
                    int size;
                    if(matcher.find()) {
                        size = Integer.parseInt(matcher.group(1));
                    }else {
                        throw new Exception("No match found in pattern");
                    }

                    String value = ((String) val);
                    if (attrType.contains("char")) {
                        if (value.length() != size && attrType.equals("char")) {
                          MessagePrinter.printMessage(MessageType.ERROR, String.format("%s can only accept %d chars; %s is %d", attrType, size, value, value.length()));
                        }
                      } else {
                        if (value.length() > size) {
                          MessagePrinter.printMessage(MessageType.ERROR, String.format("row %s: %s can only accept %d chars or less; %s is %d", attrType, size, value, value.length()));
                        }
                      }

                } else if (attrType.equals("integer")) {
                    if (!((Integer) val instanceof Integer)) {
                        MessagePrinter.printMessage(MessageType.ERROR, String.format("Invalid data type: expected ($s) got (%s)", attrType, valDataType));
                    }
                } else if (attrType.equals("double")) {
                    if (!((Double) val instanceof Double)) {
                        MessagePrinter.printMessage(MessageType.ERROR, String.format("Invalid data type: expected ($s) got (%s)", attrType, valDataType));
                    }
                } else if (attrType.equals("boolean")) {
                    if (!(val instanceof Boolean)) {
                        MessagePrinter.printMessage(MessageType.ERROR, String.format("Invalid data type: expected ($s) got (%s)", attrType, valDataType));
                    }
                } else {
                    MessagePrinter.printMessage(MessageType.ERROR, String.format("Invalid data type: expected ($s) got (%s)", attrType, valDataType));
                }
            }

            attrList.add(new AttributeSchema(attrName, attrType, notNull, pKey, unique));
        }else{
            throw new Exception("Invalid Command");
        }

        //call new storage manager method.
        StorageManager.getStorageManager().alterTable(tableNumber, op, attrName, val, isDeflt, attrList);

        // save the schema afterwards in the off chance we need to rollback
        table.setAttributes(attrList);
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public void createTable(TableSchema tableSchema) throws Exception {

        if (tableSchema.getAttributes().isEmpty()) {
            MessagePrinter.printMessage(MessageType.ERROR, "Table with no attributes");
        }

        // check for one primary key
        boolean primaryKeyFound = false;
        List<String> attributeNames = new ArrayList<String>();
        for (AttributeSchema attributeSchema : tableSchema.getAttributes()) {
            if (attributeSchema.isPrimaryKey() && !primaryKeyFound) {
                primaryKeyFound = true;
            } else if (attributeSchema.isPrimaryKey() && primaryKeyFound) {
                MessagePrinter.printMessage(MessageType.ERROR, "More than one primary key");
            } else if (attributeNames.contains(attributeSchema.getAttributeName())) {
                MessagePrinter.printMessage(MessageType.ERROR, String.format("Dublicate attribute name \"%s\"", attributeSchema.getAttributeName()));
            }
            attributeNames.add(attributeSchema.getAttributeName());
        }

        if (!primaryKeyFound) {
            MessagePrinter.printMessage(MessageType.ERROR, "No primary key defined");
        }

        for (TableSchema schema : this.schemas.values()) {
            if (tableSchema.getTableName().equals(schema.getTableName())) {
                MessagePrinter.printMessage(MessageType.ERROR,
                        "Table of name " + schema.getTableName() + " already exists");
            }
        }
        this.schemas.put(tableSchema.getTableNumber(), tableSchema);
    }

    public Map<Integer, TableSchema> getSchemas() {
        return schemas;
    }

    public TableSchema getSchema(int tableNumber) {
        return schemas.get(tableNumber);
    }

    public TableSchema getSchema(String tableName) throws Exception {
        for (Integer tableNuber : this.schemas.keySet()) {
            TableSchema tableSchema = getSchema(tableNuber);
            if (tableSchema.getTableName().equals(tableName)) {
                return tableSchema;
            }
        }
        MessagePrinter.printMessage(MessageType.ERROR, String.format("No such table %s", tableName));
        return null;
    }

    public void setSchemas(List<TableSchema> schemas) {
        Map<Integer, TableSchema> _new = new Hashtable<Integer, TableSchema>();
        for (TableSchema tableSchema : schemas) {
            _new.put(tableSchema.getTableNumber(), tableSchema);
        }
        this.schemas = _new;
    }

    public void addSchemas(TableSchema tableSchema) {
        this.schemas.put(tableSchema.getTableNumber(), tableSchema);
    }

    public String getDbLocation() {
        return dbLocation;
    }

    public String getCatalogLocation() {
        return catalogLocation;
    }

    public int getPageSize() {
        return pageSize;
    }

    public int getBufferSize() {
        return bufferSize;
    }

    public boolean isIndexingOn() {
        return indexing;
    }

}
