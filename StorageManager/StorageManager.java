package StorageManager;

import java.io.File;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Random;

import Parser.Insert;
import QueryExecutor.InsertQueryExcutor;
import StorageManager.Objects.AttributeSchema;
import StorageManager.Objects.BPlusTree;
import StorageManager.Objects.Bucket;
import StorageManager.Objects.BufferPage;
import StorageManager.Objects.Catalog;
import StorageManager.Objects.InternalNode;
import StorageManager.Objects.LeafNode;
import StorageManager.Objects.Node;
import StorageManager.Objects.MessagePrinter;
import StorageManager.Objects.Page;
import StorageManager.Objects.Record;
import StorageManager.Objects.MessagePrinter.MessageType;
import StorageManager.Objects.Utility.Pair;

public class StorageManager implements StorageManagerInterface {
    private static StorageManager storageManager;
    private PriorityQueue<BufferPage> buffer;
    private int bufferSize;

    /**
     * Constructor for the storage manager
     * initializes the class by initializing the buffer
     *
     * @param buffersize The size of the buffer
     */
    private StorageManager(int bufferSize) {
        this.bufferSize = bufferSize;
        this.buffer = new PriorityQueue<>(bufferSize, new Page());
    }

    /**
     * Static function that initializes the storageManager
     *
     * @param bufferSize The size of the buffer
     */
    public static void createStorageManager(int bufferSize) {
        storageManager = new StorageManager(bufferSize);
    }

    /**
     * Getter for the global storageManager
     *
     * @return The storageManager
     */
    public static StorageManager getStorageManager() {
        return storageManager;
    }

    /**
     * Splits a page by moving half of its records to a new page.
     *
     * @param page              The page to split.
     * @param record            The record to insert after the split.
     * @param tableSchema       The schema of the table.
     * @param primaryKeyIndex   The index in which the PK resides in the record
     * @return                  The list of pages that results from the split
     * @throws Exception If an error occurs during the split operation.
     */
    private List<Page> pageSplit(Page page, Record record, TableSchema tableSchema, int primaryKeyIndex) throws Exception {
        // Create a new page
        Page newPage = new Page(0, tableSchema.getTableNumber(), tableSchema.getNumPages() + 1);
        tableSchema.addPageNumber(page.getPageNumber(), newPage.getPageNumber());
        List<Page> results = Arrays.asList(newPage);

        // Calculate the split index
        int splitIndex = 0;
        if (page.getRecords().size() == 1) {
            Record lastRecordInCurrPage = page.getRecords().get(page.getRecords().size() - 1);
            if (record.compareTo(lastRecordInCurrPage, primaryKeyIndex) < 0) {
                page.getRecords().clear();
                page.addNewRecord(record, false);
                newPage.addNewRecord(lastRecordInCurrPage, true);
            } else {
                newPage.addNewRecord(record, false);
            }
        } else {
            splitIndex = (int) Math.floor(page.getRecords().size() / 2);

            // Move half of the records to the new page
            for (Record copyRecord : page.getRecords().subList(splitIndex, page.getRecords().size())) {
                if (!newPage.addNewRecord(copyRecord, true)) {
                    List<Page> temp = pageSplit(newPage, copyRecord, tableSchema, primaryKeyIndex);
                    results.remove(newPage);
                    results.addAll(temp);
                }
            }

            page.getRecords().subList(splitIndex, page.getRecords().size()).clear();

            // decide what page to add record to
            Record lastRecordInCurrPage = page.getRecords().get(page.getRecords().size() - 1);
            if (record.compareTo(lastRecordInCurrPage, primaryKeyIndex) < 0) {
                // record is less than lastRecord in page
                if (!page.addNewRecord(record, false)) {
                    List<Page> temp = pageSplit(page, record, tableSchema, primaryKeyIndex);
                    results.remove(page);
                    results.addAll(temp);
                }
            } else {
                if (!newPage.addNewRecord(record, false)) {
                    List<Page> temp = pageSplit(newPage, record, tableSchema, primaryKeyIndex);
                    results.remove(newPage);
                    results.addAll(temp);
                }
            }
        }

        page.setNumRecords();
        newPage.setNumRecords();
        page.setChanged();

        // Add the new page to the buffer
        this.addPageToBuffer(newPage);
        return results;
    }

    /**
     * Construct the full table path according to where
     * the DB is located
     *
     * @param tableNumber the id of the table
     *
     * @return the full table path
     */
    private String getTablePath(int tableNumber) {
        String dbLoc = Catalog.getCatalog().getDbLocation();
        return dbLoc + "/tables/" + Integer.toString(tableNumber);
    }

    /**
     * Construct the full indexing file path according to where
     * the DB is located
     *
     * @param tableNumber the id of the table
     *
     * @return the full indexing path
     */
    private String getIndexingPath(int tableNumber) {
        String dbLoc = Catalog.getCatalog().getDbLocation();
        return dbLoc + "/indexing/" + Integer.toString(tableNumber);
    }

    public Bucket getPrimaryKeyBucket(int tableNumber, Object primaryKey, List<Page> pagesToLookThrough) throws Exception {
        Catalog catalog = Catalog.getCatalog();
        TableSchema schema = catalog.getSchema(tableNumber);
        int primaryKeyIndex = schema.getPrimaryIndex();
        List<Integer> pageOrder = schema.getPageOrder();
        Page foundPage = null;
        if (pagesToLookThrough != null) {
            for (Page page : pagesToLookThrough) {
                Record lastRecord = page.getRecords().get(page.getRecords().size() - 1);
                int comparison = lastRecord.compareTo(primaryKey, primaryKeyIndex);

                if (comparison == 0 || comparison > 0) {
                    foundPage = page;
                    break;
                }
            }
        } else {
            for (int pageNumber : pageOrder) {
                Page page = this.getPage(tableNumber, pageNumber);

                Record lastRecord = page.getRecords().get(page.getRecords().size() - 1);
                int comparison = lastRecord.compareTo(primaryKey, primaryKeyIndex);

                if (comparison == 0 || comparison > 0) {
                    foundPage = page;
                }
            }
        }

        List<Record> records = foundPage.getRecords();
        for (Record i : records) {
            if (i.compareTo(primaryKey, primaryKeyIndex) == 0) {
                return new Bucket(foundPage.getPageNumber(), records.indexOf(i));
            }
        }

        return null;
    }

    public Record getRecord(int tableNumber, Object primaryKey) throws Exception {
        // used for selecting based on primary key
        Catalog catalog = Catalog.getCatalog();
        TableSchema schema = catalog.getSchema(tableNumber);
        int primaryKeyIndex = schema.getPrimaryIndex();
        List<Integer> pageOrder = schema.getPageOrder();
        Page foundPage = null;

        for (int pageNumber : pageOrder) {
            Page page = this.getPage(tableNumber, pageNumber);
            if (page.getNumRecords() == 0) {
                return null;
            }

            Record lastRecord = page.getRecords().get(page.getRecords().size() - 1);
            int comparison = lastRecord.compareTo(primaryKey, primaryKeyIndex);

            if (comparison == 0) {
                // found the record, return it
                return lastRecord;
            } else if (comparison > 0) {
                // found the correct page
                foundPage = page;
                break;
            } else {
                // record was not found, continue
                continue;
            }
        }

        if (foundPage == null) {
            // a page with the record was not found
            return null;
        } else {
            List<Record> records = foundPage.getRecords();
            for (Record i : records) {
                if (i.compareTo(primaryKey, primaryKeyIndex) == 0) {
                    return i;
                }
            }
            // record was not found
            return null;
        }
    }

    public Record getRecord(String tableName, Object primaryKey) throws Exception {
        int tableNumber = TableSchema.hashName(tableName);
        return this.getRecord(tableNumber, primaryKey);
    }

    public List<Record> getAllRecords(int tableNumber) throws Exception {
        List<Record> records = new ArrayList<>(); // List to store all records
        List<Page> allPagesForTable = new ArrayList<>();
        Catalog catalog = Catalog.getCatalog();
        TableSchema tableSchema = catalog.getSchema(tableNumber);
        for (Integer pageNumber : tableSchema.getPageOrder()) {
            allPagesForTable.add(this.getPage(tableNumber, pageNumber));
        }

        for (Page page : allPagesForTable) {
            records.addAll(page.getRecords());
        }

        return records;
    }

    public List<Record> getAllRecords(String tableName) throws Exception {
        int tableNum = TableSchema.hashName(tableName);
        return this.getAllRecords(tableNum);
    }

    public void insertRecord(int tableNumber, Record record) throws Exception {
        Catalog catalog = Catalog.getCatalog();
        // get tableSchema from the catalog
        TableSchema tableSchema = catalog.getSchema(tableNumber);
        if (record.computeSize() > (catalog.getPageSize() - (Integer.BYTES * 2))) {
            MessagePrinter.printMessage(MessageType.ERROR,
                    "Unable to insert record. The record size is larger than the page size.");
        }

        String tablePath = this.getTablePath(tableNumber);
        File tableFile = new File(tablePath);
        String indexPath = this.getIndexingPath(tableNumber);
        File indexFile = new File(indexPath);

        // determine index of the primary key
        int primaryKeyIndex = tableSchema.getPrimaryIndex();

        // check to see if the file exists, if not create it
        if (!tableFile.exists()) {
            tableFile.createNewFile();
            // create a new page and insert the new record into it
            Page _new = new Page(0, tableNumber, 1);
            tableSchema.addPageNumber(_new.getPageNumber());
            _new.addNewRecord(record, false);
            tableSchema.incrementNumRecords();
            // then add the page to the buffer
            this.addPageToBuffer(_new);

            if (catalog.isIndexingOn()) {
                if (!indexFile.exists()) {
                    indexFile.createNewFile();

                    LeafNode root = new LeafNode(tableNumber, 1, -1);
                    tableSchema.incrementNumIndexPages();
                    BPlusTree bPlusTree = new BPlusTree(tableSchema, root);
                    Object primaryKey = record.getValues().get(primaryKeyIndex);
                    this.addPageToBuffer(root);
                    tableSchema.setRoot(1);
                    bPlusTree.insert(primaryKey, new Bucket(1, 0));
                }
            }
        } else {
            if (catalog.isIndexingOn()) {
                Object primaryKey = record.getValues().get(primaryKeyIndex);
                Node root = this.getNodePage(tableNumber, tableSchema.getRootNumber());
                BPlusTree bPlusTree = new BPlusTree(tableSchema, root);
                Bucket bucketPrior = bPlusTree.canInsert(primaryKey);
                Page page = this.getPage(tableNumber, bucketPrior.getPageNumber());
                Bucket newBucket;
                if (bucketPrior.getIndex() == 0) {
                    // This means the new record can go before or after the first record in the page
                    if (!page.addNewRecord(record, false)) {
                        List<Page> pages = this.pageSplit(page, record, tableSchema, primaryKeyIndex);
                        List<Page> checkPages = new ArrayList<>(Arrays.asList(page));
                        checkPages.addAll(pages);
                        newBucket = this.getPrimaryKeyBucket(tableNumber, primaryKey, checkPages);
                    } else {
                        List<Page> pages = Arrays.asList(page);
                        newBucket = this.getPrimaryKeyBucket(tableNumber, primaryKey, pages);
                    }
                } else {
                    if (!page.addNewRecord(record, bucketPrior.getIndex() + 1)) {
                        List<Page> pages = this.pageSplit(page, record, tableSchema, primaryKeyIndex);
                        List<Page> checkPages = new ArrayList<>(Arrays.asList(page));
                        checkPages.addAll(pages);
                        newBucket = this.getPrimaryKeyBucket(tableNumber, primaryKey, checkPages);
                    } else {
                        newBucket = new Bucket(page.getPageNumber(), bucketPrior.getIndex() + 1);
                    }
                }
                tableSchema.incrementNumRecords();
                bPlusTree.insert(primaryKey, newBucket);
            } else {
                for (Integer pageNumber : tableSchema.getPageOrder()) {
                    Page page = this.getPage(tableNumber, pageNumber);
                    if (page.getNumRecords() == 0) {
                        if (!page.addNewRecord(record, false)) {
                            // page was full
                            this.pageSplit(page, record, tableSchema, primaryKeyIndex);
                        }
                        tableSchema.incrementNumRecords();
                        break;
                    }

                    Record lastRecordInPage = page.getRecords().get(page.getRecords().size() - 1);
                    if ((record.compareTo(lastRecordInPage, primaryKeyIndex) < 0) ||
                        (pageNumber == tableSchema.getPageOrder().get(tableSchema.getPageOrder().size() - 1))) {
                        // record is less than lastRecordPage
                        if (!page.addNewRecord(record, false)) {
                            // page was full
                            this.pageSplit(page, record, tableSchema, primaryKeyIndex);
                        }
                        tableSchema.incrementNumRecords();
                        break;
                    }
                }

            }
        }
    }


    /**
     * Iterates through the DB and finds the record to delete
     * @param schema        The table schema of the table to delete
     * @param primaryKey    The primaryKey of the record to delete
     * @return              The page the record was deleted from as well as the deleted record
     * @throws Exception
     */
    private Pair<Page, Record> deleteHelper(TableSchema schema, Object primaryKey) throws Exception {

        Integer tableNumber = schema.getTableNumber();
        int primaryIndex = schema.getPrimaryIndex();
        Page foundPage = null;

        // start reading pages
        // get page order
        List<Integer> pageOrder = schema.getPageOrder();

        // find the correct page
        for (int pageNumber : pageOrder) {
            Page page = this.getPage(tableNumber, pageNumber);

            // compare last record in page
            List<Record> foundRecords = page.getRecords();
            Record lastRecord = foundRecords.get(page.getNumRecords() - 1);
            int comparison = lastRecord.compareTo(primaryKey, primaryIndex);
            if (comparison == 0) {
                // found the record, delete it
                Record removed = page.deleteRecord(page.getNumRecords() - 1);
                return new Pair<Page,Record>(page, removed);
            } else if (comparison > 0) {
                // found the correct page
                foundPage = page;
                break;
            } else {
                // page was not found, continue
                continue;
            }
        }

        if (foundPage == null) {
            MessagePrinter.printMessage(MessageType.ERROR,
                    String.format("No record of primary key: (%d), was found.",
                            primaryKey));
        } else {
            // a page was found but deletion has yet to happen
            List<Record> recordsInFound = foundPage.getRecords();
            for (int i = 0; i < recordsInFound.size(); i++) {
                if (recordsInFound.get(i).compareTo(primaryKey, primaryIndex) == 0) {
                    Record removed = foundPage.deleteRecord(i);
                    return new Pair<Page, Record>(foundPage, removed);

                }
            }
            MessagePrinter.printMessage(MessageType.ERROR,
                    String.format("No record of primary key: (%d), was found.",
                            primaryKey));
        }
        return null;
    }

    /**
     * Checks if a page is empty, if it is, delete it from the database
     * if indexing is on, decrement any pageNumbeers that are affected
     * @param schema        The table schema of the table in consideration
     * @param page          The page that was deleted from and may be empty
     * @throws Exception
     */
    private void checkDeletePage(TableSchema schema, Page page) throws Exception {
        if (page.getNumRecords() == 0) {
            // begin to delete the page by moving all preceding pages up
            for (int i = 0; i < schema.getNumPages(); i++) {
                Page foundPage = this.getPage(schema.getTableNumber(), i+1);
                if (foundPage.getPageNumber() > page.getPageNumber()) {
                    foundPage.decrementPageNumber();
                    schema.setNumPages();
                }
            }

            // update the pageOrder of the schema
            schema.deletePageNumber(page.getPageNumber());
            schema.decrementPageOrder(page.getPageNumber());
        }
    }

    public Record deleteRecord(int tableNumber, Object primaryKey) throws Exception {

        TableSchema schema = Catalog.getCatalog().getSchema(tableNumber);
        Catalog catalog = Catalog.getCatalog();
        Record deletedRecord = null;
        Page deletePage = null;
        Pair<Page, Record> deletedPair = null;

        if (catalog.isIndexingOn()) {
            Node root = this.getNodePage(tableNumber, schema.getRootNumber());
            BPlusTree bPlusTree = new BPlusTree(schema, root);
            Bucket bucket = bPlusTree.search(primaryKey);
            if (bucket == null) {
                MessagePrinter.printMessage(MessageType.ERROR,
                    String.format("No record of primary key: (%d), was found.",
                            primaryKey));
            }
            Page foundPage = this.getPage(tableNumber, bucket.getPageNumber());
            Record removed = foundPage.deleteRecord(bucket.getIndex());
            deletedPair = new Pair<Page, Record>(foundPage, removed);
            bPlusTree.delete(primaryKey);
        } else {
            deletedPair = this.deleteHelper(schema, primaryKey);
        }
        deletePage = deletedPair.first;
        deletedRecord = deletedPair.second;

        schema.decrementNumRecords();
        this.checkDeletePage(schema, deletePage);
        return deletedRecord;
    }

    public void updateRecord(int tableNumber, Record newRecord, Object primaryKey) throws Exception {

        Record oldRecord = deleteRecord(tableNumber, primaryKey); // if the delete was successful then deletePage != null

        Insert insert = new Insert(Catalog.getCatalog().getSchema(tableNumber).getTableName(), null);
        InsertQueryExcutor insertQueryExcutor = new InsertQueryExcutor(insert);

        try {
            insertQueryExcutor.validateRecord(newRecord);
            this.insertRecord(tableNumber, newRecord);
        } catch (Exception e) {
            // insert failed, restore the deleted record
            this.insertRecord(tableNumber, oldRecord);
            System.err.println(e.getMessage());
            throw new Exception();
        }
    }


    /**
     * Method to drop whole tables from the DB
     *
     * @param tableNumber - the tablenumber for the table we are removing
     */
    public void dropTable(int tableNumber) {

        // Checks the hardware for a tablefile. If it finds it remove it.
        String tablePath = this.getTablePath(tableNumber);
        File tableFile = new File(tablePath);
        String indexPath = this.getIndexingPath(tableNumber);
        File indexFile = new File(indexPath);
        try {
            // if its on the file system remove it.
            if (tableFile.exists()) {
                tableFile.delete();
            }

            // if BPlus exists, drop it
            if (indexFile.exists()) {
                indexFile.delete();
            }

            // for every page in the buffer that has this table number, remove it.
            List<BufferPage> toRemove = new ArrayList<>();
            for (BufferPage page : this.buffer) {
                if (tableNumber == page.getTableNumber()) {
                    toRemove.add(page);
                }
            }

            for (BufferPage page : toRemove) {
                this.buffer.remove(page);
            }

        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

    /**
     * Method meant to alter table attributes universally
     *
     * @param tableNumber - the number of the table we are altering
     * @param op          - the operation we are performing on the table, add or
     *                    drop
     * @param attrName    - attrName name of the attr we are altering
     * @param val         - the default value if appliacable, otherwise null.
     * @return - null
     * @throws Exception
     */
    public Exception alterTable(int tableNumber, String op, String attrName, Object val, String isDeflt,
            List<AttributeSchema> attrList) throws Exception {
        Catalog catalog = Catalog.getCatalog();
        TableSchema currentSchemea = catalog.getSchema(tableNumber);
        TableSchema newSchema = new TableSchema(currentSchemea.getTableName());
        newSchema.setAttributes(attrList);

        // get all rows in old table
        List<Record> oldRecords = this.getAllRecords(tableNumber);
        List<Record> newRecords = new ArrayList<>();

        // determine value to add in the new column and add it
        Object newVal = isDeflt.equals("true") ? val : null;

        for (Record record : oldRecords) {
            if (op.equals("add")) {
                // if add col, add the new value to the record
                record.addValue(newVal);
                if (record.computeSize() > (catalog.getPageSize() - (Integer.BYTES * 2))) {
                    MessagePrinter.printMessage(MessageType.ERROR,
                            "Alter will cause a record to be greater than the page size. Aborting alter...");
                }
            } else if (op.equals("drop")) {
                // if drop col, remove the col to be removed
                List<Object> oldVals = record.getValues();
                for (int k = 0; k < currentSchemea.getAttributes().size(); k++) {
                    if (currentSchemea.getAttributes().get(k).getAttributeName().equals(attrName)) {
                        oldVals.remove(k);
                        break;
                    }
                }
                record.setValues(oldVals);
            } else {
                throw new Exception("unknown op");
            }
            newRecords.add(record);
        }

        // drop old table and create new one
        catalog.dropTableSchema(tableNumber);
        catalog.createTable(newSchema);

        for (Record record : newRecords) {
            this.insertRecord(tableNumber, record);
        }

        return null;
    }

    // ---------------------------- Page Buffer ------------------------------

    private BufferPage getLastPageInBuffer(PriorityQueue<BufferPage> buffer) {
        Object[] bufferArray = buffer.toArray();
        return ((BufferPage) bufferArray[bufferArray.length - 1]);
    }

    @Override
    public Page getPage(int tableNumber, int pageNumber) throws Exception {
        // check if page is in buffer
        for (int i = this.buffer.size()-1; i >= 0; i--) {
            Object[] bufferArray = this.buffer.toArray();
            BufferPage page = (BufferPage) bufferArray[i];
            if (page instanceof Page && page.getTableNumber() == tableNumber && page.getPageNumber() == pageNumber) {
                page.setPriority();
                return (Page) page;
            }
        }

        // read page from hardware into buffer
        readPageHardware(tableNumber, pageNumber);
        return (Page) getLastPageInBuffer(this.buffer);
    }

    public Node getNodePage(int tableNumber, int pageNumber) throws Exception {
        // Check if the node is in buffer
        for (int i = this.buffer.size() - 1; i >= 0; i--) {
            Object[] bufferArray = this.buffer.toArray();
            BufferPage page = (BufferPage) bufferArray[i];
            if (page instanceof Node && page.getTableNumber() == tableNumber && page.getPageNumber() == pageNumber) {
                page.setPriority();
                return (Node) page;
            }
        }

        // If not in buffer, read the node from hardware
        readNodePageHardware(tableNumber, pageNumber);
        return (Node) getLastPageInBuffer(this.buffer);
    }


    private void readNodePageHardware(int tableNumber, int pageNumber) throws Exception {
        Catalog catalog = Catalog.getCatalog();
        int nodeSize = catalog.getSchema(tableNumber).computeSizeOfNode(catalog);
        TableSchema tableSchema = catalog.getSchema(tableNumber);
        String filePath = this.getIndexingPath(tableNumber);
        File indexFile = new File(filePath);
        RandomAccessFile tableAccessFile = new RandomAccessFile(indexFile, "r");
        int pageIndex = pageNumber - 1;

        tableAccessFile.seek(Integer.BYTES + (nodeSize * pageIndex));
        int pageNum = tableAccessFile.readInt();
        boolean isLeaf = tableAccessFile.readBoolean();
        int parentPageNumber = tableAccessFile.readInt();
        if (pageNum != pageNumber) MessagePrinter.printMessage(MessageType.ERROR, "Page Number read does not match requested");

        if (isLeaf) {
            LeafNode leafNode = new LeafNode(tableNumber, pageNumber, parentPageNumber);
            leafNode.readFromHardware(tableAccessFile, tableSchema);
            this.addPageToBuffer(leafNode);
        } else {
            InternalNode internalNode = new InternalNode(tableNumber, pageNumber, parentPageNumber);
            internalNode.readFromHardware(tableAccessFile, tableSchema);
            this.addPageToBuffer(internalNode);
        }
        tableAccessFile.close();
    }

    private void readPageHardware(int tableNumber, int pageNumber) throws Exception {
        Catalog catalog = Catalog.getCatalog();
        TableSchema tableSchema = catalog.getSchema(tableNumber);
        String filePath = this.getTablePath(tableNumber);
        File tableFile = new File(filePath);
        RandomAccessFile tableAccessFile = new RandomAccessFile(tableFile, "r");
        int pageIndex = pageNumber - 1;

        tableAccessFile.seek(Integer.BYTES + (catalog.getPageSize() * pageIndex)); // start after numPages
        int numRecords = tableAccessFile.readInt();
        int pageNum = tableAccessFile.readInt();
        if (pageNum != pageNumber) MessagePrinter.printMessage(MessageType.ERROR, "Page Number read does not match requested");
        Page page = new Page(numRecords, tableNumber, pageNum);
        page.readFromHardware(tableAccessFile, tableSchema);
        this.addPageToBuffer(page);
        tableAccessFile.close();
    }

    private void writeNodePageHardware(BufferPage page) throws Exception {
        Catalog catalog = Catalog.getCatalog();
        int nodeSize = catalog.getSchema(page.getTableNumber()).computeSizeOfNode(catalog);
        TableSchema tableSchema = catalog.getSchema(page.getTableNumber());
        String filePath = this.getIndexingPath(page.getTableNumber());
        File indexFile = new File(filePath);
        RandomAccessFile tableAccessFile = new RandomAccessFile(indexFile, "rw");
        tableAccessFile.writeInt(tableSchema.getNumIndexPages());
        int nodeIndex = page.getPageNumber() - 1;

        tableAccessFile.seek(tableAccessFile.getFilePointer() + (nodeSize * nodeIndex));

        Random random = new Random();
        byte[] buffer = new byte[nodeSize];
        random.nextBytes(buffer);
        tableAccessFile.write(buffer, 0, nodeSize);
        tableAccessFile.seek(tableAccessFile.getFilePointer() - nodeSize);

        page.writeToHardware(tableAccessFile);
        tableAccessFile.close();
    }

    private void writePageHardware(BufferPage page) throws Exception {
        Catalog catalog = Catalog.getCatalog();
        TableSchema tableSchema = catalog.getSchema(page.getTableNumber());
        String filePath = this.getTablePath(page.getTableNumber());
        File tableFile = new File(filePath);
        RandomAccessFile tableAccessFile = new RandomAccessFile(tableFile, "rw");
        tableAccessFile.writeInt(tableSchema.getNumPages());
        int pageIndex = page.getPageNumber() - 1;

        // Go to the point where the page is in the file
        tableAccessFile.seek(tableAccessFile.getFilePointer() + (catalog.getPageSize() * pageIndex));

        // Allocate space for a Page in the table file
        Random random = new Random();
        byte[] buffer = new byte[catalog.getPageSize()];
        random.nextBytes(buffer);
        tableAccessFile.write(buffer, 0, catalog.getPageSize());
        tableAccessFile.seek(tableAccessFile.getFilePointer() - catalog.getPageSize()); // move pointer back

        page.writeToHardware(tableAccessFile);
        tableAccessFile.close();
    }

    public void addPageToBuffer(BufferPage page) throws Exception {
        if (this.buffer.size() == this.bufferSize) {
            BufferPage lruPage = this.buffer.poll(); // assuming the first Page in the buffer is LRU
            if (lruPage.isChanged()) {
                if (lruPage instanceof Page) {
                    this.writePageHardware(lruPage);
                } else if (lruPage instanceof Node) {
                    this.writeNodePageHardware(lruPage);
                } else {
                    MessagePrinter.printMessage(MessageType.ERROR, "Unknown BufferPage type: addPageToBuffer");
                }
            }
        }
        this.buffer.add(page);
    }

    public void writeAll() throws Exception {
        for (BufferPage page : buffer) {
            if (page.isChanged()) {
                if (page instanceof Page) {
                    writePageHardware(page);
                }  else if (page instanceof Node) {
                    writeNodePageHardware(page);
                } else {
                    MessagePrinter.printMessage(MessageType.ERROR, "Unknown BufferPage type: addPageToBuffer");
                }
            }
        }
        this.buffer.removeAll(buffer);
    }

}
