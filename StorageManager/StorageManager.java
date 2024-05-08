package StorageManager;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.RandomAccessFile;
import java.lang.management.MemoryType;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Random;

import BPlusTree.BPlusNode;
import BPlusTree.Bucket;
import BPlusTree.InternalNode;
import BPlusTree.LeafNode;
import Parser.Insert;
import Parser.Type;
import QueryExecutor.InsertQueryExcutor;
import StorageManager.Objects.AttributeSchema;
import StorageManager.Objects.BufferPage;
import StorageManager.Objects.Catalog;
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
                page.addNewRecord(record);
                newPage.addNewRecord(lastRecordInCurrPage);
            } else {
                newPage.addNewRecord(record);
            }
        } else {
            splitIndex = (int) Math.floor(page.getRecords().size() / 2);

            // Move half of the records to the new page
            for (Record copyRecord : page.getRecords().subList(splitIndex, page.getRecords().size())) {
                if (!newPage.addNewRecord(copyRecord)) {
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
                if (!page.addNewRecord(record)) {
                    List<Page> temp = pageSplit(page, record, tableSchema, primaryKeyIndex);
                    results.remove(page);
                    results.addAll(temp);
                }
            } else {
                if (!newPage.addNewRecord(record)) {
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
            _new.addNewRecord(record);
            tableSchema.incrementNumRecords();
            // then add the page to the buffer
            this.addPageToBuffer(_new);

            if (catalog.isIndexingOn()) {
                // check if an index file already exists, if not create it, it should never exist at this point
                // if it does error out saying the database is corrupted
                if (!indexFile.exists()) {
                    indexFile.createNewFile();
                    // create a new leaf node and insert the new pk into it, this will be the first root node
                    LeafNode root = new LeafNode(tableNumber, 1, tableSchema.computeN(catalog), -1);
                    tableSchema.incrementNumIndexPages();
                    tableSchema.setRoot(1);
                    Bucket bucket = new Bucket(1, 0, record.getValues().get(primaryKeyIndex));
                    root.addBucket(bucket, -1);

                    // then add the page to the buffer
                    this.addPageToBuffer(root);
                } else {
                    MessagePrinter.printMessage(MessageType.ERROR, "Database is corrupted. Index file found before table file was created.");
                }

                return;
            }
        } else {

            if (tableSchema.getNumPages() == 0) {
                Page _new = new Page(0, tableNumber, 1);
                tableSchema.addPageNumber(_new.getPageNumber());
                _new.addNewRecord(record);
                tableSchema.incrementNumRecords();
                // then add the page to the buffer
                this.addPageToBuffer(_new);
                if (catalog.isIndexingOn() && tableSchema.getNumIndexPages() == 0) {
                    // create a new leaf node and insert the new record into it, this will be the first root node
                    LeafNode root = new LeafNode(tableNumber, 1, tableSchema.computeN(catalog), -1);
                    Bucket bucket = new Bucket(1, 0, record.getValues().get(primaryKeyIndex));
                    root.addBucket(bucket, -1);
                    if (root.getPageNumber() < 0 ) {
                        System.out.println("flag");
                    }
                    tableSchema.incrementNumIndexPages();
                    tableSchema.setRoot(1);

                    // then add the page to the buffer
                    this.addPageToBuffer(root);
                }
            } else {

                if (catalog.isIndexingOn()) {
                    Pair<Integer, Integer> location = this.insertIndex(record, tableNumber, tableSchema, catalog);

                    if (location == null) {
                        MessagePrinter.printMessage(MessageType.ERROR, "Error in traversing B+ Tree");
                        return;
                    }

                    // get page and insert
                    Page page = this.getPage(tableNumber, location.first);
                    if (!page.addNewRecord(record, location.second)) {
                        // page was full
                        List<Page> pages = this.pageSplit(page, record, tableSchema, primaryKeyIndex);
                        Type pkType = tableSchema.getAttributeType(primaryKeyIndex);
                        for (Page p : pages) {
                            // for every page present in the split
                            // find the first leaf node that we need
                            int pageIndex = 0;
                            Pair<Integer, Integer> searchLocation = new Pair<Integer,Integer>(tableSchema.getRootNumber(), -1);
                            BPlusNode node = null;
                            Object firstSK = p.getRecords().get(0).getValues().get(primaryKeyIndex);

                            do {
                                // read in node
                                node = this.getIndexPage(tableNumber, searchLocation.first);
                                searchLocation = node.search(firstSK, pkType);
                            } while (searchLocation != null && searchLocation.second == -1);

                            try {
                                // stop when we get the first LeaFNode
                                LeafNode leaf = (LeafNode)node;
                                while (pageIndex < p.getRecords().size()) {
                                    pageIndex = leaf.replacePointerMultiple(pageIndex, pkType, p, primaryKeyIndex);

                                    if (leaf.getNextLeaf() == null) {
                                        break;
                                    }

                                    leaf = (LeafNode)this.getIndexPage(tableNumber, leaf.getNextLeaf().first);

                                }
                            } catch (Exception e) {
                                MessagePrinter.printMessage(MessageType.ERROR, e.getMessage() + ": insertRecord.indexing");
                            }
                        }
                    }
                    tableSchema.incrementNumRecords();

                } else {

                    for (Integer pageNumber : tableSchema.getPageOrder()) {
                        Page page = this.getPage(tableNumber, pageNumber);
                        if (page.getNumRecords() == 0) {
                            if (!page.addNewRecord(record)) {
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
                            if (!page.addNewRecord(record)) {
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
    }

    /**
     * Insert a record by finding where it should be insertted into based on the B+ tree
     * Traverse the B+ tree, insert the SK into the tree. Return a pointer to where it should be insertted
     * Insert the record into the DB. Increment the pointer of all pointers in the B+ tree after the newly insertted record
     * If a page splits, update the pointers of all affected buckets in the B+ tree
     * @param record            The record to insert
     * @param tableNumber       The table to insert into
     * @param tableSchema       The schema of the table
     * @param catalog           The catalog of the database
     * @return                  The location as to where to insert the record
     * @throws Exception
     */
    private Pair<Integer, Integer> insertIndex(Record record, int tableNumber, TableSchema tableSchema, Catalog catalog) throws Exception {
        // set up while loop with the root as the first to search
        int primaryKeyIndex = tableSchema.getPrimaryIndex();
        Object pk = record.getValues().get(primaryKeyIndex);
        Type pkType = tableSchema.getAttributeType(primaryKeyIndex);
        int n = tableSchema.computeN(catalog);

        Pair<Integer, Integer> location = new Pair<Integer,Integer>(tableSchema.getRootNumber(), -1);
        BPlusNode node = null;

        while (location != null && location.second == -1) {
            // read in node
            node = this.getIndexPage(tableNumber, location.first);
            location = node.insert(pk, pkType);
        }

        int leafNodeFound = node.getPageNumber();

        if (location == null) {
            MessagePrinter.printMessage(MessageType.ERROR, "Error in traversing B+ Tree");
            return null;
        }

        while (node.overfull()) {
            // get array in node
            InternalNode parent = null;
            if (node.getParent() == -1) {
                // this is the root node that is overfull
                // create new parent node which will be the new root
                parent = new InternalNode(tableNumber, tableSchema.incrementNumIndexPages(), n, -1);
                node.setParent(parent.getPageNumber());
                tableSchema.setRoot(parent.getPageNumber());
                parent.addPointer(new Pair<Integer,Integer>(node.getPageNumber(), -1), 0);
                this.addPageToBuffer(parent);
            } else {
                parent = (InternalNode)this.getIndexPage(tableNumber, node.getParent()); // only internals can be a parent
            }

            if (node instanceof InternalNode) {
                InternalNode internal = (InternalNode)node;
                ArrayList<Object> searchKeys = internal.getSK();
                ArrayList<Pair<Integer, Integer>> pointers = internal.getPointers();

                // split search keys into two
                int skNum = searchKeys.size();
                List<Object> firstSK = new ArrayList<>();
                List<Object> secondSK = new ArrayList<>();
                firstSK.addAll(searchKeys.subList(0, skNum/2));
                Object goingUp = searchKeys.get(skNum/2);
                secondSK.addAll(searchKeys.subList(skNum/2+1, skNum));

                // split pointers into two
                int pointNum = pointers.size();
                double res = pointNum / 2;
                int splitIndex = (int) Math.ceil(res);
                List<Pair<Integer, Integer>> firstPointers = new ArrayList<>();
                List<Pair<Integer, Integer>> secondPointers = new ArrayList<>();
                firstPointers.addAll(pointers.subList(0, splitIndex));
                // no need for a going up for the pointers
                secondPointers.addAll(pointers.subList(splitIndex, pointNum));

                InternalNode newNode = new InternalNode(tableNumber, tableSchema.incrementNumIndexPages(), n, parent.getPageNumber());

                // set the searck keys and pointers for the two child nodes
                internal.setSK(firstSK);
                internal.setPointers(firstPointers);
                newNode.setSK(secondSK);
                newNode.setPointers(secondPointers);

                // append new search key to parent
                // append new pointer to parent
                parent.addPointer(new Pair<Integer, Integer>(newNode.getPageNumber(), -1), node.getPageNumber(), false);
                parent.addSearchKey(goingUp, node.getPageNumber(), false);

                this.addPageToBuffer(newNode);

                // now that we have a new internal node, update the parent pointer of all child nodes of the newNode
                for (Pair<Integer, Integer> i : newNode.getPointers()) {
                    BPlusNode child = this.getIndexPage(tableNumber, i.first);
                    child.setParent(newNode.getPageNumber());
                }

            } else if (node instanceof LeafNode) {
                LeafNode leaf = (LeafNode)node;
                ArrayList<Bucket> sks = leaf.getSK();

                // split into two
                List<Bucket> first = new ArrayList<>();
                List<Bucket> second= new ArrayList<>();
                first.addAll(sks.subList(0, sks.size()/2));
                second.addAll(sks.subList(sks.size()/2, sks.size()));

                // get next search key
                Object goingUp = second.get(0).getPrimaryKey();

                // create new node
                LeafNode newNode = new LeafNode(tableNumber, tableSchema.incrementNumIndexPages(), n, parent.getPageNumber());
                leaf.setSK(first);
                newNode.setSK(second);

                if (leaf.getNextLeaf() != null) {
                    newNode.assignNextLeaf(leaf.getNextLeaf().first);
                }

                leaf.assignNextLeaf(newNode.getPageNumber());

                // append new search key to parent
                parent.addPointer(new Pair<Integer, Integer>(newNode.getPageNumber(), -1), node.getPageNumber(), false);
                parent.addSearchKey(goingUp, node.getPageNumber(), false);

                this.addPageToBuffer(newNode);
            }

            // move up in the tree and repeat
            if (parent == null) {
                break;
            } else {
                node = parent;
            }
        }

        this.incrementIndexPointer(tableNumber, leafNodeFound, location.second, location.first, pk, pkType);
        return location;
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

            // if indexing on, start at first leaf node, loop through all and decrement any pages that has a greater page number than the one being deleted
            if (Catalog.getCatalog().isIndexingOn()) {
                try {
                    // get first leaf node
                    Pair<Integer, Integer> location = new Pair<Integer,Integer>(schema.getRootNumber(), -1);
                    BPlusNode node = this.getIndexPage(schema.getTableNumber(), location.first);
                    while (location != null && node instanceof InternalNode) {
                        location = ((InternalNode)node).getPointers().get(0);
                        node = this.getIndexPage(schema.getTableNumber(), location.first);
                    }

                    // the pointer points to a leaf node now
                    LeafNode leaf = null;
                    int leafNum = location.first;

                    while (true) {
                        leaf = (LeafNode)this.getIndexPage(schema.getTableNumber(), leafNum);
                        leaf.decrementPointerPage(page.getPageNumber());
                        if (leaf.getNextLeaf() != null) {
                            leafNum = leaf.getNextLeaf().first;
                        } else {
                            // no more leaves to read in
                            break;
                        }
                    };
                } catch (Exception e) {
                    MessagePrinter.printMessage(MessageType.ERROR, e.getMessage() + ": checkDeletePage.indexing");
                }

            }

        }
    }

    public Record deleteRecord(int tableNumber, Object primaryKey) throws Exception {

        TableSchema schema = Catalog.getCatalog().getSchema(tableNumber);
        Catalog catalog = Catalog.getCatalog();
        Record deletedRecord = null;
        Page deletePage = null;
        Pair<Page, Record> deletedPair = null;

        if (catalog.isIndexingOn()) {
            deletedPair = deleteIndex(tableNumber, primaryKey, schema, catalog);
            if (deletedPair == null) {
                MessagePrinter.printMessage(MessageType.ERROR, "Error in traversing B+ Tree");
                return null;
            }

        } else {
            deletedPair = this.deleteHelper(schema, primaryKey);
        }

        deletePage = deletedPair.first;
        deletedRecord = deletedPair.second;

        schema.decrementNumRecords();
        this.checkDeletePage(schema, deletePage);
        return deletedRecord;
    }

    /**
     * Determines where a record is based off the B+ tree and deletes it from the tree
     * then returns the location of the record
     * @param tableNumber       The table to delete from
     * @param primaryKey        The primary key of the record to delete
     * @param tableSchema       The table's schema
     * @param catalog           The catalog of the DB
     * @return                  The location/pointer of where the record is in the DB
     * @throws Exception
     */
    private Pair<Page, Record> deleteIndex(int tableNumber, Object primaryKey, TableSchema tableSchema, Catalog catalog) throws Exception {
        TableSchema schema = Catalog.getCatalog().getSchema(tableNumber);
        Page deletePage = null;

        // get pk data type
        Type pkType = schema.getAttributeType(schema.getPrimaryIndex());

        // find location
        Pair<Integer, Integer> location = new Pair<Integer,Integer>(schema.getRootNumber(), -1);
        BPlusNode node = null;
        while (location != null && location.second == -1) {
            // read in node
            node = this.getIndexPage(tableNumber, location.first);
            location = node.delete(primaryKey, pkType);
        }

        if (location == null) {
            MessagePrinter.printMessage(MessageType.ERROR, "Error in traversing B+ Tree");
            return null;
        }

        int leafNodeFound = node.getPageNumber();

        while (node.underfull()) {
            // get array in node
            InternalNode parent = null;
            if (node.getParent() == -1) {
                // basically check to see if the root is ACTUALLY underfull, meaning it has less then 2 childrens, if so make the root a leafnode
                if (node instanceof InternalNode) {
                    InternalNode root = (InternalNode) node;
                    ArrayList<Pair<Integer, Integer>> pointers = root.getPointers();
                    if (pointers.size() < 2) {
                        if (pointers.size() == 1) {
                            // make the next node the new root
                            BPlusNode newRoot = this.getIndexPage(tableNumber, pointers.get(0).first);
                            this.deleteIndexNode(node, schema);
                            newRoot.setPageNumber(1);
                            tableSchema.setRoot(newRoot.getPageNumber());
                        } else if (pointers.size() == 0) {
                            // not sure how this can happen, but if it does, create a new empty root as a leafnode and replace the old one
                            LeafNode newRoot = new LeafNode(tableNumber, 1, tableSchema.computeN(catalog), -1);
                            tableSchema.setRoot(newRoot.getPageNumber());
                            this.addPageToBuffer(newRoot);
                        }
                    }
                } else if (node instanceof LeafNode) {
                    // actually should be fine if we do nothing here and let it be empty
                    deleteIndexNode(node, schema);
                    deletePage = this.getPage(tableNumber, location.first);
                    return new Pair<Page,Record>(deletePage, deletePage.deleteRecord(location.second));
                }
            } else {
                parent = (InternalNode) this.getIndexPage(tableNumber, node.getParent());
                if (node instanceof InternalNode) {
                    // an internal node will be underfull if it has less than Math.Ceil(n/2) children
                    InternalNode curr = (InternalNode) node;

                    // retrieve neighbors
                    Pair<Integer, Integer> neighbors = parent.getNeighbors(curr.getPageNumber());
                    InternalNode left = neighbors.first < 0 ? null : (InternalNode) this.getIndexPage(tableNumber, neighbors.first);
                    InternalNode right = neighbors.second < 0 ? null : (InternalNode) this.getIndexPage(tableNumber, neighbors.second);

                    // for internal nodes:

                    if (left == null || left.willOverfull(curr.getSK().size()+1)) {
                        if (right == null || right.willOverfull(curr.getSK().size()+1)) {
                            if (left == null || left.willUnderfull()) {
                                if (right == null || right.willUnderfull()) {
                                    // this should never happen
                                    MessagePrinter.printMessage(MessageType.ERROR, "An error that should never happen has been reached: BPlusUnderfull");
                                } else {
                                    // borrow right
                                    // a borrow right consists of these actions:
                                    // move first search key in right neighbor up replacing the search key that was less than the pointer to right neighbor, but greater than current
                                    Object firstSK = right.deleteSK(0);
                                    Object borrowedSK = parent.replaceSearchKey(firstSK, curr.getPageNumber(), false);

                                    // move the search key in the parent that got replaced down to be the last search key in the current node
                                    curr.addSearchKey(borrowedSK, -1);

                                    // set the first pointer in right neighbor, to be the last pointer in the current node
                                    // delete the first pointer in right neighbor
                                    Pair<Integer, Integer> firstPointer = right.removePointer(0);
                                    curr.addPointer(firstPointer, -1);

                                }
                            } else {
                                // borrow left
                                // a borrow left consists of these actions:
                                // move last search key in left neighbor up replacing the search key that was greater than the pointer to left neighbor, but less than current
                                Object lastSK = left.deleteSK(-1);
                                Object borrowedSK = parent.replaceSearchKey(lastSK, curr.getPageNumber(), true);

                                // move the search key in the parent that got replaced down to be the first search key in the current node
                                curr.addSearchKey(borrowedSK, 0);

                                // set the last pointer in left neighbor, to be the first pointer in the current node
                                // delete the last pointer in left neighbor
                                Pair<Integer, Integer> lastPointer = left.removePointer(-1);
                                curr.addPointer(lastPointer, 0);
                            }
                        } else {
                            // attempt to merge right
                            // a merge right consists of these actions:
                            // bring the search key in the parent node that is separating left neighbor and current node down
                            // to form a new array of (curr SKs) + (parent SK) + (right Sks)
                            List<Object> currSK = curr.getSK();
                            Object parentSK = parent.getSearchKey(curr.getPageNumber(), false);
                            List<Object> rightSK = right.getSK();

                            List<Object> newSK = new ArrayList<>();
                            newSK.addAll(currSK);
                            newSK.add(parentSK);
                            newSK.addAll(rightSK);

                            // set the new array into the right neighbor
                            right.setSK(newSK);

                            // new array of pointers is (curr Pointers) + (right Pointers)
                            List<Pair<Integer, Integer>> currPointers = curr.getPointers();
                            currPointers.addAll(right.getPointers());
                            right.setPointers(currPointers);

                            // delete the curr node
                            this.deleteIndexNode(node, schema);
                        }

                    } else {
                        // attempt to merge left
                        // a merge left consists of these actions:
                        // bring the search key in the parent node that is separating left neighbor and current node down
                        // to form a new array of (left SKs) + (parent SK) + (curr Sks)
                        List<Object> currSK = curr.getSK();
                        Object parentSK = parent.getSearchKey(curr.getPageNumber(), true);
                        List<Object> leftSK = left.getSK();

                        List<Object> newSK = new ArrayList<>();
                        newSK.addAll(leftSK);
                        newSK.add(parentSK);
                        newSK.addAll(currSK);

                        // set the new array into the left neighbor
                        left.setSK(newSK);

                        // new array of pointers is (left Pointers) + (curr Pointers)
                        List<Pair<Integer, Integer>> leftPointers = left.getPointers();
                        leftPointers.addAll(curr.getPointers());
                        left.setPointers(leftPointers);

                        // delete the curr node
                        this.deleteIndexNode(node, schema);
                    }


                } else if (node instanceof LeafNode) {
                    LeafNode curr = (LeafNode) node;
                    // leafnode will be underfull if it has less than Math.Ceil((n-1)/2) search keys

                    // retrieve neighbors
                    Pair<Integer, Integer> neighbors = parent.getNeighbors(curr.getPageNumber());
                    LeafNode left = neighbors.first < 0 ? null : (LeafNode) this.getIndexPage(tableNumber, neighbors.first);
                    LeafNode right = neighbors.second < 0 ? null : (LeafNode) this.getIndexPage(tableNumber, neighbors.second);

                    if (left == null || left.willOverfull(curr.getSK().size())) {
                        if (right == null || right.willOverfull(curr.getSK().size())) {
                            if (left == null || left.willUnderfull()) {
                                if (right == null || right.willUnderfull()) {
                                    // this should never happen
                                    MessagePrinter.printMessage(MessageType.ERROR, "An error that should never happen has been reached: BPlusUnderfull");
                                } else {
                                    // borrow right, borrows first element
                                    Bucket bucket = right.getSK().get(0);
                                    right.removeSearchKey(0);
                                    curr.addBucket(bucket, -1);
                                    parent.replaceSearchKey(right.getSK().get(0).getPrimaryKey(), curr.getPageNumber(), false);
                                }
                            } else {
                                // borrow left, borrows last element
                                Bucket bucket = left.getSK().get(-1);
                                left.removeSearchKey(-1);
                                curr.addBucket(bucket, 0);
                                parent.replaceSearchKey(bucket.getPrimaryKey(), curr.getPageNumber(), true);
                            }
                        } else {
                            // merge right, append this to the beginning of right's array
                            ArrayList<Bucket> currSK = curr.getSK();
                            ArrayList<Bucket> rightSK = right.getSK();

                            currSK.addAll(rightSK);
                            right.setSK(currSK);
                            int currPageNum = curr.getPageNumber();
                            parent.removeSearchKey(currPageNum, false);

                            // update the previous leafnode's pointer to the next leafNode to this one
                            BPlusNode searchLeaf = null;
                            Pair<Integer, Integer> finder = new Pair<Integer,Integer>(schema.getRootNumber(), -1);

                            // TODO potential error here
                            boolean noPrevious = false;
                            try {
                                while (finder.first != currPageNum) {
                                    searchLeaf = this.getIndexPage(tableNumber, finder.first);
                                    if (searchLeaf instanceof InternalNode) {
                                        // go down leftmosr side of the tree
                                        finder = ((InternalNode)searchLeaf).getPointers().get(0);
                                    } else {
                                        LeafNode leaf = (LeafNode)searchLeaf;
                                        if (leaf.getNextLeaf() == null) {
                                            // the node that was deleted was the firs tleaf node
                                            // no need to update a nextLeaf
                                            noPrevious = true;
                                            break;
                                        }

                                        // if we're at the leaf node level already
                                        // move to the next leaf node until we find what we need
                                        finder = leaf.getNextLeaf();
                                    }
                                }

                                

                                // read one last time
                                if (!noPrevious) {
                                    searchLeaf = (LeafNode)this.getIndexPage(tableNumber, finder.first);
                                    ((LeafNode)searchLeaf).assignNextLeaf(right.getPageNumber());
                                }

                                leafNodeFound = right.getPageNumber() > node.getPageNumber() ? 
                                    right.getPageNumber()-1 : right.getPageNumber();
                                this.deleteIndexNode(node, schema);
                            } catch (Exception e) {
                                MessagePrinter.printMessage(MessageType.ERROR, e.getMessage() + ": deleteIndex");
                            }
                        }
                    } else {
                        // merge left, append this to the end of left's array
                        ArrayList<Bucket> currSK = curr.getSK();
                        ArrayList<Bucket> leftSK = left.getSK();

                        leftSK.addAll(currSK);
                        left.setSK(leftSK);
                        left.assignNextLeaf(curr.getNextLeaf().first);

                        parent.removeSearchKey(curr.getPageNumber(), true);
                        leafNodeFound = left.getPageNumber() > node.getPageNumber() ? 
                                    left.getPageNumber()-1 : left.getPageNumber();
                        this.deleteIndexNode(node, schema);
                    }
                }
            }
            // move up in the tree and repeat
            if (parent == null) {
                break;
            } else {
                node = parent;
            }
        }

        // get page and delete
        deletePage = this.getPage(tableNumber, location.first);
        // decrement all records after the deleted record in B+ tree
        this.decrementIndexPointer(tableNumber, leafNodeFound, location.second, location.first, primaryKey, pkType);
        return new Pair<Page, Record>(deletePage, deletePage.deleteRecord(location.second));
    }

    /**
     * After a deletion decrement all records in the page that appears after the deleted record
     * @param tableNum  The table number
     * @param leafNum   The leaf node number to begin consideration
     * @param index     The index of the deleted record
     * @param pageNum   The pageNum that actual record was initially on
     * @throws Exception
     */
    private void decrementIndexPointer(int tableNum, int leafNum, int index, int pageNum, Object searchKey, Type type) throws Exception {
        LeafNode leaf = null;
        boolean end = false;
        boolean started = false;

        do {
            try {
                leaf = (LeafNode)this.getIndexPage(tableNum, leafNum);
                List<Bucket> buckets = leaf.getSK();
                for (Bucket b : buckets) {
                    // for every bucket, if the pageNum is the same
                    // check if the index is after the deleted index, if so decrement
                    if (b.getPageNumber() == pageNum) {
                        if (!started) {
                            started = true;
                        }
                    }
                    
                    if (started && b.getPageNumber() != pageNum) {
                        end = true; // we have moved on to the next page
                        break;
                    }

                    if (b.getIndex() >= index && leaf.compareKey(searchKey, b.getPrimaryKey(), type) != 0 &&
                        b.getPageNumber() == pageNum) {
                        b.setPointer(pageNum, b.getIndex()-1);
                    }
                }

                if (leaf.getNextLeaf() != null) {
                    if (started == false) {
                        break;
                    }
                    leafNum = leaf.getNextLeaf().first;
                } else {
                    break;
                }
            } catch (Exception e) {
                MessagePrinter.printMessage(MessageType.ERROR, e.getMessage() + ": incrementIndexPointer ");
            }
        } while (!end);
    }

    /**
     * After an insertion increment all records in the page that appears after the inserted record
     * @param tableNum  The table number
     * @param leafNum   The leaf node number to begin consideration
     * @param index     The index of the inserted record
     * @param pageNum   The pageNum that actual record was initially on
     * @throws Exception
     */
    private void incrementIndexPointer(int tableNum, int leafNum, int index, int pageNum, Object searchKey, Type type) throws Exception {
        LeafNode leaf = null;
        boolean end = false;
        boolean started = false;

        do {
            try {
                leaf = (LeafNode)this.getIndexPage(tableNum, leafNum);
                List<Bucket> buckets = leaf.getSK();
                for (Bucket b : buckets) {
                    // for every bucket, if the pageNum is the same
                    // check if the index is after the deleted index, if so increment
                    if (b.getPageNumber() == pageNum) {
                        if (!started) {
                            started = true;
                        }
                    } else if (started && b.getPageNumber() != pageNum) {
                        end = true; // we have moved on to the next page
                        break;
                    }

                    if (b.getIndex() >= index && leaf.compareKey(searchKey, b.getPrimaryKey(), type) != 0 &&
                        b.getPageNumber() == pageNum) {
                        b.setPointer(pageNum, b.getIndex()+1);
                    }
                }

                if (leaf.getNextLeaf() != null) {
                    leafNum = leaf.getNextLeaf().first;
                } else {
                    break;
                }
            } catch (Exception e) {
                MessagePrinter.printMessage(MessageType.ERROR, e.getMessage() + ": incrementIndexPointer ");
            }

        } while (!end);
    }

    /**
     * Deletes a BPlusNode from the tree and updates all pointers
     * @param node          The node to delete
     * @param schema        The table's schema
     * @throws Exception
     */
    private void deleteIndexNode(BPlusNode node, TableSchema schema) throws Exception {
        int deletedPageNum = node.getPageNumber();

        // decrement all node's page number
        for (int i = deletedPageNum; i < schema.getNumIndexPages(); i++) {
            // read in all nodes, if the read in node's page number is higher than the deleted
            // then decrement
            BPlusNode currNode = this.getIndexPage(schema.getTableNumber(), i);

            if (currNode.getPageNumber() > deletedPageNum) {
                if (schema.getRootNumber() == currNode.getPageNumber()) {
                    schema.setRoot(currNode.getPageNumber()-1);
                }
                currNode.setPageNumber(currNode.getPageNumber()-1);

            }

            // for every read in node, loop through it's pointers and decrement any pointer to a
            // BPlusNode whose page is greater than the deleted node
            // this will also remove the deleted node from any internal node that pointed to it
            currNode.decrementNodePointerPage(deletedPageNum);
        }
        schema.decrementNumIndexPages();

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

    @Override
    public BPlusNode getIndexPage(int tableNumber, int pageNumber) throws Exception {
        // check if page is in buffer
        for (int i = this.buffer.size()-1; i >= 0; i--) {
            Object[] bufferArray = this.buffer.toArray();
            BufferPage page = (BufferPage) bufferArray[i];
            if (page instanceof BPlusNode && page.getTableNumber() == tableNumber && page.getPageNumber() == pageNumber) {
                page.setPriority();
                return (BPlusNode) page;
            }
        }

        // read page from hardware into buffer
        readIndexPageHardware(tableNumber, pageNumber);
        return (BPlusNode) getLastPageInBuffer(this.buffer);
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
        Page page = new Page(numRecords, tableNumber, pageNum);
        page.readFromHardware(tableAccessFile, tableSchema);
        this.addPageToBuffer(page);
        tableAccessFile.close();
    }

    private void readIndexPageHardware(int tableNumber, int pageNumber) throws Exception {
        Catalog catalog = Catalog.getCatalog();
        TableSchema tableSchema = catalog.getSchema(tableNumber);
        String filePath = this.getIndexingPath(tableNumber);
        File tableIndexFile = new File(filePath);
        RandomAccessFile tableIndexAccessFile = new RandomAccessFile(tableIndexFile, "r");
        int nodeSize = tableSchema.computeSizeOfNode(catalog);
        int nodeIndex = pageNumber - 1;

        tableIndexAccessFile.seek(nodeIndex * nodeSize);

        boolean nodeType = tableIndexAccessFile.readBoolean();

        if (nodeType) {
            pageNumber = tableIndexAccessFile.readInt();
            int parent = tableIndexAccessFile.readInt();
            LeafNode leafNode = new LeafNode(tableNumber, pageNumber, tableSchema.computeN(catalog), parent);
            leafNode.readFromHardware(tableIndexAccessFile, tableSchema);
            this.addPageToBuffer(leafNode);
        } else {
            pageNumber = tableIndexAccessFile.readInt();
            int parent = tableIndexAccessFile.readInt();
            InternalNode internalNode = new InternalNode(tableNumber, pageNumber, tableSchema.computeN(catalog), parent);
            internalNode.readFromHardware(tableIndexAccessFile, tableSchema);
            this.addPageToBuffer(internalNode);
        }
        tableIndexAccessFile.close();

    }

    private void writeIndexPageHardware(BPlusNode page) throws Exception {
        Catalog catalog = Catalog.getCatalog();
        TableSchema tableSchema = catalog.getSchema(page.getTableNumber());
        String filePath = this.getIndexingPath(page.getTableNumber());
        File tableIndexFile = new File(filePath);
        RandomAccessFile tableIndexAccessFile = new RandomAccessFile(tableIndexFile, "rw");
        int nodeSize = tableSchema.computeSizeOfNode(catalog);
        int nodeIndex = page.getPageNumber() - 1;

        // got to node location
        tableIndexAccessFile.seek(nodeIndex * nodeSize);

        // allocate space for max size a node can be
        Random random = new Random();
        byte[] buffer = new byte[nodeSize];
        random.nextBytes(buffer);
        tableIndexAccessFile.write(buffer, 0, nodeSize);
        tableIndexAccessFile.seek(tableIndexAccessFile.getFilePointer() - nodeSize); // move pointer back
        page.writeToHardware(tableIndexAccessFile);
        tableIndexAccessFile.close();


    }

    private void writePageHardware(BufferPage page) throws Exception {
        // TODO check between indexPage and regular page
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

    private void addPageToBuffer(BufferPage page) throws Exception {
        if (this.buffer.size() == this.bufferSize) {
            BufferPage lruPage = this.buffer.poll(); // assuming the first Page in the buffer is LRU
            if (lruPage.isChanged()) {
                if (lruPage instanceof Page) {
                    this.writePageHardware(lruPage);
                } else if (lruPage instanceof BPlusNode) {
                    this.writeIndexPageHardware((BPlusNode)lruPage);
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
                if (page instanceof BPlusNode) {
                    writeIndexPageHardware((BPlusNode) page);
                } else {
                    writePageHardware(page);
                }
            }
        }
        this.buffer.removeAll(buffer);
    }

}
