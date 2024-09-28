package StorageManager.Objects;

import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import StorageManager.StorageManager;
import StorageManager.TableSchema;
import StorageManager.Objects.MessagePrinter.MessageType;

public class Page extends BufferPage {
    private int numRecords;
    private List<Record> records;

    public Page(int numRecords, int tableNumber, int pageNumber) {
        super(tableNumber, pageNumber);
        this.numRecords = numRecords;
        this.changed = false;
        this.records = new ArrayList<>();
    }

    public Page() {
        super(0, 0);
    }

    public int getNumRecords() {
        return numRecords;
    }

    public void setNumRecords() {
        this.numRecords = this.records.size();
    }

    public List<Record> getRecords() {
        return records;
    }

    public void setRecords(List<Record> records) {
        this.records = records;
        this.setNumRecords();
    }

    public int getRecordLocation(Record record, int primaryKeyIndex) throws Exception {
        for (int i = 0; i < this.records.size(); i++) {
            if (record.compareTo(this.records.get(i), primaryKeyIndex) == 0) {
                return i;
            }
        }
        // error 404
        MessagePrinter.printMessage(MessageType.ERROR, "Unable to find record in page: getRecordLocation");
        return -1;
    }

    /**
     * Adds a record to the page in the correct order
     *
     * @param record    The record to be inserted
     *
     * @return          true: insert success
     *                  false: page is full
     * @throws Exception
     */
    public boolean addNewRecord(Record record, boolean includeInsertedRecordInBucketUpdate) throws Exception {
        Catalog catalog = Catalog.getCatalog();
        // check if record can fit in this page.
        if ((catalog.getPageSize() - this.computeSize()) < record.computeSize()) {
            return false;
        } else {
            Map<Integer, TableSchema> schemas = catalog.getSchemas();
            TableSchema schema = schemas.get(this.tableNumber);
            int primaryIndex = schema.getPrimaryIndex();
            String primaryType = schema.getAttributes().get(primaryIndex).getDataType();
            Comparator<Record> comparator = recordComparator(primaryIndex, primaryType);
            // Add record
            this.records.add(record);
            // sort
            this.records.sort(comparator);

            this.setNumRecords();

            if (includeInsertedRecordInBucketUpdate) {
                updateBplusTreeBuckets(this.records.indexOf(record));
            } else {
                updateBplusTreeBuckets(this.records.indexOf(record) + 1);
            }
            this.changed = true;
            this.setPriority();
            return true;
        }
    }

    /**
     * Adds a record to the page at a specific index
     * @param record    The record to be inserted
     * @param index     The index to insert at
     * @return          true: insert success
     *                  false: page is full
     * @throws Exception
     */
    public boolean addNewRecord(Record record, int index) throws Exception {
        Catalog catalog = Catalog.getCatalog();
        // check if record can fit in this page.
        if ((catalog.getPageSize() - this.computeSize()) < record.computeSize()) {
            return false;
        } else {
            this.records.add(index, record);
            this.setNumRecords();
            updateBplusTreeBuckets(index + 1);
            this.changed = true;
            this.setPriority();
            return true;
        }
    }

    /**
     * Deletes a record at a specific index
     *
     * @param index     The index to delete the record at
     *
     * @return          The deleted record
     * @throws Exception
     */
    public Record deleteRecord(int index) throws Exception {
        Record removed = this.records.remove(index);
        this.changed = true;
        this.setPriority();
        this.setNumRecords();
        updateBplusTreeBuckets(index);
        return removed;
    }


    /**
     * returns the number of bytes of space is left in this page
     *
     * @return  int - number of bytes of space left
     * @throws Exception
     */
    public int computeSize() {
        // Page: numRecord, PageNumber, records..
        int size = Integer.BYTES * 2;
        for (Record record: this.records) {
            size += record.computeSize();
        }
        return size;
    }

    @Override
    public void writeToHardware(RandomAccessFile tableAccessFile) throws Exception {
        tableAccessFile.writeInt(this.numRecords);
        tableAccessFile.writeInt(this.pageNumber);
        for (Record record: this.records) {
           record.writeToHardware(tableAccessFile);
        }
    }

    @Override
    public void readFromHardware(RandomAccessFile tableAccessFile, TableSchema tableSchema) throws Exception {
        for (int i=0; i < numRecords; ++i) {
            Record record = new Record(new ArrayList<>());
            record.readFromHardware(tableAccessFile, tableSchema);
            this.records.add(record);
        }
    }


    private void updateBplusTreeBuckets(int startingRecordIndex) throws Exception {
        TableSchema tableSchema = Catalog.getCatalog().getSchema(tableNumber);
        int primaryIndex = tableSchema.getPrimaryIndex();
        for (int i=startingRecordIndex; i < this.records.size(); ++i) {
            Record record = this.records.get(i);
            Object primaryKey = record.getValues().get(primaryIndex);
            Bucket bucket = new Bucket(this.pageNumber, this.records.indexOf(record));
            Node root = StorageManager.getStorageManager().getNodePage(this.tableNumber, tableSchema.getRootNumber());
            BPlusTree bPlusTree = new BPlusTree(tableSchema, root);

            bPlusTree.update(primaryKey, primaryKey, bucket);
        }
    }

    /**
     * Creates a comparator to compare records based on the specified primary key index and data type.
     *
     * @param primaryKeyIndex The index of the primary key in each record.
     * @param dataType        The data type to determine the comparison method. Supported types are "integer", "double",
     *                        "boolean", "char(x)", and "varchar(x)" where x represents the length.
     * @return A comparator for comparing records based on the specified primary key index and data type.
     * @throws IllegalArgumentException If an unsupported data type is provided.
     */
    private Comparator<Record> recordComparator(int primaryKeyIndex, String dataType) {
        return (record1, record2) -> {
            Object obj1 = record1.getValues().get(primaryKeyIndex);
            Object obj2 = record2.getValues().get(primaryKeyIndex);

            if (dataType.equalsIgnoreCase("integer")) {
                Integer int1 = (Integer) obj1;
                Integer int2 = (Integer) obj2;
                return int1.compareTo(int2);
            } else if (dataType.equalsIgnoreCase("double")) {
                Double double1 = (Double) obj1;
                Double double2 = (Double) obj2;
                return double1.compareTo(double2);
            } else if (dataType.equalsIgnoreCase("boolean")) {
                Boolean bool1 = (Boolean) obj1;
                Boolean bool2 = (Boolean) obj2;
                return Boolean.compare(bool1, bool2);
            } else if (dataType.contains("char") || dataType.contains("varchar")) {
                String str1 = obj1.toString();
                String str2 = obj2.toString();
                return str1.compareTo(str2);
            } else {
                throw new IllegalArgumentException("Unsupported data type: " + dataType);
            }
        };
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        Page other = (Page) obj;
        if (pageNumber != other.pageNumber)
            return false;
        return true;
    }

}
