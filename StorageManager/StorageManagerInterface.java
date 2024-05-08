package StorageManager;

import java.util.List;

import BPlusTree.BPlusNode;
import StorageManager.Objects.Page;
import StorageManager.Objects.Record;

public interface StorageManagerInterface {

    /**
     * Gets a single record from a table
     *
     * @param tableNumber   the table to search in
     * @param primaryKey    the key associated with the record
     *                      we are looking for
     *
     * @return              the record that was found, otherwise null
     */
    public Record getRecord(int tableNumber, Object primaryKey) throws Exception;

    /**
     * Gets a single record from a table
     *
     * @param tableName     the table to search in
     * @param primaryKey    the key associated with the record
     *                      we are looking for
     *
     * @return              the record that was found, otherwise null
     */
    public Record getRecord(String tableName, Object primaryKey) throws Exception;

    /**
     * Gets all records from a table
     *
     * @param tableNumber   the table to search in
     *
     * @return              a list of all records in the table
     */
    public List<Record> getAllRecords(int tableNumber) throws Exception;

    /**
     * Gets all records from a table
     *
     * @param tableName     the table to search in
     *
     * @return              a list of all records in the table
     */
    public List<Record> getAllRecords(String tableName) throws Exception;

    /**
     * Inserts a record into a specified table
     * Process:
     *      Read the page in which the record will be inserted into
     *      Add record to the page, if page is full, page split
     *      Leave the page in the buffer, the page will be written
     *      to disk when the page leaves the buffer.
     *
     * @param tableNumber   the id associated with the table to insert into
     * @param record        the record to insert
     *
     */
    public void insertRecord(int tableNumber, Record record) throws Exception;

    /**
     * Deletes a record from the DB
     * Given a primary key's value, read in pages from the given table
     * and find the record with the associated primary key,
     * then delete that record and reconstruct the page
     *
     * @param tableNumber   the table to delete from
     * @param primaryKey    the value of the primaryKey to search for
     */
    public Record deleteRecord(int tableNumber, Object primaryKey) throws Exception;

    /**
     * Update a record in the DB
     * Given a primary key of a record, search for the record
     * Then update the founded record with the inputted record
     * Similar to a search and replace
     *
     * If the inputted record does not have ALL attributes filled in
     * then only replace the attributes that were changed
     *
     * @param tableNumebr   the table to find the record in
     * @param primaryKey    value fo the primaryKey to search for
     * @param record        a record with the updated values
     */
    public void updateRecord(int tableNumber, Record newRecord, Object primaryKey) throws Exception;

    public Page getPage(int tableNumber, int pageNumber) throws Exception;
    public BPlusNode getIndexPage(int tableNumber, int pageNumber) throws Exception;

    /**
     * Writes all pages in the buffer to disk
     * This is used upon system shut off
     */
    public void writeAll() throws Exception;
}
