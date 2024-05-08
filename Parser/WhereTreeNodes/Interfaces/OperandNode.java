package Parser.WhereTreeNodes.Interfaces;

import StorageManager.TableSchema;
import StorageManager.Objects.Record;

/*
 * Interface for the leaf nodes
 */
public interface OperandNode {

    /**
     * Returns the value of the leaf node
     * If the leaf node is a literal, then return the literal
     * If the leaf node is a column name, retrieve the value from the passed in record
     * data types will be validated in the operator node 
     * @param schema        The schema that the record abides by
     * @param record        The record in question
     * @return              The value of the node/attribute
     * @throws Exception
     */
    public Object getValue(TableSchema schema, Record record) throws Exception;

}
