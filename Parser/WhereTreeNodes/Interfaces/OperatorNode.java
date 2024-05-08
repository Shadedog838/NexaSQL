package Parser.WhereTreeNodes.Interfaces;

import StorageManager.TableSchema;
import StorageManager.Objects.Record;

/*
 * Used for all comparison operators: +, -, *, /, and, or
 */
public interface OperatorNode {

    /**
     * determines whether or not a record is valid for this where tree
     * @param schema        The table schema that the record abides by
     * @param record        The record in question
     * @return              A boolean to signify validity
     * @throws Exception
     */
    public boolean evaluate(TableSchema schema, Record record) throws Exception;

}
