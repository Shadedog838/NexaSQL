package Parser.WhereTreeNodes;

import Parser.WhereTreeNodes.Interfaces.OperandNode;
import StorageManager.TableSchema;
import StorageManager.Objects.Record;

/*
* A literal value node, this should be a leaf
*/
public class ValueNode implements OperandNode {

    private Object value;

    public ValueNode(Object value) {
        this.value = value;
    }

    @Override
    public Object getValue(TableSchema schema, Record record) {
        return this.value;
    }

}
