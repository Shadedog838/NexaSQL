package Parser.WhereTreeNodes;

import java.util.ArrayList;
import java.util.List;

import Parser.WhereTreeNodes.Interfaces.OperandNode;
import StorageManager.TableSchema;
import StorageManager.Objects.AttributeSchema;
import StorageManager.Objects.MessagePrinter;
import StorageManager.Objects.Record;
import StorageManager.Objects.MessagePrinter.MessageType;

/*
* Node that holds an attribute name, this should be a leaf
*/
public class AttributeNode implements OperandNode {

    String dataName;

    public AttributeNode(String name) {
        this.dataName = name;
    }

    @Override
    public Object getValue(TableSchema schema, Record record) throws Exception {
        List<AttributeSchema> attrs = schema.getAttributes();
        List<Object> foundVal = new ArrayList<>();

        for (int i = 0; i < attrs.size(); i++)  {
            String[] spList = attrs.get(i).getAttributeName().split("\\.");
            List<String> potentialMatches = new ArrayList<>();
            potentialMatches.add(attrs.get(i).getAttributeName().toLowerCase());
            if (spList.length > 1) {
                potentialMatches.add(spList[1]);
            } else {
                potentialMatches.add(schema.getTableName() + "." + attrs.get(i).getAttributeName());
            }

            if (potentialMatches.contains(dataName)) {
                foundVal.add(record.getValues().get(i));
            }
        }

        if (foundVal.size() < 1) {
            MessagePrinter.printMessage(MessageType.ERROR, "Invalid attribute name: " + this.dataName + ".");
        } else if (foundVal.size() > 1) {
            MessagePrinter.printMessage(MessageType.ERROR, String.format("%s is ambigious", this.dataName));
        } else {
            return foundVal.get(0);
        }
        return null;

    }

}
