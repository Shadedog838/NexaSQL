package Parser.WhereTreeNodes;

import Parser.WhereTreeNodes.Interfaces.OperandNode;
import Parser.WhereTreeNodes.Interfaces.OperatorNode;
import StorageManager.TableSchema;
import StorageManager.Objects.MessagePrinter;
import StorageManager.Objects.MessagePrinter.MessageType;

/*
* Node that holds what operation is to be performed (=, <=, <, >, >=, !=)
* This will always be the root node
*/
public class ComparisonOpNode implements OperatorNode {

    OperandNode rigthChild;
    OperandNode leftChild;
    String operator;

    public ComparisonOpNode(OperandNode left, OperandNode right, String operator) {
        this.leftChild = left;
        this.rigthChild = right;
        this.operator = operator;
    }

    @Override
    public boolean evaluate(TableSchema schema, StorageManager.Objects.Record record) throws Exception {
        Object right = this.rigthChild.getValue(schema, record);
        Object left = this.leftChild.getValue(schema, record);

        if (right == null || left == null) {
            if (this.operator.equals("=")) {
                return right == left;
            } else if (this.operator.equals("!=")) {
                return right != left;
            }
            return false;
        }

        if (left instanceof String) {
            if (!(right instanceof String)) {
                MessagePrinter.printMessage(MessageType.ERROR, "Mismatch data type in where comparison. Expected right side: String, Got: " + right);
            }
            return this.compareChildren((String) left, (String) right);
        } else if (left instanceof Integer) {
            if (!(right instanceof Integer)) {
                MessagePrinter.printMessage(MessageType.ERROR, "Mismatch data type in where comparison. Expected right side: Integer, Got: " + right);
            }
            return this.compareChildren((Integer) left, (Integer) right);
        } else if (left instanceof Boolean) {
            if (!(right instanceof Boolean)) {
                MessagePrinter.printMessage(MessageType.ERROR, "Mismatch data type in where comparison. Expected right side: Boolean, Got: " + right);
            }
            boolean leftBool = (Boolean) left;
            boolean rightBool = (Boolean) right;
            return this.compareChildren(leftBool, rightBool);
        } else if (left instanceof Double) {
            if (!(right instanceof Double)) {
                MessagePrinter.printMessage(MessageType.ERROR, "Mismatch data type in where comparison. Expected right side: Double, Got: " + right);
            }
            return this.compareChildren((Double) left, (Double) right);
        } else {
            MessagePrinter.printMessage(MessageType.ERROR, "Unsupported datatype.");
            return false;
        }

    }

    /**
     * Takes in two parameters of any type and compares them based on some comparison
     * @param <T>
     * @param left      The left operand
     * @param right     The right operand
     * @return          A boolean
     * @throws Exception
     */
    private <T extends Comparable<T>> boolean compareChildren (T left, T right) throws Exception {
        switch (this.operator) {
            case "=":
                return left.equals(right);
            case "!=":
                return !(left.equals(right));
            case ">":
                return left.compareTo(right) > 0;
            case "<":
                return left.compareTo(right) < 0;
            case ">=":
                return left.compareTo(right) >= 0;
            case "<=":
                return left.compareTo(right) <= 0;
            default:
                MessagePrinter.printMessage(MessageType.ERROR, "Unsupported comparison operator: " + this.operator + ".");
                break;
        }
        return false;

    }

}
