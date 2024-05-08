package Parser;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Queue;
import java.util.Stack;

import Parser.WhereTreeNodes.AndNode;
import Parser.WhereTreeNodes.AttributeNode;
import Parser.WhereTreeNodes.ComparisonOpNode;
import Parser.WhereTreeNodes.OrNode;
import Parser.WhereTreeNodes.ValueNode;
import Parser.WhereTreeNodes.WhereTree;
import Parser.WhereTreeNodes.Interfaces.OperandNode;
import Parser.WhereTreeNodes.Interfaces.OperatorNode;
import StorageManager.Objects.MessagePrinter;
import StorageManager.Objects.MessagePrinter.MessageType;

public class WhereTreeBuilder {
  Queue<Token> postfixExpression;
  WhereTree whereTree;

  public WhereTreeBuilder(Queue<Token> postfixExpression) {
    this.postfixExpression = postfixExpression;
  }

  public WhereTree buildWhereTree() throws Exception {
    Stack<Object> nodeStack = new Stack<>();

    ArrayList<String> operators = new ArrayList<>(Arrays.asList("and", "or"));
    ArrayList<String> comparisons = new ArrayList<>(Arrays.asList(">", ">=", "<", "<=", "=", "!="));

    while (!this.postfixExpression.isEmpty()) {
      Token token = this.postfixExpression.remove();
      String value = token.getVal().trim();

      if (operators.contains(value)) {
        // create comparison node
        if (nodeStack.size() < 2) {
          MessagePrinter.printMessage(MessageType.ERROR, "Invalid number of operator/operands, operator/operand mismatch.");
        }

        try {
          // attempt to cast the children after popping
          OperatorNode right = (OperatorNode) nodeStack.pop();
          OperatorNode left = (OperatorNode) nodeStack.pop();

          // push new node to stack
          if (value.equals("or")) {
            nodeStack.push(new OrNode(left, right));
          } else {
            nodeStack.push(new AndNode(left, right));
          }

        } catch (Exception e) {
          MessagePrinter.printMessage(MessageType.ERROR, "Invalid number of operator/operands, operator/operand mismatch.");
        }

      } else if (comparisons.contains(value)) {
        // create operator node
        if (nodeStack.size() < 2) {
          MessagePrinter.printMessage(MessageType.ERROR, "Invalid number of operator/operands, operator/operand mismatch.");
        }

        try {
          // attempt to cast the children after popping
          OperandNode right = (OperandNode) nodeStack.pop();
          OperandNode left = (OperandNode) nodeStack.pop();

          // push new ndoe to stack
          nodeStack.push(new ComparisonOpNode(left, right, value));

        } catch (Exception e) {
          MessagePrinter.printMessage(MessageType.ERROR, "Invalid number of operator/operands, operator/operand mismatch.");
        }

      } else {
        // operand node
        OperandNode operand = null;
        switch (token.getType()) {
          case NAME:
          case QUALIFIER:
            operand = new AttributeNode(value);
            break;
          case BOOLEAN:
            switch (value) {
              case "true":
                operand = new ValueNode(true);
                break;
              case "false":
                operand = new ValueNode(false);
                break;
              default:
              MessagePrinter.printMessage(MessageType.ERROR, "Invalid value for type boolean.");
                break;
            }
            break;
          case INTEGER:
            operand = new ValueNode(Integer.parseInt(value));
            break;
          case DOUBLE:
            operand = new ValueNode(Double.parseDouble(value));
            break;
          case STRING:
            operand = new ValueNode(value);
            break;
          default:
            MessagePrinter.printMessage(MessageType.ERROR, "Unknown data type found");
            break;
        }

        nodeStack.push(operand);
      }
    }

    if (nodeStack.size() != 1) {
      // mismatch of operands and operators occured
      MessagePrinter.printMessage(MessageType.ERROR, "Invalid number of operator/operands, operator/operand mismatch.");
    }

    // after this algorithm, the last node in the node stack is guarenteed
    // to be an operator, unless the statement itself has an incorrect syntax
    // TODO check for that syntax
    return new WhereTree((OperatorNode) nodeStack.pop());

  }

}
