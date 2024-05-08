package Parser.WhereTreeNodes;

import Parser.WhereTreeNodes.Interfaces.OperatorNode;
import StorageManager.TableSchema;
import StorageManager.Objects.Record;

public class WhereTree {
  private OperatorNode root;
  private Boolean empty;


  public WhereTree(OperatorNode root) {
    this.root = root;
    this.empty = false;
  }

  public WhereTree() {
    // if this is true then everything in the update should return true
    // meaning, update EVERY record
    this.empty = true;
  }

  public boolean evaluate(TableSchema tableSchema, Record record) throws Exception {
    if (!this.empty) {
      return root.evaluate(tableSchema, record);
    } else {
      return true;
    }
  }


}
