package Parser;

import Parser.WhereTreeNodes.WhereTree;

public class Delete {
  String tableName;
  WhereTree whereTree;

  public Delete(String tableName, WhereTree whereTree) {
    this.tableName = tableName;
    this.whereTree = whereTree;
  }

  public String getTableName() {
    return tableName;
  }

  public WhereTree getWhereTree() {
    return whereTree;
  }
}
