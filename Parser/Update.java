package Parser;

import Parser.WhereTreeNodes.WhereTree;

public class Update {
  private String tableName;
  private String column;
  private Object value;
  private WhereTree whereTree;

  public Update(String tableName, String column, Object value, WhereTree whereTree) {
    this.tableName = tableName;
    this.column = column;
    this.value = value;
    this.whereTree = whereTree;
  }

  public String getTableName() {
    return tableName;
  }

  public String getColumn() {
    return column;
  }

  public Object getValue() {
    return value;
  }

  public WhereTree getWhereTree() {
    return whereTree;
  }
}
