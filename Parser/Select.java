package Parser;

import java.util.List;

import Parser.WhereTreeNodes.WhereTree;

public class Select {
  List<String> attrubuteNames;
  List<String> tableNames;
  WhereTree whereTree;
  String orderByAttribute;

  public Select(List<String> attrubuteNames, List<String> tableNames, WhereTree whereTree,
      String orderByAttribute) {
    this.attrubuteNames = attrubuteNames;
    this.tableNames = tableNames;
    this.whereTree = whereTree;
    this.orderByAttribute = orderByAttribute;
  }

  public List<String> getAttributeNames() {
    return attrubuteNames;
  }

  public List<String> getTableNames() {
    return tableNames;
  }

  public WhereTree getWhereTree() {
    return whereTree;
  }

  public String getOrderByAttribute() {
    return orderByAttribute;
  }


}
