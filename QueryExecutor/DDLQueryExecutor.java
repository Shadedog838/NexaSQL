package QueryExecutor;

import StorageManager.TableSchema;
import StorageManager.Objects.Catalog;
import StorageManager.Objects.MessagePrinter;
import StorageManager.Objects.MessagePrinter.MessageType;

public class DDLQueryExecutor implements QueryExecutorInterface{

  private String name;
  private String action;
  private TableSchema tableSchema;
  private String defaultValue; // alter use
  private String attributeName; //alter use
  private String dataType; // alter use
  private String attributeAction;

  private String isDeflt;


  public DDLQueryExecutor(String name, String action, String defaultValue, String attributeName, String dataType,
      String attributeAction, String isDeflt) {
    this.name = name;
    this.action = action;
    this.defaultValue = defaultValue;
    this.attributeName = attributeName;
    this.dataType = dataType;
    this.attributeAction = attributeAction;
    this.isDeflt = isDeflt;
  }

  public DDLQueryExecutor(String name, String action) {
    this.name = name;
    this.action = action;
  }

  public DDLQueryExecutor(String action, TableSchema tableSchema) {
    this.action = action;
    this.tableSchema = tableSchema;
  }





  @Override
  public void excuteQuery() throws Exception {
    // validate and execute
    Catalog catalog = Catalog.getCatalog();
    switch (this.action) {
      case "create":
        catalog.createTable(this.tableSchema);
        MessagePrinter.printMessage(MessageType.SUCCESS, null);
        break;
      case "drop":
        TableSchema tableSchema = catalog.getSchema(this.name);
        catalog.dropTableSchema(tableSchema.getTableNumber());
        MessagePrinter.printMessage(MessageType.SUCCESS, null);
        break;
      case "alter":
        tableSchema = catalog.getSchema(this.name);
        catalog.alterTableSchema(tableSchema.getTableNumber(), this.attributeAction,this.attributeName,this.dataType,
                this.defaultValue,this.isDeflt,false,false,false);
        MessagePrinter.printMessage(MessageType.SUCCESS, null);
      default:
        break;
    }
  }

}
