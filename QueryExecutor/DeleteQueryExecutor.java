package QueryExecutor;

import java.lang.management.MemoryType;
import java.util.ArrayList;
import java.util.List;

import Parser.Delete;
import StorageManager.StorageManager;
import StorageManager.TableSchema;
import StorageManager.Objects.Catalog;
import StorageManager.Objects.MessagePrinter;
import StorageManager.Objects.Record;
import StorageManager.Objects.MessagePrinter.MessageType;

public class DeleteQueryExecutor implements QueryExecutorInterface{
  private Delete delete;
  private List<Object> primaryKeys;
  private TableSchema tableSchema;

  public DeleteQueryExecutor(Delete delete) {
    this.delete = delete;
    this.primaryKeys = new ArrayList<>();
  }

  @Override
  public void excuteQuery() throws Exception {
    validateQuery();
    StorageManager storageManager = StorageManager.getStorageManager();
    for (Object primaryKey: primaryKeys) {
      try {
        storageManager.deleteRecord(tableSchema.getTableNumber(), primaryKey);
      } catch (Exception e) {
        System.err.println(e.getMessage());
        MessagePrinter.printMessage(MessageType.ERROR, "Error found in delete. Stopping delete...");
      }
    }
    MessagePrinter.printMessage(MessageType.SUCCESS, null);
  }

  private void validateQuery() throws Exception {
    Catalog catalog = Catalog.getCatalog();
    tableSchema = catalog.getSchema(this.delete.getTableName());

    if (this.delete.getWhereTree() != null) {
      List<Record> records = StorageManager.getStorageManager().getAllRecords(tableSchema.getTableNumber());
      int primaryKeyIndex = tableSchema.getPrimaryIndex();
      for (Record record : records) {
        if (this.delete.getWhereTree().evaluate(tableSchema, record)) {
          this.primaryKeys.add(record.getValues().get(primaryKeyIndex));
        }
      }
    }
  }
}
