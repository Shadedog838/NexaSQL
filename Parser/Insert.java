package Parser;

import java.util.List;

import StorageManager.Objects.Record;

public class Insert {
  private String tableName;
  private List<Record> records;
  private String query;


  public Insert(String tableName, List<Record> records) {
    this.tableName = tableName;
    this.records = records;
    this.query = "";
  }


  public String getTableName() {
    return tableName;
  }


  public List<Record> getRecords() {
    return records;
  }


  public String getQuery() {
    return query;
  }


  public void setQuery(String query) {
    this.query = query;
  }

}
