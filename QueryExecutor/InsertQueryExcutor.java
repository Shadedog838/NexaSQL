package QueryExecutor;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import Parser.Insert;
import StorageManager.StorageManager;
import StorageManager.TableSchema;
import StorageManager.Objects.AttributeSchema;
import StorageManager.Objects.Catalog;
import StorageManager.Objects.MessagePrinter;
import StorageManager.Objects.Record;
import StorageManager.Objects.MessagePrinter.MessageType;

public class InsertQueryExcutor implements QueryExecutorInterface {

  private Insert insert;

  public InsertQueryExcutor(Insert insert) {
    this.insert = insert;
  }

  @Override
  public void excuteQuery() throws Exception {

    for (Record record : this.insert.getRecords()) {
      int tableNumber = validateRecord(record);
      StorageManager.getStorageManager().insertRecord(tableNumber, record);
    }

    MessagePrinter.printMessage(MessageType.SUCCESS, null);
  }

  public int validateRecord(Record record) throws Exception {
    Catalog catalog = Catalog.getCatalog();
    TableSchema tableSchema = catalog.getSchema(this.insert.getTableName());
    List<AttributeSchema> attributeSchemas = tableSchema.getAttributes();
    checkCorrectNumberOfValues(attributeSchemas, record);
    checkDataTypes(attributeSchemas, record);

    // determine index of the primary key
    int primaryKeyIndex = tableSchema.getPrimaryIndex();
    List<AttributeSchema> attrs = tableSchema.getAttributes();

    // check for unique primary key
    if (StorageManager.getStorageManager().getRecord(tableSchema.getTableNumber(),
        record.getValues().get(primaryKeyIndex)) != null) {
      MessagePrinter.printMessage(MessageType.ERROR, String.format("row (%s): Duplicate %s for row (%s)",
          printRow(record), "primary key", printRow(record)));
    }

    // check unique constraints

    List<Integer> uniqueAttributes = new ArrayList<>();
    for (int i = 0; i < attrs.size(); i++) {
      if (attrs.get(i).isUnique() && !attrs.get(i).isPrimaryKey()) {
        uniqueAttributes.add(i);
      }
    }

    // confirm unique
    if (uniqueAttributes.size() > 0) {
      this.checkUniqueContraints(uniqueAttributes, tableSchema.getTableNumber(), primaryKeyIndex, record);
    }

    return tableSchema.getTableNumber();

  }

  private void checkCorrectNumberOfValues(List<AttributeSchema> attributeSchemas, Record record) throws Exception {
    // check for correct number of values
    if (attributeSchemas.size() != record.getValues().size()) {
      StringBuilder expected = new StringBuilder();
      StringBuilder got = new StringBuilder();

      getGotAndExpected(attributeSchemas, record, got, expected);

      if (attributeSchemas.size() < record.getValues().size()) {
        MessagePrinter.printMessage(MessageType.ERROR,
            String.format("row (%s): Too many attributes: expected (%s) got (%s)", printRow(record), expected, got));
      } else {
        MessagePrinter.printMessage(MessageType.ERROR,
            String.format("row (%s): Too few attributes: expected (%s) got (%s)", printRow(record), expected, got));
      }

    }
  }

  private void checkDataTypes(List<AttributeSchema> attributeSchemas, Record record) throws Exception {

    for (int i = 0; i < attributeSchemas.size(); ++i) {
      if (!attributeSchemas.get(i).isNotNull()) {
        if (record.getValues().get(i) == null) {
          continue;
        }
      }

      String expectedDataType = attributeSchemas.get(i).getDataType();

      StringBuilder expected = new StringBuilder();
      StringBuilder got = new StringBuilder();

      if (expectedDataType.equals("integer")) {
        if (!(record.getValues().get(i) instanceof Integer)) {
          getGotAndExpected(attributeSchemas, record, got, expected);
          MessagePrinter.printMessage(MessageType.ERROR,
              String.format("row (%s): Invalid data type: expected (%s) got (%s).", printRow(record), expected, got));
        }
      } else if (expectedDataType.equals("double")) {
        if (!(record.getValues().get(i) instanceof Double)) {
          getGotAndExpected(attributeSchemas, record, got, expected);
          MessagePrinter.printMessage(MessageType.ERROR,
              String.format("row (%s): Invalid data type: expected (%s) got (%s).", printRow(record), expected, got));
        }
      } else if (expectedDataType.equals("boolean")) {
        if (!(record.getValues().get(i) instanceof Boolean)) {
          getGotAndExpected(attributeSchemas, record, got, expected);
          MessagePrinter.printMessage(MessageType.ERROR,
              String.format("row (%s): Invalid data type: expected (%s) got (%s).", printRow(record), expected, got));
        }
      } else if (expectedDataType.contains("char") || expectedDataType.contains("varchar")) {
        if (!(record.getValues().get(i) instanceof String)) {
          getGotAndExpected(attributeSchemas, record, got, expected);
          MessagePrinter.printMessage(MessageType.ERROR,
              String.format("row (%s): Invalid data type: expected (%s) got (%s).", printRow(record), expected, got));
        } else {
          checkSizeOfStrings(((String) record.getValues().get(i)), expectedDataType, record);
        }
      } else {
        MessagePrinter.printMessage(MessageType.ERROR,
            String.format("row (%s): Invalid data type: expected (%s) got (%s).", printRow(record), expected, got));
      }
    }
  }

  private void checkSizeOfStrings(String value, String dataType, Record record) throws Exception {
    Pattern pattern = Pattern.compile("\\((\\d+)\\)");
    Matcher matcher = pattern.matcher(dataType);
    int size = 0;
    while (matcher.find()) {
      size = Integer.parseInt(matcher.group(1));
    }

    if (dataType.contains("varchar")) {
      if (value.length() > size) {
        MessagePrinter.printMessage(MessageType.ERROR, String.format(
            "row (%s): %s can only accept %d chars or less; \"%s\" is %d", printRow(record), dataType, size, value,
            value.length()));
      }
    } else {
      if (value.length() != size) {
        MessagePrinter.printMessage(MessageType.ERROR,
            String.format("row (%s): %s can only accept %d chars; \"%s\" is %d", printRow(record), dataType, size,
                value, value.length()));
      }
    }
  }

  private void checkUniqueContraints(List<Integer> uniqueAttributeIndexes, int tableNumber, int primaryKeyIndex,
      Record newRecord) throws Exception {
    List<Record> records = StorageManager.getStorageManager().getAllRecords(tableNumber);
    for (Record record : records) {
      for (Integer attributeIndex : uniqueAttributeIndexes) {
        if (newRecord.compareTo(record, attributeIndex) == 0) {
          MessagePrinter.printMessage(MessageType.ERROR,
              String.format("row (%s): Duplicate %s for row (%s)", printRow(record), "value", printRow(record)));
        }
      }
    }
  }

  private String printRow(Record record) {
    StringBuilder row = new StringBuilder();
    Boolean addSpace = false;
    for (Object value : record.getValues()) {
      if (addSpace) {
        row.append(" ");
      }

      if (!(value == null)) {
        row.append(value.toString());
      } else {
        row.append("null");
      }
      addSpace = true;
    }
    return row.toString();
  }

  private void getGotAndExpected(List<AttributeSchema> attributeSchemas, Record record, StringBuilder got,
      StringBuilder expected) {
    boolean addSpace = false;
    for (AttributeSchema attributeSchema : attributeSchemas) {
      if (addSpace)
        expected.append(" ");
      expected.append(attributeSchema.getDataType());
      addSpace = true;
    }

    addSpace = false;
    for (Object value : record.getValues()) {
      if (addSpace)
        got.append(" ");
      String valueDataType = getDataType(value, expected.toString().contains("varchar") ? "varchar" : "char");
      got.append(valueDataType);
      addSpace = true;
    }
  }

  public static String getDataType(Object object, String dataTypeForString) {
    if (object instanceof Integer) {
      return "integer";
    } else if (object instanceof Double) {
      return "double";
    } else if (object instanceof Boolean) {
      return "boolean";
    } else if (object instanceof String) {
      if (dataTypeForString.contains("varchar")) {
        return "varchar(" + ((String) object).length() + ")";
      } else {
        return "char(" + ((String) object).length() + ")";
      }
    }
    return "null";

  }

}
