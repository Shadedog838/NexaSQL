package StorageManager.Objects;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

import StorageManager.StorageManagerObjectIntereface;
import StorageManager.TableSchema;

public class Record implements java.io.Serializable, StorageManagerObjectIntereface {
    private List<Object> values;

    public Record(List<Object> values) {
        this.values = values;
    }

    public Record(Record a, Record b){
        this.values = new ArrayList<>();
        this.values.addAll(a.values);
        this.values.addAll(b.values);
        //System.out.println("yesyt");
    }

    public List<Object> getValues() {
        return values;
    }

    public void setValues(List<Object> values) {
        this.values = values;
    }

    public void addValue(Object val) {
        this.values.add(val);
    }

    /**
     * Compares two values to one another
     * PreReq: The inputted record as well as this record, are from the same table
     *
     * @param: other    The record to compare
     * @param: keyIndex The index of the column to compare
     *
     * @return:     0: The two are equal
     *             <0: this < other
     *             >0: this > other
     */
    public int compareTo(Record other, int keyIndex) {
        Object thisKey = this.values.get(keyIndex);
        Object otherKey = other.values.get(keyIndex);

        if (thisKey instanceof String) {
            return ((String) thisKey).compareTo((String) otherKey);
        } else if (thisKey instanceof Integer) {
            return Integer.compare((Integer) thisKey, (Integer) otherKey);
        } else if (thisKey instanceof Boolean) {
            boolean thisBool = (Boolean) thisKey;
            boolean otherBool = (Boolean) otherKey;
            return Boolean.compare(otherBool, thisBool);
        } else if (thisKey instanceof Double) {
            return Double.compare((Double) thisKey, (Double) otherKey);
        } else {
            throw new IllegalArgumentException("Unsupported datatype");
        }
    }

    /**
     * Compares two values to one another
     * PreReq: The inputted object as well as the object at the index given,
     * must be of the same dataType
     *
     * @param: other    The value to compare
     * @param: keyIndex The index of the column of this record to compare to
     *
     * @return:     0: The two are equal
     *             <0: this < other
     *             >0: this > other
     */
    public int compareTo(Object otherKey, int keyIndex) {
        Object thisKey = this.values.get(keyIndex);

        if (thisKey instanceof String) {
            return ((String) thisKey).compareTo((String) otherKey);
        } else if (thisKey instanceof Integer) {
            return Integer.compare((Integer) thisKey, (Integer) otherKey);
        } else if (thisKey instanceof Boolean) {
            boolean thisBool = (Boolean) thisKey;
            boolean otherBool = (Boolean) otherKey;
            return Boolean.compare(thisBool, otherBool);
        } else if (thisKey instanceof Double) {
            return Double.compare((Double) thisKey, (Double) otherKey);
        } else {
            throw new IllegalArgumentException("Unsupported datatype");
        }
    }

    /**
     * Returns the size of this record in number of bytes
     *
     * @return  the number of bytes this record is
     * @throws Exception
     */
    @Override
    public int computeSize() {
        int size = 0;
        for (Object value: this.values) {
            if (value instanceof Integer) {
                size += Integer.BYTES;
            } else if (value instanceof String) {
                size += ((String) value).length() + Short.BYTES;
            } else if (value instanceof Boolean) {
                size += 1;
            } else if (value instanceof Double) {
                size += Double.BYTES;
            } else {
                size += 1;
            }
        }
        return size;
    }

    @Override
    public void writeToHardware(RandomAccessFile tableAccessFile) throws IOException {
        for (Object value : this.values) {
            if (value instanceof Integer) {
                tableAccessFile.writeInt((Integer) value);
            } else if (value instanceof String) {
                tableAccessFile.writeUTF((String) value);
            } else if (value instanceof Double) {
                tableAccessFile.writeDouble((Double) value);
            } else if (value instanceof Boolean) {
                tableAccessFile.writeBoolean((Boolean) value);
            } else {
                // null value
                tableAccessFile.writeByte(-1);
            }
        }
    }

    @Override
    public void readFromHardware(RandomAccessFile tableAccessFile, TableSchema tableSchema) throws IOException {
        for (AttributeSchema attributeSchema : tableSchema.getAttributes()) {
            if (!attributeSchema.isNotNull()) {
                // check for null value
                byte nullMarker = tableAccessFile.readByte();

                if (nullMarker == -1) {
                    this.values.add(null);
                    continue;
                } else {
                    tableAccessFile.seek(tableAccessFile.getFilePointer() - 1);
                }

            }
            if (attributeSchema.getDataType().equalsIgnoreCase("integer")) {
                int value = tableAccessFile.readInt();
                this.values.add(value);
            } else if (attributeSchema.getDataType().equalsIgnoreCase("double")) {
                double value = tableAccessFile.readDouble();
                this.values.add(value);
            } else if (attributeSchema.getDataType().equalsIgnoreCase("boolean")) {
                boolean value = tableAccessFile.readBoolean();
                this.values.add(value);
            } else if (attributeSchema.getDataType().contains("char") || attributeSchema.getDataType().contains("varchar")) {
                String value = tableAccessFile.readUTF();
                this.values.add(value);
            }
        }
    }
}
