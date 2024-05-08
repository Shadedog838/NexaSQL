package StorageManager.Objects;

import java.io.IOException;
import java.io.RandomAccessFile;

public class AttributeSchema implements java.io.Serializable, SchemaInterface {
    private String attributeName;
    private String dataType;
    private boolean notNull;
    private boolean primaryKey;
    private boolean unique;

    public AttributeSchema(String attributeName, String dataType, boolean notNull, boolean primaryKey, boolean unique) {
        this.attributeName = attributeName;
        this.dataType = dataType;
        this.notNull = notNull;
        this.primaryKey = primaryKey;
        this.unique = unique;
    }

    public AttributeSchema(String attributeName, AttributeSchema oldSchema) {
        this.attributeName = attributeName;
        this.dataType = oldSchema.getDataType();
        this.notNull = isNotNull();
        this.primaryKey = oldSchema.isPrimaryKey();
        this.unique = oldSchema.isUnique();
    }


    public AttributeSchema(){
    }

    public String getAttributeName() {
        return attributeName;
    }

    public void setAttributeName(String attributeName) {
        this.attributeName = attributeName;
    }

    public String getDataType() {
        return dataType;
    }

    public void setDataType(String dataType) {
        this.dataType = dataType;
    }

    public boolean isNotNull() {
        return notNull;
    }

    public void setNotNull(boolean notNull) {
        this.notNull = notNull;
    }

    public boolean isPrimaryKey() {
        return primaryKey;
    }

    public void setPrimaryKey(boolean primaryKey) {
        this.primaryKey = primaryKey;
    }

    public boolean isUnique() {
        return unique;
    }

    public void setUnique(boolean unique) {
        this.unique = unique;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void saveSchema(RandomAccessFile catalogAccessFile) throws IOException {
        // Write attribute name to the catalog file as UTF string
        catalogAccessFile.writeUTF(this.attributeName);

        // Write data type to the catalog file as UTF string
        catalogAccessFile.writeUTF(this.dataType);

        // Write whether the attribute is not null to the catalog file
        catalogAccessFile.writeBoolean(this.notNull);

        // Write whether the attribute is a primary key to the catalog file
        catalogAccessFile.writeBoolean(this.primaryKey);

        // Write whether the attribute is unique to the catalog file
        catalogAccessFile.writeBoolean(this.unique);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void loadSchema(RandomAccessFile catalogAccessFile) throws Exception {
        // Read attribute Name
        this.attributeName = catalogAccessFile.readUTF();

        // Read datatype
        this.dataType = catalogAccessFile.readUTF();

        // Read whether the attribute is not null from the catalog file
        this.notNull = catalogAccessFile.readBoolean();

        // Read whether the attribute is a primary key from the catalog file
        this.primaryKey = catalogAccessFile.readBoolean();

        // Read whether the attribute is unique from the catalog file
        this.unique = catalogAccessFile.readBoolean();
    }
}
