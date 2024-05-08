package StorageManager;

import java.io.RandomAccessFile;

public interface StorageManagerObjectIntereface {

  public void writeToHardware(RandomAccessFile tableAccessFile) throws Exception;

  public void readFromHardware(RandomAccessFile tableAccessFile, TableSchema tableSchema) throws Exception;

  public int computeSize();

}
