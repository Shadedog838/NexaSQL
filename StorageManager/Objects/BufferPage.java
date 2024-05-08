package StorageManager.Objects;

import java.io.RandomAccessFile;
import java.util.Comparator;

import StorageManager.TableSchema;

public abstract class BufferPage implements Comparator<BufferPage> {
    protected long priority;
    protected boolean changed;
    protected int tableNumber;
    protected int pageNumber;

    public BufferPage(int tableNumber, int pageNumber) {
        this.tableNumber = tableNumber;
        this.pageNumber = pageNumber;
        this.setPriority();
    }

    /**
     * Returns whether or not this page has been changed
     *
     * @return  bool
     */
    public boolean isChanged() {
        return this.changed;
    }

    /**
     * Sets the value that indicates that this page has been changed
     */
    public void setChanged() {
        this.changed = true;
    }

    /**
     * Sets the priority of this page
     * This is used in ordering the buffer for LRU
     */
    public void setPriority() {
        this.priority = System.currentTimeMillis();
    }

    /**
     * Gets the table number that this page is associated with
     *
     * @return  The associated table number
     */
    public int getTableNumber() {
        return this.tableNumber;
    }

    public int getPageNumber() {
        return this.pageNumber;
    }

    public void decrementPageNumber() {
        this.pageNumber--;
        this.setChanged();
    }

    public void setPageNumber(int n) {
        this.pageNumber = n;
        this.setChanged();
    }

    /**
     * Compare function used for the priority queue
     * of the buffer to determine which page is LRU
     */
    @Override
    public int compare(BufferPage o1, BufferPage o2) {
        if (o1.priority > o2.priority) {
            return 1;
        } else if (o1.priority == o2.priority) {
            return 0;
        } else {
            return -1;
        }
    }

    public abstract void readFromHardware(RandomAccessFile tableAccessFile, TableSchema tableSchema) throws Exception;

    public abstract void writeToHardware(RandomAccessFile tableAccessFile) throws Exception;

}
