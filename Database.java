import java.io.File;
import StorageManager.StorageManager;
import StorageManager.Objects.Catalog;
import StorageManager.Objects.MessagePrinter;
import StorageManager.Objects.MessagePrinter.MessageType;

public class Database {
    private UserInterface userInterface;
    private String dbLocation;
    private int pageSize;
    private int bufferSize;
    private boolean indexing;

    public Database(String dbLocation, int pageSize, int bufferSize, boolean indexing) {
        this.bufferSize = bufferSize;
        this.pageSize = pageSize;
        this.dbLocation = dbLocation;
        this.indexing = indexing;
    }

    public void start() throws Exception {
        System.out.println("Welcome to CASE-C QL");
        System.out.println("Looking at " + dbLocation);
        File dbDirectory = new File(dbLocation);
        File schemaFile = new File(dbDirectory.getAbsolutePath().concat("/schema"));
        if (!dbDirectory.exists()) {
            MessagePrinter.printMessage(MessageType.ERROR, "Failed to find database at " + dbDirectory.getAbsolutePath());
        }
        if (schemaFile.exists()) {
            System.out.println("Database found..." + "\n" +
            "Restarting the database...");
            if (schemaFile.length() == 0) {
                Catalog.createCatalog(dbDirectory.getAbsolutePath(), schemaFile.getAbsolutePath(), pageSize, bufferSize, indexing);
            } else {
                System.out.println("\tIgnoring provided pages size, using stored page size");
                Catalog.createCatalog(dbDirectory.getAbsolutePath(), schemaFile.getAbsolutePath(), -1, bufferSize, true);
            }
            StorageManager.createStorageManager(bufferSize);
            System.out.println("Page Size: " + Catalog.getCatalog().getPageSize());
            System.out.println("Buffer Size: " + bufferSize + "\n");
            if (Catalog.getCatalog().isIndexingOn()) {
                System.out.println("Indexing: On");
            } else {
                System.out.println("Indexing: Off");
            }
            System.out.println("Database restarted successfully");
        } else {
            System.out.println("Creating new db at " + dbDirectory.getAbsolutePath());
            File tableDirectory = new File(dbDirectory.getAbsolutePath().concat("/tables"));
            File indexDirectory = new File(dbDirectory.getAbsolutePath().concat("/indexing"));
            boolean success = tableDirectory.mkdir() && schemaFile.createNewFile() && indexDirectory.mkdir();
            if (success){
                StorageManager.createStorageManager(bufferSize);
                Catalog.createCatalog(dbDirectory.getAbsolutePath(), schemaFile.getAbsolutePath(), pageSize, bufferSize, indexing);
                System.out.println("New db created successfully");
                System.out.println("Page Size: " + pageSize);
                System.out.println("Buffer Size: " + bufferSize);
                if (this.indexing) {
                    System.out.println("Indexing: On");
                } else {
                    System.out.println("Indexing: Off");
                }
            } else {
                MessagePrinter.printMessage(MessageType.ERROR, "Unable to successfully create the database.");
            }
        }
        userInterface = new UserInterface();
        userInterface.start();
        shutdown();
    }

    private void shutdown() {
        Catalog catalog = Catalog.getCatalog();
        StorageManager storageManager = StorageManager.getStorageManager();
        try {
            System.out.println("Safely shutting down the database...\r\n" +
                                "Purging page buffer...");
            storageManager.writeAll();
            System.out.println("Saving catalog...\n");
            catalog.saveCatalog();
        } catch (Exception e) {
            System.out.println("Database Failed to shut down successfully");
        }
        System.out.println("Exiting the database...");
    }

}
