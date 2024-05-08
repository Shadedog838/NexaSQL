public class Main {
  public static void main(String[] args) {
    if (args.length != 4) {
      System.err.println("java Main <db_loc> <page_size> <buffer_size> <indexing>");
      System.exit(0);
    } else {
      String dbLocation = args[0];
      int pageSize =  Integer.parseInt(args[1]);
      int bufferSize = Integer.parseInt(args[2]);
      boolean indexing = Boolean.parseBoolean(args[3]);
      Database database = new Database(dbLocation, pageSize, bufferSize, indexing);
      try {
        database.start();
      } catch (Exception e) {
        System.err.println("Failed to start database");
        if (e.getMessage() != null) {
          System.err.println(e.getMessage());
        }
        System.exit(0);
      }
    }
  }
}