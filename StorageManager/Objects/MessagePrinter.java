package StorageManager.Objects;

public class MessagePrinter{
  // Enum to represent message types
  public enum MessageType {
      SUCCESS,
      ERROR
  }

  // Method to print messages
  public static void printMessage(MessageType messageType, String message) throws Exception {
      switch (messageType) {
          case SUCCESS:
              System.out.println(MessageType.SUCCESS + "\n");
              break;
          case ERROR:
              throw new Exception(message + "\n" + MessageType.ERROR + "\n");
          default:
              throw new Exception("Unsupported message type: " + messageType);
      }
  }
}
