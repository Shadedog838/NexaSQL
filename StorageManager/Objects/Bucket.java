package StorageManager.Objects;

public class Bucket {
  private int pageNumber;
  private int index;

  public Bucket(int pageNumber, int index) {
      this.pageNumber = pageNumber;
      this.index = index;
  }

  public int getPageNumber() {
      return pageNumber;
  }

  public int getIndex() {
      return index;
  }
}

