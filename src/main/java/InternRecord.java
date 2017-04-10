
public class InternRecord {

    private int id;
    private String name;

    public InternRecord(int id, String name) {
        this.id = id;
        this.name = name;
    }

    @Override public String toString() {
        return "( " + this.id + "," + this.name + " )";
    }
}