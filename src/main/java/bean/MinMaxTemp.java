package bean;

public class MinMaxTemp {
    private String id;
    private double min;
    private double max;
    private long startTs;
    private long endTs;

    public MinMaxTemp() {
    }

    public MinMaxTemp(String id, double min, double max, long startTs, long endTs) {
        this.id = id;
        this.min = min;
        this.max = max;
        this.startTs = startTs;
        this.endTs = endTs;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public double getMin() {
        return min;
    }

    public void setMin(double min) {
        this.min = min;
    }

    public double getMax() {
        return max;
    }

    public void setMax(double max) {
        this.max = max;
    }

    public long getStartTs() {
        return startTs;
    }

    public void setStartTs(long startTs) {
        this.startTs = startTs;
    }

    public long getEndTs() {
        return endTs;
    }

    public void setEndTs(long endTs) {
        this.endTs = endTs;
    }

    @Override
    public String toString() {
        return "MinMaxTemp{" +
                "id='" + id + '\'' +
                ", min=" + min +
                ", max=" + max +
                ", startTs=" + startTs +
                ", endTs=" + endTs +
                '}';
    }
}
