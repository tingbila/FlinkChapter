package bean;

public class WaterThreshold {
    //传感器ID
    private String id;

    //阈值水位高度
    private Integer thresholdVc;

    public WaterThreshold() {
    }

    public WaterThreshold(String id, Integer thresholdVc) {
        this.id = id;
        this.thresholdVc = thresholdVc;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Integer getThresholdVc() {
        return thresholdVc;
    }

    public void setThresholdVc(Integer thresholdVc) {
        this.thresholdVc = thresholdVc;
    }

    @Override
    public String toString() {
        return "WaterThreshold{" +
                "id='" + id + '\'' +
                ", thresholdVc=" + thresholdVc +
                '}';
    }
}
