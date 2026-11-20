package raft04;

public final class Partitioner {
    private final int n;

    public Partitioner(int n) {
        this.n = n;
    }

    public int of(String sensorId) {
        return Math.floorMod(sensorId.hashCode(), n);
    }
}
