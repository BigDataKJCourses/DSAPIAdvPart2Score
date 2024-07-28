package com.example.bigdata.model;

public class HouseStats {
    private long howMany;
    private long sumScore;
    private long noCharacters;

    public HouseStats(long howMany, long sumScore, long noCharacters) {
        this.howMany = howMany;
        this.sumScore = sumScore;
        this.noCharacters = noCharacters;
    }

    public HouseStats() {
        this.howMany = 0;
        this.sumScore = 0;
        this.noCharacters = 0;
    }

    public long getHowMany() {
        return howMany;
    }

    public void setHowMany(long howMany) {
        this.howMany = howMany;
    }

    public long getSumScore() {
        return sumScore;
    }

    public void setSumScore(long sumScore) {
        this.sumScore = sumScore;
    }

    public long getNoCharacters() {
        return noCharacters;
    }

    public void setNoCharacters(long noCharacters) {
        this.noCharacters = noCharacters;
    }

    @Override
    public String toString() {
        return "HouseStats{" +
                " howMany=" + howMany +
                ", sumScore=" + sumScore +
                ", noCharacters=" + noCharacters +
                '}';
    }
}

