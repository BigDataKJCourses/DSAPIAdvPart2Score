package com.example.bigdata.model;

import java.text.SimpleDateFormat;
import java.util.Date;

public class HouseStatsResult extends HouseStats {
    private String house;
    private long from;
    private long to;


    public HouseStatsResult(String house, long howMany, long sumScore, long noCharacters, long from, long to) {
        super(howMany, sumScore, noCharacters);
        this.house = house;
        this.from = from;
        this.to = to;
    }

    public String getHouse() {
        return house;
    }

    public void setHouse(String house) {
        this.house = house;
    }

    public long getFrom() {
        return from;
    }

    public String getFromAsString() {
        SimpleDateFormat timeFormat = new SimpleDateFormat("HH:mm");
        return timeFormat.format(new Date(getFrom()));
    }

    public void setFrom(long from) {
        this.from = from;
    }

    public long getTo() {
        return to;
    }

    public String getToAsString() {
        SimpleDateFormat timeFormat = new SimpleDateFormat("HH:mm");
        return timeFormat.format(new Date(getTo()));
    }

    public void setTo(long to) {
        this.to = to;
    }

    @Override
    public String toString() {
        SimpleDateFormat timeFormat = new SimpleDateFormat("HH:mm");

        return "HouseStatsResult{" +
                "house='" + getHouse() + '\'' +
                ", from=" + timeFormat.format(new Date(getFrom())) +
                ", to=" + timeFormat.format(new Date(getTo())) +
                ", howMany=" + getHowMany() +
                ", sumScore=" + getSumScore() +
                ", noCharacters=" + getNoCharacters() +
                '}';
    }
}

