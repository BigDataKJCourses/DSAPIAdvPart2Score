package com.example.bigdata.model;

import java.util.HashSet;
import java.util.Set;

public class HouseStatsAccumulator {
    private long howMany;
    private long sumScore;
    private Set<String> characters;

    public HouseStatsAccumulator() {
        this.howMany = 0;
        this.sumScore = 0;
        this.characters = new HashSet<>();
    }

    public void add(ScoreEvent event) {
        howMany++;
        sumScore += event.getScore();
        characters.add(event.getCharacter());
    }

    public HouseStats getResult() {
        HouseStats result = new HouseStats();
        result.setHowMany(howMany);
        result.setSumScore(sumScore);
        result.setNoCharacters(characters.size());
        return result;
    }

    public HouseStatsAccumulator merge(HouseStatsAccumulator other) {
        this.howMany += other.howMany;
        this.sumScore += other.sumScore;
        this.characters.addAll(other.characters);
        return this;
    }

}
