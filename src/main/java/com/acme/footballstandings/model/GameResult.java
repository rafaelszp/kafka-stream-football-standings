package com.acme.footballstandings.model;

public class GameResult extends AbstractModel{

    private String id;
    private Short season;
    private TeamType homeTeam;
    private TeamType awayTeam;
    private Integer homeScore;
    private Integer awayScore;

    public GameResult() {
    }

    public GameResult(String id, Short season, TeamType homeTeam, TeamType awayTeam, Integer homeScore, Integer awayScore) {
        this.id = id;
        this.season = season;
        this.homeTeam = homeTeam;
        this.awayTeam = awayTeam;
        this.homeScore = homeScore;
        this.awayScore = awayScore;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Short getSeason() {
        return season;
    }

    public void setSeason(Short season) {
        this.season = season;
    }

    public TeamType getHomeTeam() {
        return homeTeam;
    }

    public void setHomeTeam(TeamType homeTeam) {
        this.homeTeam = homeTeam;
    }

    public TeamType getAwayTeam() {
        return awayTeam;
    }

    public void setAwayTeam(TeamType awayTeam) {
        this.awayTeam = awayTeam;
    }

    public Integer getHomeScore() {
        return homeScore;
    }

    public void setHomeScore(Integer homeScore) {
        this.homeScore = homeScore;
    }

    public Integer getAwayScore() {
        return awayScore;
    }

    public void setAwayScore(Integer awayScore) {
        this.awayScore = awayScore;
    }



}
