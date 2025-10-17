package com.acme.footballstandings;

import com.acme.footballstandings.model.AbstractModel;
import com.acme.footballstandings.model.TeamType;

public class TeamStatistics extends AbstractModel {

    private TeamType team;
    private Short gamesPlayed  = 0;
    private Short wins  = 0;
    private Short losses = 0;
    private Short draws  = 0;
    private Integer goalsScored  = 0;
    private Integer goalsConceded  = 0;
    private Short goalDifference  = 0;
    private int points  = 0;
    private Short season;


    public TeamStatistics() {
    }

    public TeamStatistics update(TeamType team, Integer goalsScored, Integer goalsConceded){

        if(goalsScored > goalsConceded){
            this.wins++;
            this.points += 3;
        } else if(goalsScored < goalsConceded){
            this.losses++;
        } else {
            this.draws++;
            this.points++;
        }
        this.gamesPlayed++;
        this.goalsScored += goalsScored;
        this.goalsConceded += goalsConceded;
        this.goalDifference = (short) (this.goalsScored - this.goalsConceded);
        this.team = team;
        return this;
    }

    public TeamType getTeam() {
        return team;
    }

    public void setTeam(TeamType team) {
        this.team = team;
    }

    public Short getGamesPlayed() {
        return gamesPlayed;
    }

    public void setGamesPlayed(Short gamesPlayed) {
        this.gamesPlayed = gamesPlayed;
    }

    public Short getWins() {
        return wins;
    }

    public void setWins(Short wins) {
        this.wins = wins;
    }

    public Short getLosses() {
        return losses;
    }

    public void setLosses(Short losses) {
        this.losses = losses;
    }

    public Short getDraws() {
        return draws;
    }

    public void setDraws(Short draws) {
        this.draws = draws;
    }

    public Integer getGoalsScored() {
        return goalsScored;
    }

    public void setGoalsScored(Integer goalsScored) {
        this.goalsScored = goalsScored;
    }

    public Integer getGoalsConceded() {
        return goalsConceded;
    }

    public void setGoalsConceded(Integer goalsConceded) {
        this.goalsConceded = goalsConceded;
    }

    public Short getGoalDifference() {
        return goalDifference;
    }

    public void setGoalDifference(Short goalDifference) {
        this.goalDifference = goalDifference;
    }

    public int getPoints() {
        return points;
    }

    public void setPoints(int points) {
        this.points = points;
    }

    public Short getSeason() {
        return season;
    }

    public void setSeason(Short season) {
        this.season = season;
    }
}
