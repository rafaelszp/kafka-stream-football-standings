package com.acme.footballstandings.util;
import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithName;

@ConfigMapping(prefix = "config.football")
public interface Config {

    @WithName("game_result_topic")
    String gameResultTopic();

    @WithName("seasons_table")
    String seasonsTable();

    @WithName("team_results")
    String teamResults();

    @WithName("seasons_results")
    String seasonResults();

    @WithName("processed_game_table")
    String processedGamesStore();
}
