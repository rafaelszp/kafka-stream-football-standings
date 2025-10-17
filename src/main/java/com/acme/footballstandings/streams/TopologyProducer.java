package com.acme.footballstandings.streams;

import com.acme.footballstandings.TeamStatistics;
import com.acme.footballstandings.model.GameResult;
import com.acme.footballstandings.model.TeamType;
import com.acme.footballstandings.serdes.JSONSerdes;
import com.acme.footballstandings.serdes.SerdeFactory;
import com.acme.footballstandings.util.Config;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.Arrays;
import java.util.Objects;

@ApplicationScoped
public class TopologyProducer {

    @Inject
    Config config;

/*
* Atenção!
*
* Esta topologia funciona desde que os resultados na sink não sejam reenviados pelo producer
* caso contrário ele vai continuar somando os valores da tabela do time, somando de forma duplicada.
* Em relação ao reprocessamento por uma aplicação de group.id diferente, o problema é resolvido ao utilizar a property
*`kafka.processing.guarantee=exactly_once_v2` que garante que o offset será processado apenas uma vez
* Porém se surgir um evento duplicado no sink, mais controles precisam ser feitos para evitar que isso ocorra
 * */
    @Produces
    public Topology createTopology(){
        StreamsBuilder builder = new StreamsBuilder();

        JSONSerdes<GameResult> gameResultSerdes = getGameResultSerdes();
        JSONSerdes<TeamStatistics> teamStatisticsSerdes = getTeamStatisticsSerdes();

        //Nesta stream, capturamos os resultados de todos os jogos produzidos
        KStream<String,GameResult> gameResultStream = builder.stream(config.gameResultTopic(), Consumed.with(Serdes.String(),gameResultSerdes));

        //Separando a stream para que cada time tenha seus jogos registrados por temporada
        //Usamos flatMap pois estamos mudando a chave que era por id do resultado, agora será por {temporada}-{time}
        //Cada resultado gerará outros 2, um para o time da casa e outro para o time fora
        //De modo que utilizaremos esses novos dados para computar na agregação adiante
        KStream<String, GameResult> resultPerTeamStream = gameResultStream.flatMap((key, game) -> Arrays.asList(
                new KeyValue<>(game.getSeason() + "-" + game.getHomeTeam().name(), game),
                new KeyValue<>(game.getSeason() + "-" + game.getAwayTeam().name(), game)
        ));

        //Aqui estamos agrupando pela nova chave
        KGroupedStream<String, GameResult> groupedBySeasonTeam = resultPerTeamStream.groupByKey(Grouped.with(Serdes.String(), gameResultSerdes));

        KTable<String, TeamStatistics> teamStatisticsTable = groupedBySeasonTeam.aggregate(
                TeamStatistics::new,
                (key, game, aggregate) -> {
                    String teamName = key.split("-")[1];
                    TeamType team = TeamType.getByName(teamName);
                    if(team==null){
                        return aggregate;
                    }
                    if(aggregate.getSeason() == null){
                        aggregate.setSeason(game.getSeason());
                    }
                    //Aqui é onde a mágica acontece
                    //Se o time atual estiver sendo processado, vamos computar o resultado para a tabela
                    //Caso contrário, coputaremos as estatísticas do outro time
                    //Como é uma agregação separada por por temporada e time, cada um terá suas estatísticas atualizadas
                    //a cada novo registro GameResult que chega na Sink
                    if(Objects.equals(team,game.getHomeTeam())){
                       return  aggregate.update(team, game.getHomeScore(), game.getAwayScore());
                    }else {
                        aggregate.update(team, game.getAwayScore(), game.getHomeScore());
                    }
                    return aggregate;
                },
                Materialized.<String,TeamStatistics, KeyValueStore<Bytes,byte[]>>as(config.seasonsTable())
                        .withKeySerde(Serdes.String())
                        .withValueSerde(teamStatisticsSerdes)
        );

        //Salvando em uma "Visão materalizada" para que possa ser processado por qualquer aplicação interessada
        //nos resultados da temporada e time
        teamStatisticsTable.toStream().to(config.teamResults(), Produced.with(Serdes.String(),teamStatisticsSerdes));

        //Agora vamos registrar os resultados em um tópico separado pela nova chave: {temporada}-{time}
        //Quem quiser fazer outras análises não precisará fazer o split da sink original novamente.
        resultPerTeamStream.to(config.seasonResults(),Produced.with(Serdes.String(),gameResultSerdes));

        return  builder.build();


    }

    private JSONSerdes<TeamStatistics> getTeamStatisticsSerdes() {
        return new SerdeFactory<TeamStatistics>().createSerde(TeamStatistics.class);
    }

    private JSONSerdes<GameResult> getGameResultSerdes() {
        return new SerdeFactory<GameResult>().createSerde(GameResult.class);
    }

}
