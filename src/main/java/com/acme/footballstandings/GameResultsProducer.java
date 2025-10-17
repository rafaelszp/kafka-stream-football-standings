package com.acme.footballstandings;

import com.acme.footballstandings.model.GameResult;
import com.acme.footballstandings.model.TeamType;
import com.acme.footballstandings.serdes.JsonSerializer;
import com.github.f4b6a3.ulid.UlidCreator;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;

public class GameResultsProducer {

    public static final String TOPIC = "game-result-events";

    public static void main(String[] args) {

        System.out.println("Hello, Kafka Transaction Producer!");
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.ACKS_CONFIG, "all"); // Confirmação de recebimento
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        JsonSerializer<GameResult> serializer = new JsonSerializer<>();

        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, serializer.getClass().getName());

        ThreadLocalRandom random = ThreadLocalRandom.current();
        short season = (short)random.nextInt(2023,2026);
        List<GameResult> resultList = new ArrayList<>();
//        try (Producer<String, GameResult> producer = new KafkaProducer<>(props))
        try (Producer<String, GameResult> producer = new KafkaProducer<>(props)) {
            for (TeamType team : TeamType.values()) {
                for (TeamType otherTeam : TeamType.values()) {
                    if (team == otherTeam) {
                        continue;
                    }
                    String gameId = UlidCreator.getMonotonicUlid().toLowerCase();
                    Integer homeScore = random.nextInt(0, 5);
                    Integer awayScore = random.nextInt(0, 5);
                    GameResult gameResult = new GameResult(gameId, season, team, otherTeam, homeScore, awayScore);
                    ProducerRecord<String, GameResult> record = new ProducerRecord<>(TOPIC, gameId, gameResult);
                    producer.send(record, ((metadata, exception) -> {
                        if (exception == null) {
                            // Mensagem enviada com sucesso!
                            System.out.printf("Mensagem enviada com sucesso! Tópico: %s, Partição: %d, Offset: %d%n",
                                    metadata.topic(), metadata.partition(), metadata.offset());
                        } else {
                            // Ocorreu um erro
                            System.err.println("Erro ao enviar mensagem: " + exception.getMessage());
                        }
                    }));
                }
            }
        }
    }

}
