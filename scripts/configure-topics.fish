#!/usr/bin/fish


set KAFKA localhost:9092
set REPLICATION_FACTOR 1
set PARTITIONS 10
set RETENTION_MS (echo '3*365*24*3600*1000'|bc) # 3 anos (ou 3 temporadas)

set GAME_RESULTS_TOPIC game-result-events
set TEAM_STATISTICS_TOPIC team-statistics
set SEASONS_RESULTS_TOPIC seasons-results



kafka-topics.sh --bootstrap-server $KAFKA --delete \
--topic $GAME_RESULTS_TOPIC || echo "Tópicos não existiam"

kafka-topics.sh --bootstrap-server $KAFKA --delete \
--topic $SEASONS_RESULTS_TOPIC || echo "Tópicos não existiam"

kafka-topics.sh --bootstrap-server $KAFKA --delete \
--topic $TEAM_STATISTICS_TOPIC || echo "Tópicos não existiam"


## TOpico compactado
kafka-topics.sh --bootstrap-server $KAFKA --create \
--topic $SEASONS_RESULTS_TOPIC  \
--partitions $PARTITIONS \
--replication-factor $REPLICATION_FACTOR \
--config cleanup.policy=compact


kafka-topics.sh --bootstrap-server $KAFKA --create \
--topic $TEAM_STATISTICS_TOPIC  \
--partitions $PARTITIONS \
--replication-factor $REPLICATION_FACTOR \
--config cleanup.policy=compact


# Criando tópico sem compactação
kafka-topics.sh --bootstrap-server $KAFKA --create \
--topic $GAME_RESULTS_TOPIC \
--partitions $PARTITIONS \
--replication-factor $REPLICATION_FACTOR \
--config cleanup.policy=delete \
--config retention.ms=$RETENTION_MS
