## Sample App: Broker-Side SQL Filtering with RabbitMQ Streams

This repo contains the sample application for the blog post [Broker-Side SQL Filtering with RabbitMQ Streams](https://rabbitmq.com/blog/2025/09/23/sql-filter-expressions).

### Usage

1. Start RabbitMQ server 4.2:
```bash
docker run -it --rm --name rabbitmq -p 5672:5672 -e ERL_AFLAGS="+S 1" rabbitmq:4.2.0-beta.2
```
2. In this root directory, run the client:
```bash
mvn clean compile exec:java
```
