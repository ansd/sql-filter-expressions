package com.github.ansd;

import java.nio.charset.StandardCharsets;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import com.rabbitmq.client.amqp.*;
import com.rabbitmq.client.amqp.impl.AmqpEnvironmentBuilder;
import static com.rabbitmq.client.amqp.ConsumerBuilder.StreamOffsetSpecification.FIRST;

public class App {
    private static final int TOTAL_MESSAGES = 10_000_000;
    private static final int ORDER_CREATED_INTERVAL = 100_000;
    private static final String STREAM_NAME = "customer-events";

    private static final String[] EVENT_TYPES = {
        "user.login",
        "product.view",
        "cart.add",
        "cart.remove",
        "product.search",
        "user.logout",
        "payment.method.added",
        "address.updated",
        "wishlist.add",
        "review.submitted"
    };

    private static final String[] REGIONS = {"LATAM", "AMER", "EMEA", "APAC"};

    private static final String SQL_FILTER =
        "p.subject = 'order.created' AND " +
        "p.creation_time > UTC() - 3600000 AND " +
        "region IN ('AMER', 'EMEA', 'APJ') AND " +
        "(h.priority > 4 OR price >= 99.99 OR premium_customer = TRUE)";

    private static final Random random = new Random(42); // Seed for reproducible results

    public static void main(String[] args) throws Exception {
        System.out.println("SQL Filter Expressions Demo for RabbitMQ 4.2");
        System.out.println("===========================================");
        System.out.println("Simulating an e-commerce event stream with various customer events");
        System.out.println();

        Environment environment = new AmqpEnvironmentBuilder().build();
        Connection connection = environment.connectionBuilder().build();
        Management management = connection.management();

        management
            .queue()
            .name(STREAM_NAME)
            .stream()
            .maxSegmentSizeBytes(ByteCapacity.MB(500))
            .queue()
            .declare();

        System.out.println("Declared stream: " + STREAM_NAME);

        publishMessages(connection);

        System.out.println("\n=== Starting Consumer 1: SQL Filter Only (Stage 2) ===");
        System.out.println("SQL Filter: " + SQL_FILTER);
        consumeMessages(connection, false);

        System.out.println("\n=== Starting Consumer 2: Bloom Filter (Stage 1) + SQL Filter (Stage 2) ===");
        System.out.println("Bloom filter: 'order.created'");
        System.out.println("SQL Filter: " + SQL_FILTER);
        consumeMessages(connection, true);

        connection.close();
        environment.close();

        System.out.println("\nDemo completed!");
    }

    private static void publishMessages(Connection connection) throws Exception {
        Publisher publisher = connection
            .publisherBuilder()
            .queue(STREAM_NAME)
            .build();

        System.out.println("\nPublishing messages...");
        System.out.println("  Regular events: " + String.join(", ", EVENT_TYPES));
        System.out.println("  Special event 'order.created' every " + ORDER_CREATED_INTERVAL + " messages");
        System.out.println("  1 in 10 'order.created' events will match all SQL filter criteria");
        System.out.println();

        int orderCreatedCount = 0;
        int matchingCount = 0;

        for (int i = 1; i <= TOTAL_MESSAGES; i++) {
            Message message;

            if (i % ORDER_CREATED_INTERVAL == 0) {
                // This is an order.created event
                orderCreatedCount++;

                // Every 10th order.created event will match all our SQL criteria
                boolean shouldMatchAllCriteria = (orderCreatedCount % 10 == 0);
                message = createMessage(publisher, i, "order.created", shouldMatchAllCriteria);
                if (shouldMatchAllCriteria) {
                    matchingCount++;
                    System.out.printf("  Creating MATCHING order at position %,d\n", i);
                }
            } else {
                // Regular event
                String eventType = EVENT_TYPES[i % EVENT_TYPES.length];
                message = createMessage(publisher, i, eventType, false);
            }

            publisher.publish(message, context -> {});
        }

        System.out.printf("\nTotal published: %,d messages\n", TOTAL_MESSAGES);
        System.out.printf("Total 'order.created' events: %d\n", orderCreatedCount);
        System.out.printf("Total events matching all SQL criteria: %d\n", matchingCount);
    }

    private static Message createMessage(Publisher publisher, int index, String eventType,
                                       boolean shouldMatchSqlCriteria) {

        String region = shouldMatchSqlCriteria ? "EMEA" : REGIONS[index % REGIONS.length];
        byte priority = (byte) (shouldMatchSqlCriteria ? 5 : 4);

        long creationTime = System.currentTimeMillis();

        String body = String.format("Event #%d - Type: %s, Region: %s",
            index, eventType, region);

        return publisher
            .message(body.getBytes(StandardCharsets.UTF_8))
            // header section
            .priority(priority)
            // message-annotation section
            // set the Bloom filter value
            .annotation("x-stream-filter-value", eventType)
            // properties section
            .subject(eventType)
            .creationTime(creationTime)
            // application-properties section
            .property("region", region);
    }

    private static void consumeMessages(Connection connection, boolean useBloomFilter) throws Exception {
        String consumerType = useBloomFilter ? "Bloom+SQL" : "SQL Only";

        // Expect 10 matching messages
        CountDownLatch latch = new CountDownLatch(10);
        long startTime = System.currentTimeMillis();

        // Build consumer with or without Bloom filter
        ConsumerBuilder.StreamOptions builder = connection.consumerBuilder()
            .queue(STREAM_NAME)
            .stream()
            .offset(FIRST);

        if (useBloomFilter) {
            // Stage 1: Bloom filter - quickly skip chunks without order.created events
            builder = builder.filterValues("order.created");
        }

        Consumer consumer = builder
            // Stage 2: SQL filter - precise broker-side per-message filtering
            .filter()
                .sql(SQL_FILTER)
            .stream()
            .builder()
            .messageHandler((ctx, msg) -> {
                System.out.printf("  [%s] Received: %s\n",
                    consumerType, new String(msg.body(), StandardCharsets.UTF_8));
                latch.countDown();
                ctx.accept();
            })
            .build();

        // Wait for all matching messages
        latch.await();

        long durationMillis = System.currentTimeMillis() - startTime;
        double durationSeconds = durationMillis / 1000.0;
        double filteringRate = TOTAL_MESSAGES / durationSeconds;

        System.out.printf("Received 10 messages in %.2f seconds using %s\n",
            durationSeconds, useBloomFilter ? "Bloom + SQL filters" : "SQL filter only");
        System.out.printf("Broker-side filtering rate: %,.0f messages/second\n", filteringRate);

        consumer.close();
    }
}
