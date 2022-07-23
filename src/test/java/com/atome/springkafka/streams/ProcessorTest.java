package com.atome.springkafka.streams;

import com.ibm.gbs.schema.Balance;
import com.ibm.gbs.schema.Customer;
import com.ibm.gbs.schema.CustomerBalance;
import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

class ProcessorTest {
    private static final String SCHEMA_REGISTRY_SCOPE = ProcessorTest.class.getName();
    private static final String MOCK_SCHEMA_REGISTRY_URL = "mock://" + SCHEMA_REGISTRY_SCOPE;

    private TopologyTestDriver testDriver;

    private TestInputTopic<String, Customer> customerTopic;
    private TestInputTopic<String, Balance> balanceTopic;
    private TestOutputTopic<String, CustomerBalance> customerBalanceTopic;

    @BeforeEach
    @SuppressWarnings("resource")
    void beforeEach() {
        // Create topology to handle streams
        StreamsBuilder builder = new StreamsBuilder();
        new Processor().process(builder);
        Topology topology = builder.build();

        // Dummy properties needed for test diver
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, MOCK_SCHEMA_REGISTRY_URL);

        // Create test driver
        testDriver = new TopologyTestDriver(topology, props);

        // Create Serdes used for test record keys and values
        Serde<String> stringSerde = Serdes.String();
        Serde<Customer> avroCustomerSerde = new SpecificAvroSerde<>();
        Serde<Balance> avroBalanceSerde = new SpecificAvroSerde<>();
        Serde<CustomerBalance> avroCustomerBalanceSerde = new SpecificAvroSerde<>();

        // Configure Serdes to use the same mock schema registry URL
        Map<String, String> config = Map.of(
                AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, MOCK_SCHEMA_REGISTRY_URL);
        avroCustomerSerde.configure(config, false);
        avroBalanceSerde.configure(config, false);
        avroCustomerBalanceSerde.configure(config, false);

        // Define input and output topics to use in tests
        customerTopic = testDriver.createInputTopic(
                "Customer",
                stringSerde.serializer(),
                avroCustomerSerde.serializer());
        balanceTopic = testDriver.createInputTopic(
                "Balance",
                stringSerde.serializer(),
                avroBalanceSerde.serializer());
        customerBalanceTopic = testDriver.createOutputTopic(
                "CustomerBalance",
                stringSerde.deserializer(),
                avroCustomerBalanceSerde.deserializer());
    }

    @AfterEach
    void afterEach() {
        testDriver.close();
        MockSchemaRegistry.dropScope(SCHEMA_REGISTRY_SCOPE);
    }

    @Test
    void shouldPropagateUserWithFavoriteColorRed() {
        customerTopic.pipeInput("678", Customer.newBuilder()
                .setAccountId("11")
                .setCustomerId("678")
                .setPhoneNumber("888-888-8888")
                .setName("Nicole Anne Dime")
                .build());
        balanceTopic.pipeInput("11", Balance.newBuilder()
                .setAccountId("11")
                .setBalanceId("123")
                .setBalance(2.75f)
                .build());

        assertEquals(CustomerBalance.newBuilder()
                .setAccountId("11")
                .setCustomerId("678")
                .setPhoneNumber("888-888-8888")
                .setBalance(2.75f)
                .build(), customerBalanceTopic.readValue());
    }

}