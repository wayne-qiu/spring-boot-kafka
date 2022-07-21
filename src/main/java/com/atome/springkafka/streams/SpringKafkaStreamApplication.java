package com.atome.springkafka.streams;

import com.github.javafaker.Faker;
import com.ibm.gbs.schema.Balance;
import com.ibm.gbs.schema.Customer;
import com.ibm.gbs.schema.CustomerBalance;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.event.ApplicationStartedEvent;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Profile;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

@SpringBootApplication
@EnableKafkaStreams  //<- enable kafka streams
public class SpringKafkaStreamApplication {
    public static void main(String[] args) {
        SpringApplication.run(SpringKafkaStreamApplication.class, args);
    }

//    @Bean
//    NewTopic customer() {
//        return TopicBuilder.name("Customer")
//                .partitions(1)
//                .replicas(3)
//                .build();
//    }
//
//    @Bean
//    NewTopic balance() {
//        return TopicBuilder.name("Balance")
//                .partitions(1)
//                .replicas(3)
//                .build();
//    }
//
//    @Bean
//    NewTopic customerBalance() {
//        return TopicBuilder.name("CustomerBalance")
//                .partitions(1)
//                .replicas(3)
//                .build();
//    }
}

@RequiredArgsConstructor
@Component
class Producer {
    @Autowired
//    private final KafkaTemplate<String, Customer> template;
//    private final KafkaTemplate<String, Balance> template;
//    private final KafkaTemplate<String, Customer> template;

    @EventListener(ApplicationStartedEvent.class)
    public void generate() {

//        template.send("Customer", "678",
//                Customer.newBuilder()
//                        .setAccountId("11")
//                        .setCustomerId("678")
//                        .setPhoneNumber("888-888-8888")
//                        .setName("Nicole Anne Dime")
//                        .build());

//        template.send("Balance", "11",
//                Balance.newBuilder()
//                        .setAccountId("11")
//                        .setBalanceId("123")
//                        .setBalance(2.75f)
//                        .build());

//        template.send("Customer", "11",
//                Customer.newBuilder()
//                        .setAccountId("11")
//                        .setCustomerId("678")
//                        .setPhoneNumber("888-888-8888")
//                        .setName("Nicole Anne Dime")
//                        .build());

        System.out.println("sent......");
    }

}


//@Component
class Consumer {
    @KafkaListener(topics = {"Customer"}, groupId = "spring-boot-kafka")
    public void consume(ConsumerRecord<String, Customer> record) {
//    @KafkaListener(topics = {"Balance"}, groupId = "spring-boot-kafka")
//    public void consume(ConsumerRecord<String, Balance> record) {
        System.out.println("received......");
        System.out.println("received = " + record.value() + " with key " + record.key());
    }
}

@Component
class Processor {

    @Autowired
    public void process(StreamsBuilder builder) {
        System.out.println("stream.....");

        KStream<String, Customer> customerStream = builder.stream("Customer");
        KStream<String, Balance> balanceStream = builder.stream("Balance");


        KStream<String, CustomerBalance> joinedKStream = customerStream.selectKey((k,v)->{
            return v.getAccountId();
        }).join(balanceStream,
                new ValueJoiner<Customer, Balance, CustomerBalance>() {
                    @Override
                    public CustomerBalance apply(Customer value1, Balance value2) {
                        return CustomerBalance.newBuilder()
                                .setAccountId(value1.getAccountId())
                                .setCustomerId(value1.getCustomerId())
                                .setPhoneNumber(value1.getPhoneNumber())
                                .setBalance(value2.getBalance())
                                .build();
                    }
                },
                JoinWindows.of(Duration.ofMinutes(10)));

        joinedKStream.peek((k,v)->{
            System.out.println("class:"+v.getClass()+"  K:"+ k +"V:"+v.toString());
        }).to("CustomerBalance");
    }

}
