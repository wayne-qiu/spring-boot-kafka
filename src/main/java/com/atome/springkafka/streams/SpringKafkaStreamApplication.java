package com.atome.springkafka.streams;

import com.ibm.gbs.schema.Balance;
import com.ibm.gbs.schema.Customer;
import com.ibm.gbs.schema.CustomerBalance;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.event.ApplicationStartedEvent;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.stereotype.Component;

import java.time.Duration;

@SpringBootApplication
@EnableKafkaStreams  //<- enable kafka streams
public class SpringKafkaStreamApplication {
    public static void main(String[] args) {
        SpringApplication.run(SpringKafkaStreamApplication.class, args);
    }
}

@Configuration
class KafkaConfig {

    @Bean
    NewTopic customer() {
        return TopicBuilder.name("Customer")
                .partitions(1)
                .replicas(3)
                .build();
    }
    @Bean
    NewTopic balance() {
        return TopicBuilder.name("Balance")
                .partitions(1)
                .replicas(3)
                .build();
    }

    @Bean
    NewTopic customerBalance() {
        return TopicBuilder.name("CustomerBalance")
                .partitions(1)
                .replicas(3)
                .build();
    }
    @Autowired
    ProducerFactory  producerFactory;

    @Bean
    @Qualifier("customerKafkaTemplate")
    public KafkaTemplate<String, Customer> customerKafkaTemplate() {
        KafkaTemplate<String, Customer> kafkaTemplate = new KafkaTemplate<>(producerFactory);
        return kafkaTemplate;
    }

    @Bean
    @Qualifier("balanceKafkaTemplate")
    public KafkaTemplate<String, Balance> balanceKafkaTemplate() {
        KafkaTemplate<String, Balance> kafkaTemplate = new KafkaTemplate<>(producerFactory);
        return kafkaTemplate;
    }
}

@RequiredArgsConstructor
@Component
class Producer {
    @Autowired
    @Qualifier("customerKafkaTemplate")
    private final KafkaTemplate<String, Customer> customerKafkaTemplate;
    @Autowired
    @Qualifier("balanceKafkaTemplate")
    private final KafkaTemplate<String, Balance> balanceKafkaTemplate;

    @EventListener(ApplicationStartedEvent.class)
    public void generate() {

        customerKafkaTemplate.send("Customer", "678",
                Customer.newBuilder()
                        .setAccountId("11")
                        .setCustomerId("678")
                        .setPhoneNumber("888-888-8888")
                        .setName("Nicole Anne Dime")
                        .build());

        balanceKafkaTemplate.send("Balance", "11",
                Balance.newBuilder()
                        .setAccountId("11")
                        .setBalanceId("123")
                        .setBalance(2.75f)
                        .build());

        System.out.println("sent......");
    }

}


@Component
class Consumer {
    @KafkaListener(topics = {"CustomerBalance"}, groupId = "spring-boot-kafka")
    public void consume(ConsumerRecord<String, CustomerBalance> record) {
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
            System.out.println("class:"+v.getClass()+" K:"+ k +" V:"+v.toString());
        }).to("CustomerBalance");
    }

}
