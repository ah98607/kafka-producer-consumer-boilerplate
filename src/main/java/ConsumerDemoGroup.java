import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Properties;

// TODO
public class ConsumerDemoGroup {
    public static void main(String[] args) {

        Logger logger = LoggerFactory.getLogger(ConsumerDemoGroup.class.getName());

        // bootstrap server
        String bootstrapServer = "127.0.0.1:9092";
        String groupId = "my_first_application";
        Collection<String> topicList = new ArrayList<String>();
        topicList.add("first_topic");

        // consumer properties
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // initialize consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>((properties));

        // subscribe to topic
        consumer.subscribe(topicList);

        // poll for new data
        while (true) {
            Duration timeout = Duration.ofMillis(100);
            ConsumerRecords<String, String> consumerRecords = consumer.poll(timeout);

            for (ConsumerRecord consumerRecord : consumerRecords) {
                logger.info("Key: " + consumerRecord.key());
                logger.info("Value: " + consumerRecord.value());
                logger.info("Partition: " + consumerRecord.partition());
                logger.info("Offset: " + consumerRecord.offset());
            }
        }
    }
}
