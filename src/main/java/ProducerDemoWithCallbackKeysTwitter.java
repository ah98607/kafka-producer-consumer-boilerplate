import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class ProducerDemoWithCallbackKeysTwitter {

    // twitter4j tutorial
    // https://linuxhint.com/twitter4j-tutorial/

    static final String CONSUMER_KEY = "you-key";
    static final String CONSUMER_SECRET = "secret";
    static final String ACCESS_TOKEN = "token";
    static final String ACCESS_TOKEN_SECRET = "token-secret";

    public static Twitter getTwitterInstance() {
        ConfigurationBuilder cb = new ConfigurationBuilder();
        cb.setDebugEnabled(true);
        cb.setOAuthConsumerKey(CONSUMER_KEY);
        cb.setOAuthConsumerSecret(CONSUMER_SECRET);
        cb.setOAuthAccessToken(ACCESS_TOKEN);
        cb.setOAuthAccessTokenSecret(ACCESS_TOKEN_SECRET);
        TwitterFactory tf = new TwitterFactory(cb.build());
        return tf.getInstance();
    }

    private static QueryResult findTweetsByKeyWord(Twitter twitter, String searchTerm) throws TwitterException {

        Query query = new Query("source:" + searchTerm);
        return twitter.search(query);
    }

    public static void main(String[] args) throws TwitterException {

        // bootstrap server
        String bootstrapServer = "127.0.0.1:9092";

        // create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // logger
        final Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallbackKeysTwitter.class.getName());

        // initialize producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        // initialize twitter helper instance
        Twitter twitter = getTwitterInstance();

        // query tweets by a key word
        QueryResult queryResult = findTweetsByKeyWord(twitter, "New York");
        System.out.println("Size of queryResult: " + queryResult.getCount());

        // feed producer with tweets
        for (Status status : queryResult.getTweets()) {
            System.out.println("@" + status.getUser().getScreenName() + ":" + status.getText());

            // feed consumer with tweet data
            String topic = "first_topic";
            String value = status.getText();
            // if you run main again, message with the same key will go to
            // the same partition where the message went previously
            String key =  String.valueOf(status.getId());
            logger.info(("key = " + key));

            ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, key, value);
            //producer.send(record);
            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    // this function is called every time a record is sent
                    if (e == null) {
                        logger.info("Received meta data \n" +
                                "Topic: " + recordMetadata.topic() + "\n" +
                                "Partition: " + recordMetadata.partition() + "\n" +
                                "Offset: " + recordMetadata.offset() + "\n" +
                                "Timestamp: " + recordMetadata.timestamp());
                    }
                    else {
                        logger.error("Exception while producing", e);
                    }
                }
            });
        }

        producer.flush();
        producer.close();

        // how to test
        // suppose topic "first_topic" alerady exists
        // run kafka-console-consumer.sh --bootstrap-server 127.0.0.1:9092 --topic first_topic
        // click run main

    }

}
