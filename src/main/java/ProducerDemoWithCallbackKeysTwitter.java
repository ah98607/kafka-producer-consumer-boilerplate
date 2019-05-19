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

    // access keys and tokens have been intentionally modified
    // please replace them with yours
    static final String CONSUMER_KEY = "oYcIXQZKndRyocIMxrw7b0CnL";
    static final String CONSUMER_SECRET = "5IkIyBrHtIqdpZFKHuGGKrmipjv78lgHXUnmHvtCfdcEDEWB5d";
    static final String ACCESS_TOKEN = "1033057440403615744-7DYaHDy5JvKpOVEBCmljBGpju3rqUS";
    static final String ACCESS_TOKEN_SECRET = "OaRIgV1P1Z1OwykPRWjBFgvUodhsbQD9IsZ9afTU1CgAP";

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

    private static List<Status> findTweetsByKeyWord(Twitter twitter, String searchTerm) throws TwitterException {

        Query query = new Query(searchTerm);
        QueryResult queryResult = twitter.search(query);
        return queryResult.getTweets();
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
        List<Status> tweets = findTweetsByKeyWord(twitter, "New York");
        logger.info("Size of tweets: " + tweets.size());

        for (Status status : tweets) {
            //System.out.println("@" + status.getUser().getScreenName() + ":" + status.getText());
            logger.info("@" + status.getUser().getScreenName() + ":" + status.getText());

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
