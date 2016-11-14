import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Consumer {
    private final ConsumerConnector consumer;
    private final String topic;
    private ExecutorService executor;

    public Consumer(String zookeeper, String groupId, String topic) {
        consumer = kafka.consumer.Consumer.createJavaConsumerConnector(createConsumerConfig(zookeeper, groupId));
        this.topic = topic;
    }

    public void shutdown() {
        if (consumer != null)
            consumer.shutdown();
        if (executor != null)
            executor.shutdown();
    }

    public void run(int a_numThreads) {
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(topic, new Integer(a_numThreads));
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
        List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);

        executor = Executors.newFixedThreadPool(a_numThreads);

        int threadNumber = 0;
        for (final KafkaStream stream : streams) {
            executor.submit(new ConsumerTest(stream, threadNumber));
            threadNumber++;
        }
    }

    private static ConsumerConfig createConsumerConfig(String zookeeper, String groupId) {
        Properties props = new Properties();
        props.put("zookeeper.connect", zookeeper);
        props.put("auto.offset.reset", "largest");
        props.put("group.id", groupId);
        props.put("zookeeper.session.timeout.ms", "400");
        props.put("zookeeper.sync.time.ms", "200");
        props.put("auto.commit.interval.ms", "1000");

        return new ConsumerConfig(props);
    }

    public static void main(String[] args) throws InterruptedException {
        String zooKeeper = "localhost:2181";
        String groupId = "group0";
        String topic = "GrokkingLog";
        int threads = 4;

        Consumer example = new Consumer(zooKeeper, groupId, topic);
        example.run(threads);

        try {
            Thread.sleep(10000);
        } catch (InterruptedException ie) {

        }
        example.shutdown();

//        Thread.sleep(24*60*60*1000);
//
//        example.shutdown();
    }
}