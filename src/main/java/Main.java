import static spark.Spark.*;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

public class Main {
    private static Properties props;
    private static ProducerConfig config;
    private static Producer<String, String> producer;
    private static final String topic = "GrokkingLog";
    private static final String brokers = "localhost:9092";

    public static void sendToKafka(String s) {
        KeyedMessage<String, String> data = new KeyedMessage<String, String>(topic, s);
        producer.send(data);
    }

    public static String getCurrentTimeStamp() {
        SimpleDateFormat sdfDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        Date now = new Date();
        String strDate = sdfDate.format(now);
        return strDate;
    }

    public static void main(String[] args) {
        port(8080);
        threadPool(8);

        props = new Properties();
        props.put("metadata.broker.list", brokers);
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("producer.type", "async");

        config = new ProducerConfig(props);

        producer = new Producer<String, String>(config);

        get("/hello", (req, res) -> {
            StringBuilder builder = new StringBuilder();
            builder.append(req.ip());
            builder.append(" ");
            builder.append(req.userAgent());
            builder.append(" ");
            builder.append(getCurrentTimeStamp());
            builder.append(" ");
            builder.append(req.queryString());
            builder.append(" ");
            builder.append(req.body());

            String ret = builder.toString();
            sendToKafka(ret);

            System.out.println(ret);
            return ret;
        });
    }
}