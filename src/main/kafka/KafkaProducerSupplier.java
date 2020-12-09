package kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

public class KafkaProducerSupplier<K, V> implements Supplier<Producer<K, V>>, ProducerSupplier {
    private static final Logger logger = LoggerFactory.getLogger("console");
    private static final String TRANSACTIONAL_ID = "transactional.id";
    private final AtomicInteger producerCount = new AtomicInteger(0);
    private final String baseTransactionId;
    private final Properties properties;

    public KafkaProducerSupplier(Properties properties) {
        this.properties = properties;
        this.baseTransactionId = (String) properties.get(TRANSACTIONAL_ID);
    }

    @Override
    public Producer<K, V> get() {
        String id = baseTransactionId + "." + producerCount.getAndIncrement();
        logger.info("Creating new KafkaProducer with transactionalId: {}", id);
        properties.setProperty(TRANSACTIONAL_ID, id);
        return new KafkaProducer<>(properties);
    }

    @Override
    public int getProducerCount() {
        return producerCount.get();
    }
}
