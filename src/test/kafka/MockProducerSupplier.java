package kafka;

import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Supplier;

public class MockProducerSupplier<K, V> implements Supplier<Producer<K, V>> {
    private static final Logger logger = LoggerFactory.getLogger("console");

    @Override
    public Producer<K, V> get() {
        logger.info("Creating new MockProducer...");
        return new MockProducer(false, new StringSerializer(), new StringSerializer());
    }
}
