package kafka;

import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.function.Supplier;

public class MockProducerSupplier<K, V> implements Supplier<Producer<K, V>> {
    private static final Logger logger = LoggerFactory.getLogger(MockProducerSupplier.class);
    Queue<MockProducer> globalQueue = new ConcurrentLinkedDeque<>();

    @Override
    public Producer<K, V> get() {
        logger.info("Thread {} is creating new MockProducer...", Thread.currentThread().getName());
        final MockProducer mockProducer = new MockProducer(false, new StringSerializer(), new StringSerializer());
        globalQueue.add(mockProducer);
        return mockProducer;
    }

    public long getCountSentMessages() {
        return globalQueue.stream().mapToInt(p -> p.history().size()).sum();
    }
}
