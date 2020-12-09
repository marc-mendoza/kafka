package kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.function.Supplier;

public class TransactionalProducer<K, V> implements Producer<K, V> {
    private final static Logger LOGGER = LoggerFactory.getLogger(TransactionalProducer.class);
    private final static ThreadLocal<Producer> localProducer = new ThreadLocal<>();
    private final Supplier<Producer> producerSupplier;

    public TransactionalProducer(Supplier<Producer> producerSupplier) {
        LOGGER.info("Creating TransactionalProducer...");
        this.producerSupplier = producerSupplier;
    }

    private Producer producer() {
        if (localProducer.get() == null) {
            synchronized (producerSupplier) {
                localProducer.set(producerSupplier.get());
                localProducer.get().initTransactions();
            }
        }
        return localProducer.get();
    }

    @Override
    public void initTransactions() {
        producer().initTransactions();
    }

    @Override
    public void beginTransaction() throws ProducerFencedException {
        producer().beginTransaction();
    }

    @Override
    public void commitTransaction() throws ProducerFencedException {
        producer().commitTransaction();
    }

    @Override
    public void abortTransaction() throws ProducerFencedException {
        producer().abortTransaction();
    }

    @Override
    public Future<RecordMetadata> send(ProducerRecord producerRecord) {
        return producer().send(producerRecord);
    }

    @Override
    public Future<RecordMetadata> send(ProducerRecord producerRecord, Callback callback) {
        return producer().send(producerRecord, callback);
    }

    @Override
    public void flush() {
        producer().flush();
    }

    @Override
    public List<PartitionInfo> partitionsFor(String s) {
        return producer().partitionsFor(s);
    }

    @Override
    public Map<MetricName, ? extends Metric> metrics() {
        return producer().metrics();
    }

    @Override
    public void close() {
        producer().close();
    }

    @Override
    public void close(Duration duration) {
        producer().close(duration);
    }

    @Override
    public void sendOffsetsToTransaction(Map map, String s) throws ProducerFencedException {
        producer().sendOffsetsToTransaction(map, s);
    }
}
