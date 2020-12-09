package kafka;

import com.salesforce.kafka.test.KafkaTestUtils;
import com.salesforce.kafka.test.junit4.SharedKafkaTestResource;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.assertj.core.api.Assertions.assertThat;

public class TransactionalProducerTest {
    private final static Logger logger = LoggerFactory.getLogger(TransactionalProducerTest.class);
    private final static int MESSAGE_COUNT = 1000;
    private final static int THREAD_COUNT = 5;
    private final static int TASK_COUNT = 50;

    @ClassRule
    public static final SharedKafkaTestResource sharedKafkaTestResource = new SharedKafkaTestResource();

    private String topicName;

    @Before
    public void beforeTest() {
        // Generate topic name
        topicName = getClass().getSimpleName() + Clock.systemUTC().millis();

        // Create topic with 3 partitions,
        // NOTE: This will create partition ids 0 thru 2, because partitions are indexed at 0 :)
        getKafkaTestUtils().createTopic(topicName, 1, (short) 1);
    }

    @Test
    public void testTransactionalProducer() {
        final KafkaTestUtils kafkaTestUtils = sharedKafkaTestResource.getKafkaTestUtils();
        final TransactionalProducer transactionalProducer = new TransactionalProducer(new KafkaProducerSupplier<>(createOverridingProducerTransactionalProperties()));
        sendMessages(transactionalProducer);
        final List<ConsumerRecord<byte[], byte[]>> consumerRecords = kafkaTestUtils.consumeAllRecordsFromTopic(topicName);
        logger.info("consumer record count: {}", consumerRecords.size());
        assertThat(consumerRecords.size() == TASK_COUNT * MESSAGE_COUNT);
    }

    @Test
    public void testTransactionalProducerWithMockSupplier() {
        final MockProducerSupplier supplier = new MockProducerSupplier<>();
        final TransactionalProducer transactionalProducer = new TransactionalProducer(supplier);
        sendMessages(transactionalProducer);
        logger.info("consumer record count: {}", supplier.getCountSentMessages());
        assertThat(supplier.getCountSentMessages() == TASK_COUNT * MESSAGE_COUNT);
    }

    private void sendMessages(TransactionalProducer transactionalProducer) {
        try {
            ExecutorService executorService = Executors.newFixedThreadPool(THREAD_COUNT);
            CountDownLatch latch = new CountDownLatch(TASK_COUNT);
            executorService.invokeAll(getCallables(TASK_COUNT, transactionalProducer, latch));
            latch.await();
            executorService.shutdown();
        } catch (InterruptedException e) {
            logger.error("error: {}", e.getMessage());
        }
    }

    private List<Callable<Void>> getCallables(Integer callableCount, TransactionalProducer transactionalProducer, CountDownLatch latch) {
        return Collections.nCopies(callableCount, () -> {
            try {
                transactionalProducer.beginTransaction();
                simulateWork(transactionalProducer, topicName);
                transactionalProducer.commitTransaction();
                logger.info("{} done with task...", Thread.currentThread().getName());
                latch.countDown();
            } catch (Exception e) {
                transactionalProducer.abortTransaction();
                logger.error("KafkaException thrown! Message: {}", e.getMessage());
            }
            return null;
        });
    }

    private static void simulateWork(TransactionalProducer transactionalProducer, String topicName) throws InterruptedException {
        Thread.sleep(1000L);
        for (int i = 0; i < MESSAGE_COUNT; i++) {
            transactionalProducer.send(new ProducerRecord<>(topicName, Thread.currentThread().getName() + " message " + i));
        }
    }

    private Properties createOverridingProducerTransactionalProperties() {
        Properties producerProperties = new Properties();
        producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, sharedKafkaTestResource.getKafkaConnectString());
        producerProperties.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaExampleProducer");
        producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProperties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        producerProperties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "prod");
        producerProperties.put(ProducerConfig.ACKS_CONFIG, "all");
        return producerProperties;
    }

    private KafkaTestUtils getKafkaTestUtils() {
        return sharedKafkaTestResource.getKafkaTestUtils();
    }

}