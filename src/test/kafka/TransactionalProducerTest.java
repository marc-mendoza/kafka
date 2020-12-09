package kafka;

import com.salesforce.kafka.test.KafkaTestUtils;
import com.salesforce.kafka.test.junit4.SharedKafkaTestResource;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
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

    private KafkaTestUtils kafkaTestUtils;

    @Before
    public void beforeTest() {
        // Generate topic name
        topicName = getClass().getSimpleName() + Clock.systemUTC().millis();

        // Create topic with 3 partitions,
        // NOTE: This will create partition ids 0 thru 2, because partitions are indexed at 0 :)
        getKafkaTestUtils().createTopic(topicName, 1, (short) 1);
        kafkaTestUtils = sharedKafkaTestResource.getKafkaTestUtils();
    }

    @Test
    public void testTransactionalProducerSendMessagesWithMockSupplier() {
        final MockProducerSupplier supplier = new MockProducerSupplier<>();
        final TransactionalProducer transactionalProducer = new TransactionalProducer(supplier);
        sendMessages(transactionalProducer, THREAD_COUNT, TASK_COUNT);
        assertThat(supplier.getCountSentMessages()).isEqualTo(TASK_COUNT * MESSAGE_COUNT);
    }

    @Test
    public void testTransactionalProducerSingleThread() {
        final KafkaProducerSupplier<Object, Object> supplier = new KafkaProducerSupplier<>(createOverridingProducerTransactionalProperties());
        final TransactionalProducer transactionalProducer = new TransactionalProducer(supplier);
        transactionalProducer.beginTransaction();
        transactionalProducer.send(new ProducerRecord<>(topicName, Thread.currentThread().getName() + " message " + 1));
        transactionalProducer.send(new ProducerRecord<>(topicName, Thread.currentThread().getName() + " message " + 2));
        transactionalProducer.commitTransaction();
        final Consumer consumer = createConsumer(createOverridingConsumerTransactionalProperties());
        final ConsumerRecords consumerRecords = consumer.poll(Duration.ofSeconds(5L));
        assertThat(consumerRecords.count()).isEqualTo(2);
    }

    @Test
    public void testTransactionalProducerMultithreaded() {
        final TransactionalProducer transactionalProducer = new TransactionalProducer(new KafkaProducerSupplier<>(createOverridingProducerTransactionalProperties()));
        sendMessages(transactionalProducer, THREAD_COUNT, TASK_COUNT);
        final List<ConsumerRecord<byte[], byte[]>> consumerRecords = kafkaTestUtils.consumeAllRecordsFromTopic(topicName);
        assertThat(consumerRecords.size()).isEqualTo(TASK_COUNT * MESSAGE_COUNT);
    }

    @Test
    public void testTransactionalProducerMultipleBatch() {
        final KafkaProducerSupplier<Object, Object> producerSupplier = new KafkaProducerSupplier<>(createOverridingProducerTransactionalProperties());
        final TransactionalProducer transactionalProducer = new TransactionalProducer(producerSupplier);

        sendMessages(transactionalProducer, THREAD_COUNT, TASK_COUNT);
        sendMessages(transactionalProducer, THREAD_COUNT + 2, TASK_COUNT);

        final List<ConsumerRecord<byte[], byte[]>> consumerRecords = kafkaTestUtils.consumeAllRecordsFromTopic(topicName);
        logger.info("consumer record count: {}", consumerRecords.size());
        logger.info("producer count: {}", producerSupplier.getProducerCount());
        assertThat(consumerRecords.size()).isEqualTo(TASK_COUNT * MESSAGE_COUNT * 2);
        assertThat(producerSupplier.getProducerCount()).isEqualTo(THREAD_COUNT + 2);
    }

    @Test
    public void testTransactionalProducerWithFailures() {
        final KafkaProducerSupplier<Object, Object> producerSupplier = new KafkaProducerSupplier<>(createOverridingProducerTransactionalProperties());
        final TransactionalProducer transactionalProducer = new TransactionalProducer(producerSupplier);

        sendMessagesWithFailures(transactionalProducer, THREAD_COUNT, TASK_COUNT, 20);

        final Consumer consumer = createConsumer(createOverridingConsumerTransactionalProperties());
        final ConsumerRecords consumerRecords = consumer.poll(Duration.ofSeconds(5L));
        logger.info("consumer record count: {}", consumerRecords.count());
        logger.info("producer count: {}", producerSupplier.getProducerCount());

        // consumer is reading committed records only so count should still be verifiable
        assertThat(consumerRecords.count()).isEqualTo(TASK_COUNT * MESSAGE_COUNT);
        assertThat(producerSupplier.getProducerCount()).isEqualTo(THREAD_COUNT + 2);
    }

    private void sendMessages(TransactionalProducer transactionalProducer, int threadCount, int taskCount) {
        try {
            ExecutorService executorService = Executors.newFixedThreadPool(threadCount);
            CountDownLatch latch = new CountDownLatch(taskCount);
            executorService.invokeAll(getCallables(taskCount, transactionalProducer, latch));
            latch.await();
            executorService.shutdown();
        } catch (InterruptedException e) {
            logger.error("error: {}", e.getMessage());
        }
    }

    private void sendMessagesWithFailures(TransactionalProducer transactionalProducer, int threadCount, int taskCount, int failureCount) {
        try {
            ExecutorService executorService = Executors.newFixedThreadPool(threadCount);
            CountDownLatch latch = new CountDownLatch(taskCount);
            final List<Callable<Void>> callables = new ArrayList<>(getCallables(taskCount, transactionalProducer, latch));
            callables.addAll(getFailingCallables(failureCount, transactionalProducer, latch));
            Collections.shuffle(callables);
            executorService.invokeAll(callables);
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
            } catch (Exception e) { // transaction should be aborted for ANY reason
                transactionalProducer.abortTransaction();
                logger.error("Exception thrown! Message: {}", e.getMessage());
            }
            return null;
        });
    }

    private List<Callable<Void>> getFailingCallables(Integer callableCount, TransactionalProducer transactionalProducer, CountDownLatch latch) {
        return Collections.nCopies(callableCount, () -> {
            try {
                transactionalProducer.beginTransaction();
                simulateWork(transactionalProducer, topicName);
                logger.info("{} done with failing task...", Thread.currentThread().getName());
                transactionalProducer.abortTransaction();
                latch.countDown();
            } catch (Exception e) {
                transactionalProducer.abortTransaction();
                logger.error("KafkaException thrown! Thread name: {} Message: {}", Thread.currentThread().getName(), e.getMessage());
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

    private Consumer createConsumer(Properties properties) {
        final Consumer consumer = new KafkaConsumer(properties);
        consumer.subscribe(Collections.singleton(topicName));
        return consumer;
    }

    // properties for a consumer that can only retrieve committed messages
    private Properties createOverridingConsumerTransactionalProperties() {
        Properties consumerProperties = new Properties();
        consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, sharedKafkaTestResource.getKafkaConnectString());
        consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        consumerProperties.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, "consumer-group");
        consumerProperties.put(ConsumerConfig.CLIENT_ID_CONFIG, "consumer-id");
        consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProperties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "999999");
        consumerProperties.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, "999999999");
        consumerProperties.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, "999999999");
        return consumerProperties;
    }

    private KafkaTestUtils getKafkaTestUtils() {
        return sharedKafkaTestResource.getKafkaTestUtils();
    }

}
