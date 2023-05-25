package com.javatechie.spring.kafka.api;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.PartitionOffset;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@SpringBootApplication
@RestController
public class KafkaConsumerApplication {

	private static final Logger logger = LoggerFactory.getLogger(KafkaConsumerApplication.class);

	private final String initOffset = "26";

	List<String> messages = new ArrayList<>();

	User userFromTopic = null;

	@GetMapping("/consumeStringMessage")
	public List<String> consumeMsg() {
		return messages;
	}

	@GetMapping("/consumeJsonMessage")
	public User consumeJsonMessage() {
		return userFromTopic;
	}

	@KafkaListener(groupId = "group-str", topics = "testTopicStr", containerFactory = "kafkaListenerContainerFactory")
	public List<String> getMsgFromTopic(String data) {
		logger.info(data);
		messages.add(data);
		return messages;
	}

	@KafkaListener(groupId = "group-json", topics = "testTopicJson", containerFactory = "userKafkaListenerContainerFactory")
	public User getJsonMsgFromTopic(User user) {
		logger.info(user.toString());
		userFromTopic = user;
		return userFromTopic;
	}

	// Debezium kafka connector
	@KafkaListener(groupId = "group-debezium", topics = "dbserver1.inventory.customers", containerFactory = "kafkaListenerContainerFactory")
	public void getMsgFromDatabaseTopic(@Payload String data,
										@Header(KafkaHeaders.RECEIVED_PARTITION) String partition,
										@Header(KafkaHeaders.OFFSET) String offset) {
		logger.info("partition:{}, offset:{}", partition, offset);
		logger.info(data);
	}


	// Debezium kafka connector with manual ACK (another thread to get same messages)
	//@KafkaListener(groupId = "group-debezium-ack",
	//		topics = "dbserver1.inventory.customers",
	//topicPartitions = @TopicPartition(topic = "dbserver1.inventory.customers",
	//	partitionOffsets = {@PartitionOffset(partition = "0", initialOffset = initOffset)}),
	//		containerFactory = "ackKafkaListenerContainerFactory")
	public void getMsgManualACKTopic(@Payload String data,
									 @Header(KafkaHeaders.RECEIVED_PARTITION) String partition,
									 @Header(KafkaHeaders.OFFSET) String offset,
									 Acknowledgment acknowledgment) {
		logger.info("Manual ACK ---> partition:{}, offset:{}", partition, offset);
		try {
			// business logic, then commit
			logger.info("offset:{} --> {} ", offset, data);
			acknowledgment.acknowledge();
			// save offset to a file or DB
			logger.info("Save offset:{}", offset);
		} catch (Exception e){
			logger.error(e.getMessage());
		}
	}

	public static void main(String[] args) {
		SpringApplication.run(KafkaConsumerApplication.class, args);
	}
}
