package com.example.kafka.kafkaconsumer;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class ConsumerController {
	
	@KafkaListener(topics= "Topic1",groupId = "group_id")
	public void consumer(String message) 
	{
		System.out.println("message = " + message);
	}

}