package xue.mq.filter;

import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.client.producer.DefaultMQProducer;
import com.alibaba.rocketmq.client.producer.SendResult;
import com.alibaba.rocketmq.common.message.Message;

public class Producer {
	public static void main(String[] args) throws MQClientException {
		
		String group_name = "filter_producer";
		DefaultMQProducer producer = new DefaultMQProducer(group_name);
		
		producer.setNamesrvAddr("192.168.1.220:9876;192.168.1.221:9876");
		producer.start();
		
		try {
			for (int i = 0; i < 100; i++) {
				Message msg = new Message("TopicFilter7",
						"TagA",
						"OrderID001",//key
						("Hello RocketMQ" + i).getBytes());
				msg.putUserProperty("SequenceId", String.valueOf(i));
				
				SendResult sendResult = producer.send(msg);
				System.out.println(sendResult);
				
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		producer.shutdown();
	}
}
