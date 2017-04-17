package xue.mq.quickstart;

import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.client.producer.DefaultMQProducer;
import com.alibaba.rocketmq.client.producer.SendResult;
import com.alibaba.rocketmq.common.message.Message;

/**
 *
 * @author Administrator
 *
 */
public class Producer {
	public static void main(String[] args) throws MQClientException, InterruptedException {
		DefaultMQProducer producer = new DefaultMQProducer("quickstart_producer");
		producer.setNamesrvAddr("192.168.1.220:9876;192.168.1.221:9876");
		producer.start();
		
		for (int i = 0; i < 100; i++) {
			try {
				Message msg = new Message("TopicQuickStart", //topic
						"TagA", //tag
						("Hello RocketMQ" + i).getBytes()//body
						);
				SendResult sendResult = producer.send(msg);
				System.out.println(sendResult);
			} catch (Exception e) {
				e.printStackTrace();
				Thread.sleep(1000);
			}
		}
		
		producer.shutdown();
	}
}
