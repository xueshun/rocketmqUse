package xue.mq.quickstart;

import java.util.List;

import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.consumer.ConsumeFromWhere;
import com.alibaba.rocketmq.common.message.MessageExt;

/**
 * Consumer，订阅消息
 * @author Administrator
 *
 */
public class Consumer {
	public static void main(String[] args) throws MQClientException {
		DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("quickstart_consumer");
		consumer.setNamesrvAddr("192.168.1.220:9876;192.168.1.221:9876");
		
		/**
		 * 设置Consumer第一次启动时从队列头部分消费 还是从队列的尾部开始消费
		 * 如果非第一次启动，那么就按照上次消费的位置继续消费
		 */
		consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
		
		consumer.subscribe("TopicQuickStart", "*");
		
		consumer.registerMessageListener(new MessageListenerConcurrently() {
			
			public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, 
					ConsumeConcurrentlyContext context) {
				System.out.println(Thread.currentThread().getName() + "Receive New Messages: " +msgs);
				
				return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
			}
		});
		
		consumer.start();
		
		System.out.println("Consumer Started.");
		
	}
}
