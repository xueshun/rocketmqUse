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
 * Consumer��������Ϣ
 * @author Administrator
 *
 */
public class Consumer {
	public static void main(String[] args) throws MQClientException {
		DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("quickstart_consumer");
		consumer.setNamesrvAddr("192.168.1.220:9876;192.168.1.221:9876");
		
		/**
		 * ����Consumer��һ������ʱ�Ӷ���ͷ�������� ���ǴӶ��е�β����ʼ����
		 * ����ǵ�һ����������ô�Ͱ����ϴ����ѵ�λ�ü�������
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