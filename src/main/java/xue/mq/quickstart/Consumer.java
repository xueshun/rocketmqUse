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
 * 
 * @author Administrator
 *
 */
public class Consumer {
	public static void main(String[] args) throws MQClientException {
		DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("quickstart_consumer");
		consumer.setNamesrvAddr("192.168.1.220:9876;192.168.1.221:9876");
		
		/**
		 * 设置Consumer第一次启动是从队列头部开始消费还是从队列尾部开始消费
		 * 如果非第一次启动，那么按照上一次消费的位置继续消费
		 */
		consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
		
		//批量消费，一次消费多少条消息默认为1条  MQPushConsumer 不支持批量处理
		consumer.setConsumeMessageBatchMaxSize(10);
		
		//批量拉取消息，一次最多拉多少条，默认为32条
		//consumer.setPullBatchSize(32); 这个对应的是PushConsumer
		
		consumer.subscribe("TopicQuickStart", "*");
		
		
		consumer.registerMessageListener(new MessageListenerConcurrently() {
			
			public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, 
					ConsumeConcurrentlyContext context) {
				//System.out.println(Thread.currentThread().getName() + "Receive New Messages: " +msgs);
				System.out.println(msgs.size());
				
				try {
					for (MessageExt msg : msgs) {
						String topic = msg.getTopic();
						String msgBody = new String(msg.getBody(),"utf-8");
						String tags = msg.getTags();
						System.out.println("收到消息：" + "topic : " + topic +" , tags : " +tags + ",msg " + msgBody );
					}
				} catch (Exception e) {
					e.printStackTrace();
					//如果消费失败，等一会rocketmq 继续想消费者发送消息
					return ConsumeConcurrentlyStatus.RECONSUME_LATER;
				}
				
				//rocketmq的一个好处就是消费消息是否成功会返回一个标志
				return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
			}
		});
		
		consumer.start();
		
		System.out.println("Consumer Started.");
		
	}
}
