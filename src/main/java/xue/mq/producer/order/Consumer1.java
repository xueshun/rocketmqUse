package xue.mq.producer.order;

import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;

import com.alibaba.rocketmq.client.consumer.listener.ConsumeOrderlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;

import com.alibaba.rocketmq.client.consumer.listener.MessageListenerOrderly;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.consumer.ConsumeFromWhere;
import com.alibaba.rocketmq.common.message.MessageExt;

public class Consumer1 {
	
	public Consumer1() throws MQClientException{
		String group_name= "order_consumer";
		
		DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(group_name);
		
		consumer.setNamesrvAddr("192.168.1.220:9876;192.168.1.221:9876");
		
		/**
		 * 设置Consumer第一次启动是从队列头部开始消费还是从尾部开始消费
		 * 如果非第一次启动，那么按照上次消费的位置继续消费
		 */
		consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
		
		//订阅的主题
		consumer.subscribe("TopicTest", "*");
		
		
		//注册监听
		consumer.registerMessageListener(new Listener());
		consumer.start();
		System.out.println("C1 start ....");
	}
	
	class Listener implements MessageListenerOrderly{
		private Random random = new Random();
		public ConsumeOrderlyStatus consumeMessage(List<MessageExt> msgs, ConsumeOrderlyContext context) {
			//设置自动提交
			context.setAutoCommit(true);
			
			for (MessageExt msg : msgs) {
				System.out.println(msg +", content: " + new String(msg.getBody()));
			}
			try {
				//模拟业务逻辑处理中...
				TimeUnit.SECONDS.sleep(random.nextInt(5));
			} catch (Exception e) {
				e.printStackTrace();
			}
			return ConsumeOrderlyStatus.SUCCESS;
		}
	}
	
	public static void main(String[] args) throws MQClientException {
		Consumer1 c = new Consumer1();
	}
}
