package xue.mq.producer.order;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import com.alibaba.rocketmq.client.producer.DefaultMQProducer;
import com.alibaba.rocketmq.client.producer.MessageQueueSelector;
import com.alibaba.rocketmq.client.producer.SendResult;
import com.alibaba.rocketmq.common.message.Message;
import com.alibaba.rocketmq.common.message.MessageQueue;

public class Producer {

	public static void main(String[] args) {
		try {
			String group_name = "order_producer";

			DefaultMQProducer producer = new DefaultMQProducer(group_name);

			producer.setNamesrvAddr("192.168.1.220:9876;192.168.1.221:9876");

			producer.start();

			//String[] tags = new String[] {"TagA" , "TagC" ,"TagD"};

			Date date = new Date();

			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

			String dateStr = sdf.format(date);

			for (int i = 0; i < 5; i++) {

				//时间戳
				String body = dateStr + "Order_01 " +i;

				//参数： topic tag message
				Message msg = new Message("TopicTest", 
						"order01", 
						"KEY" + i,
						body.getBytes());

				//发送数据：如果使用顺序消费，则必须自己实现MessageQueueSelector,保证消息进入同一个队列中去
				SendResult sendresult = producer.send(msg,new MessageQueueSelector() {

					public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
						Integer id = (Integer) arg;
						return mqs.get(id);
					}
				},0); //0 位topic主题下的queue(队列)的下标 
				System.out.println(sendresult + ",body" +body);
			}

			for (int i = 0; i < 5; i++) {

				//时间戳
				String body = dateStr + "Order_02 " +i;

				//参数： topic tag message
				Message msg = new Message("TopicTest", 
						"order02", 
						"KEY" + i,
						body.getBytes());

				//发送数据：如果使用顺序消费，则必须自己实现MessageQueueSelector,保证消息进入同一个队列中去
				SendResult sendresult = producer.send(msg,new MessageQueueSelector() {

					public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
						Integer id = (Integer) arg;
						return mqs.get(id);
					}
				},1); //0 位topic主题下的queue(队列)的下标 
				System.out.println(sendresult + ",body" +body);
			}
			for (int i = 0; i < 5; i++) {

				//时间戳
				String body = dateStr + "Order_03 " +i;

				//参数： topic tag message
				Message msg = new Message("TopicTest", 
						"order03", 
						"KEY" + i,
						body.getBytes());

				//发送数据：如果使用顺序消费，则必须自己实现MessageQueueSelector,保证消息进入同一个队列中去
				SendResult sendresult = producer.send(msg,new MessageQueueSelector() {

					public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
						Integer id = (Integer) arg;
						return mqs.get(id);
					}
				},2); //0 位topic主题下的queue(队列)的下标 
				System.out.println(sendresult + ",body" +body);
			}
			for (int i = 0; i < 5; i++) {

				//时间戳
				String body = dateStr + "Order_04 " +i;

				//参数： topic tag message
				Message msg = new Message("TopicTest", 
						"order04", 
						"KEY" + i,
						body.getBytes());

				//发送数据：如果使用顺序消费，则必须自己实现MessageQueueSelector,保证消息进入同一个队列中去
				SendResult sendresult = producer.send(msg,new MessageQueueSelector() {

					public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
						Integer id = (Integer) arg;
						return mqs.get(id);
					}
				},3); //0 位topic主题下的queue(队列)的下标 
				System.out.println(sendresult + ",body" +body);
			}

			producer.shutdown();
		} catch (Exception e) {
			// TODO: handle exception
		}
	}
}
