package xue.mq.message.model;

import java.util.List;

import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.client.consumer.rebalance.AllocateMessageQueueAveragely;
import com.alibaba.rocketmq.client.consumer.rebalance.AllocateMessageQueueAveragelyByCircle;
import com.alibaba.rocketmq.client.consumer.rebalance.AllocateMessageQueueByConfig;
import com.alibaba.rocketmq.client.consumer.rebalance.AllocateMessageQueueByMachineRoom;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.rocketmq.common.protocol.heartbeat.MessageModel;

/**
 * Rocketmq 要先启动消费端，再启动生产端，
 * 			如果要先启动生产端，在启动消费端
 * 				出现的情况 1. 如果有一个consumer 及时的处理了消息，其他的Consumer 就不会再接收到消息
 * 						  2. 如果有一个Consumer接收到了消息，处理消息的时候出现了异常，没有返回给Broker
 * 							 处理标志，那么其他的Consumer,就会获取到该消息，进行处理 
 * @author Administrator
 *
 */
public class Consumer01 {

	public Consumer01(){
		try {
			String group_name = "message_consumer";

			DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(group_name);
			consumer.setNamesrvAddr("192.168.1.220:9876;192.168.1.221:9876");

			//订阅Topic01 主题 并且过滤 Tag01 Tag02 Tag03
			consumer.subscribe("Topic01", "Tag01 || Tag02 || Tag03");
			
			//广播模式下需要先启动Consumer 【默认的为集群消费】
			/**
			 * 广播消费 和 集群消费 模式的区别
			 * 		广播消费 ： 生产数据 所有的 消费端 消费相同的生产端生产的数据 (一个生产端生产100条消息，两个消费端，每个消费端会处理100条数据)
			 * 		集群消费 ： 生产端生产的数据 由消费端一起消费例如:(生产端生产100条消息，两个消费端会每个处理50条) 
			 */
			consumer.setMessageModel(MessageModel.BROADCASTING);
			consumer.registerMessageListener(new Listener());

			/* 权重策略
			权重
			AllocateMessageQueueAveragely;
			权重循环
			AllocateMessageQueueAveragelyByCircle;
			AllocateMessageQueueByConfig;
			AllocateMessageQueueByMachineRoom
			 */
			consumer.start();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	class Listener implements MessageListenerConcurrently{

		public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
			try{
				for (MessageExt msg : msgs) {
					String topic =msg.getTopic();
					String msgBody = new String(msg.getBody(),"utf-8");
					String tags = msg.getTags();
					System.out.println("收到信息：" + " topic :" + topic +" ,tags" + tags +" , msg" +msgBody);
					//System.out.println("==========开始暂停========");
					//Thread.sleep(60000);

				}
			}catch (Exception e) {
				e.printStackTrace();
				//消费者处理失败的情况  返回给Broker的标志
				return ConsumeConcurrentlyStatus.RECONSUME_LATER;
			}
			//消费者处理成功的情况  返回给Broker的标志
			return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
		}

	}

	public static void main(String[] args) {
		Consumer01 c01 = new Consumer01();
		System.out.println("c01 start .... ");
	}
}
