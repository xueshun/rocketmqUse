package xue.mq.message.model;

import java.util.List;

import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.rocketmq.common.protocol.heartbeat.MessageModel;

public class Consumer02 {
	public Consumer02(){
		try {
			String group_name = "message_consumer";
			
			DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(group_name);
			consumer.setNamesrvAddr("192.168.1.220:9876;192.168.1.221:9876");
			
			//订阅Topic01 主题 并且过滤 Tag01 Tag02 Tag03
			consumer.subscribe("Topic01", "Tag01 || Tag02 || Tag03");
			
			//广播模式下需要先启动Consumer 【默认的为集群消费】
			consumer.setMessageModel(MessageModel.BROADCASTING);
			consumer.registerMessageListener(new Listener());
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
		Consumer02 c02 = new Consumer02();
		System.out.println("c02 start .... ");
	}
}
