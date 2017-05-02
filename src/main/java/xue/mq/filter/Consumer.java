package xue.mq.filter;

import java.io.UnsupportedEncodingException;
import java.util.List;

import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.MixAll;
import com.alibaba.rocketmq.common.message.Message;
import com.alibaba.rocketmq.common.message.MessageExt;

public class Consumer {
	
	public static void main(String[] args) throws MQClientException {
		
		String group_name = "filter_consumer";
		DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(group_name);
		
		consumer.setNamesrvAddr("192.168.1.220:9876;192.168.1.221:9876");
		//使用java代码，在服务器做消息过滤
		String filterCode = MixAll.file2String("E:\\MyDownloads\\SourceCode\\rocketmqUse\\src\\main\\java\\xue\\mq\\filter\\MessageFilterImpl.java");
		//如果filterCode=null 是不正确的
		System.out.println(filterCode);
		consumer.subscribe("TopicFilter7", "xue.mq.filter.MessageFilterImpl", filterCode);
		
		//consumer.subscribe(topic, MessageFilterImpl.class.getCanonicalName());
		consumer.registerMessageListener(new MessageListenerConcurrently() {
			
			public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
				//System.out.println(Thread.currentThread().getName() + "Receive New Message:" + msgs);
				MessageExt me = msgs.get(0);
				try {
					System.out.println("收到信息：" + new String(me.getBody(),"utf-8"));
				} catch (UnsupportedEncodingException e) {
					e.printStackTrace();
				}
				return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
			}
		});
		/**
		 * Consumer对象在使用之前必须要调用start初始化，初始化一次即可
		 */
		consumer.start();
		
		System.out.println("Consumer Started ....");
	}

}
