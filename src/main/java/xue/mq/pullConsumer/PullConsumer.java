package xue.mq.pullConsumer;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.message.MessageQueue;

/**
 * PullConusmer 订阅消息
 * @author Administrator
 *http://lifestack.cn/archives/387.html
 */
public class PullConsumer {
	//Map<key,value> key为指定的队列，value为这个队列拉取数据的最后位置
	private static final Map<MessageQueue, Long> offseTable = new HashMap<MessageQueue, Long>();
	
	public static void main(String[] args) throws MQClientException {
		
		String group_name = "pull_consumer";
		DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(group_name);
		consumer.start();
		//从TopicTest这个主题曲获取所有队列(默认会有4个队列)
		Set<MessageQueue> mqs = consumer.fetchSubscribeMessageQueues("TopicTest");
		//遍历每个队列，进行拉取数据
		for (MessageQueue mq : mqs) {
			System.out.println("Consumer from the queue:" + mq);
			
			SINGLE_MQ: while(true){
				try {
					//从queue中获取数据，从什么位置开始拉取数据 单次最多拉取32条记录
				
				} catch (Exception e) {
					// TODO: handle exception
				}
			}
		}
	}
}
