package xue.mq.message.model;

import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.client.producer.DefaultMQProducer;
import com.alibaba.rocketmq.client.producer.SendResult;
import com.alibaba.rocketmq.common.message.Message;

/**
 * Rocketmq 自身支持负载均衡
 * @author Administrator
 *
 */
public class Producer {
	public static void main(String[] args) throws MQClientException, InterruptedException {
		
		String group_name = "message_producer";
		
		DefaultMQProducer producer = new DefaultMQProducer(group_name);
		
		//多master
		//producer.setNamesrvAddr("192.168.1.220:9876;192.168.1.221:9876");
		
		//多master 多slave
		producer.setNamesrvAddr("192.168.1.220:9876;192.168.1.221:9876;192.168.1.222:9876;192.168.1.223:9876");
		producer.start();
		
		//测试发送一条数据
		
		for (int i = 1; i <= 100; i++) {
			try {
				Message  msg = new Message("Topic01",
						"Tag01", 
						("消息内容" + i).getBytes() );
				
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
