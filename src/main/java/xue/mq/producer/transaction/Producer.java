package xue.mq.producer.transaction;

import java.util.concurrent.TimeUnit;

import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.client.producer.LocalTransactionState;
import com.alibaba.rocketmq.client.producer.SendResult;
import com.alibaba.rocketmq.client.producer.TransactionCheckListener;
import com.alibaba.rocketmq.client.producer.TransactionMQProducer;
import com.alibaba.rocketmq.common.message.Message;
import com.alibaba.rocketmq.common.message.MessageExt;

public class Producer {
	
	public static void main(String[] args) throws MQClientException, InterruptedException {
		/**
		 * 一个应用创建一个Producer,由应用来维护次对象，可以设置为全局对象或者单例
		 * 注意：ProducerGroupName 需要由引用来保证唯一，一类Producer集合的名称，这类Producer通常发送一类消息，且发送逻辑一致
		 * ProducerGroup这个概念发送普通的消息时，作用不大，但是发送分布式事务消息时，比较关键
		 * 因为服务器会回查这个Group下的任意Producer
		 */
		
		String group_name ="transaction_producer";
		
		final TransactionMQProducer producer = new TransactionMQProducer(group_name);
		
		//nameServer服务
		producer.setNamesrvAddr("192.168.1.220:9876;192.168.1.221:9876");
		//事务回查最小并发数
		producer.setCheckThreadPoolMinSize(5);
		//事务回查最大并发数
		producer.setCheckThreadPoolMaxSize(20);
		//队列数
		producer.setCheckRequestHoldMax(2000);
		
		/**
		 * Producer对象在使用之前必须要调用start初始化，初始化一次即可
		 * 注意：切记不可以在每次发送消息时，都调用start方法。
		 */
		producer.start();
		
		//服务器回调producer,检查本地事务分支成功还是失败
		producer.setTransactionCheckListener(new TransactionCheckListener() {
			public LocalTransactionState checkLocalTransactionState(MessageExt msg) {
				//msg.key
				// 查询数据库 看一下 是否是有的
				System.out.println("state---"+ new String(msg.getBody()));
				return LocalTransactionState.COMMIT_MESSAGE;
			}
		});
		
		/**
		 * 下面这段代码表明一个Producer对象可以发送对个topic，多个tag消息。
		 * 注意：send方法是同步调用，只要不抛异常就标识成功，但是发送成功也可能会有多种状态
		 * 例如：消息写入Master成功，但是Slave不成功。这种情况消息属于成功，但是对于个别应用，如果
		 *       对消息可靠性要求极高，需要对这种情况进行处理。另外，消息可能会存在发送失败的情况，
		 *       失败重试由应用来处理。
		 */
		TransactionExecterImpl tranExecuter = new TransactionExecterImpl();
		
		for (int i = 0; i < 3; i++) {
			try {
				Message msg = new Message("TopicTransaction", 
						"Transaction" + i, //tag 
						"key", //key消息关键词，多个key用MessageConst.KEY_SEPARATOR隔开(查询消息使用) 
						("Hello Rocket" + i).getBytes());
				SendResult sendResult = producer.sendMessageInTransaction(msg, tranExecuter, "tq");
				System.out.println(sendResult);
			} catch (Exception e) {
				e.printStackTrace();
			}
			TimeUnit.MILLISECONDS.sleep(1000);
		}
		
		/**
		 * 应用退出时，要调用shutdown来清理资源，关闭网络连接，从MetaQ服务器上注销自己
		 * 注意：建议应用在JBOSS,Tomcat等容器的退出钩子里调用shutdown方法
		 */
		
		Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
			public void run() {
				producer.shutdown();
			}
		}));
		
		System.exit(0);
	}
}
