package xue.mq.pullConsumer;

import java.util.List;

import com.alibaba.rocketmq.client.consumer.MQPullConsumer;
import com.alibaba.rocketmq.client.consumer.MQPullConsumerScheduleService;
import com.alibaba.rocketmq.client.consumer.PullResult;
import com.alibaba.rocketmq.client.consumer.PullTaskCallback;
import com.alibaba.rocketmq.client.consumer.PullTaskContext;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.rocketmq.common.message.MessageQueue;
import com.alibaba.rocketmq.common.protocol.heartbeat.MessageModel;

public class PullScheduleService {
	public static void main(String[] args) throws MQClientException {
		
		String group_name = "schedule_consume";
		
		final MQPullConsumerScheduleService scheduleService = new MQPullConsumerScheduleService(group_name);
		
		scheduleService.getDefaultMQPullConsumer().setNamesrvAddr("192.168.1.220:9876:192.168.1.221:9876");
		
		scheduleService.setMessageModel(MessageModel.CLUSTERING);
		
		scheduleService.registerPullTaskCallback("TopicPull", new PullTaskCallback() {
			
			public void doPullTask(MessageQueue mq, PullTaskContext context) {
				MQPullConsumer consumer = context.getPullConsumer();
				try {
					//获取从哪里拉取
					long offset = consumer.fetchConsumeOffset(mq, false);
					if(offset < 0){
						offset = 0;
					}
					
					PullResult pullResult = consumer.pull(mq, "*", offset, 32);
					switch (pullResult.getPullStatus()) {
					case FOUND:
						List<MessageExt> list = pullResult.getMsgFoundList();
						for (MessageExt msg : list) {
							try {
								//做一些具体的业务
							} catch (Exception e) {
								//出现异常未消费的数据做持久化处理 保存到数据库中
								continue;
							}
							
							//消费数据
							System.out.println(new String(msg.getBody()));
						}
						break;
					case NO_MATCHED_MSG:
						break;
					case NO_NEW_MSG:
						break;
					case OFFSET_ILLEGAL:
						break;
					default:
						break;
					}
					
					//存储Offset，客户端每个5s会定时刷新到Broker
					consumer.updateConsumeOffset(mq, pullResult.getNextBeginOffset());
					
					//设置再过10s后重新拉取
					context.setPullNextDelayTimeMillis(2000);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		});
		
		scheduleService.start();
	}
	
}
