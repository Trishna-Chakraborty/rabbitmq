package demo;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

import java.io.IOException;
import java.util.concurrent.ExecutorService;

public class workerThread extends DefaultConsumer {

    //String name;
    Channel channel;
    String queue;
    ExecutorService executorService;

    public workerThread(int prefetch, ExecutorService threadExecutor, Channel c, String q) throws Exception {
        super(c);
        channel = c;
        queue = q;
        channel.basicQos(prefetch);
        channel.basicConsume(queue, false, this);
        executorService = threadExecutor;
    }

    public workerThread(Channel channel) {
        super(channel);
    }

    @Override
    public void handleDelivery(String consumerTag,
                               Envelope envelope,
                               AMQP.BasicProperties properties,
                               byte[] body) throws IOException {
        Runnable task = null;
        executorService.submit(task);
    }
}