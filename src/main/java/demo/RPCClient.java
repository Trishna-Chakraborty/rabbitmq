package demo;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeoutException;

public class RPCClient implements AutoCloseable {

    public Connection connection;
    public Channel channel;
    private static String requestQueueName1 = "rpc_queue1";
    private static String requestQueueName2 = "rpc_queue2";



    public RPCClient() throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        connection = factory.newConnection();
        channel = connection.createChannel();

    }

    public static void main(String[] argv) {
        try (RPCClient fibonacciRpc = new RPCClient()) {
            String replyQueueName="REPLY_QUEUE";
            fibonacciRpc.channel.queueDeclare(replyQueueName, false, false, false, null);
            //fibonacciRpc.channel.queueDeclare().getQueue();;

            for (int i = 0; i < 20; i++) {
                String i_str = Integer.toString(i);
                System.out.println(" [x] Requesting fib(" + i_str + ")" + " to server 1");
                String response1 = fibonacciRpc.call(i_str,requestQueueName1,replyQueueName);
                System.out.println(" [.] Got '" + response1 + "'" +" from server 1 ");

                System.out.println(" [x] Requesting fib(" + response1 + ")" + " to server 2");
                String response2 = fibonacciRpc.call(response1,requestQueueName2,replyQueueName);
                System.out.println(" [.] Got '" + response2 + "'" +" from server 2 ");

            }
        } catch (IOException | TimeoutException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    public String call(String message, String requestQueueName,String replyQueueName) throws IOException, InterruptedException {
        final String corrId = UUID.randomUUID().toString();

        //replyQueueName =channel.queueDeclare().getQueue();
        //System.out.println("Reply Queue name " + replyQueueName + corrId);
        AMQP.BasicProperties props = new AMQP.BasicProperties
                .Builder()
                .correlationId(corrId)
                .replyTo(replyQueueName)
                .deliveryMode(2)
                .build();

        channel.basicPublish("", requestQueueName, props,message.getBytes("UTF-8"));

        final BlockingQueue<String> response = new ArrayBlockingQueue<>(1);

        String ctag = channel.basicConsume(replyQueueName, true, (consumerTag, delivery) -> {
            if (delivery.getProperties().getCorrelationId().equals(corrId)) {
                response.offer(new String(delivery.getBody(), "UTF-8"));
            }
        }, consumerTag -> {
        });

        String result = response.take();
        channel.basicCancel(ctag);
        return result;
    }

    public void close() throws IOException {
        System.out.println("connection is closed");
        connection.close();
    }
}
