package demo;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeoutException;

public class RPCClient implements AutoCloseable {

    public Connection connection;
    public Channel channel;
    private static String requestQueueName1 = "one";
    private static String requestQueueName2 = "two";



    public RPCClient() throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        //factory.setChannelRpcTimeout(60000);
        /*factory.setConnectionTimeout(60000);
        factory.setRequestedHeartbeat(60);*/
        Map<String,Object> map=new HashMap<>();
        Map<String, Object> capabilities = new HashMap<String, Object>();
        capabilities.put("consumer_cancel_notify", true);
        map.put("capabilities", capabilities);
        factory.setClientProperties(map);
        System.out.println(factory.getChannelRpcTimeout() +" "+factory.getConnectionTimeout());

        factory.setHost("localhost");
        connection = factory.newConnection();
        channel = connection.createChannel();

    }

    public static void main(String[] argv) {
        try (RPCClient fibonacciRpc = new RPCClient()) {
            String replyQueueName="REPLY_QUEUE";
            Map<String, Object> mapArgs = new HashMap<String, Object>();
            mapArgs.put("x-expires", 15000);
            fibonacciRpc.channel.queueDeclare(replyQueueName, false, false, false, mapArgs);
            //fibonacciRpc.channel.queueDeclare().getQueue();;

            for (int i = 0; i < 6; i++) {
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

    public String call(String message, String requestQueueName,String replyQueueName) throws IOException, InterruptedException,ShutdownSignalException {
        final String corrId = UUID.randomUUID().toString();

        //replyQueueName =channel.queueDeclare().getQueue();
        //System.out.println("Reply Queue name " + replyQueueName + corrId);
        AMQP.BasicProperties props = new AMQP.BasicProperties
                .Builder()
                .correlationId(corrId)
                .replyTo(replyQueueName)
                .deliveryMode(1)
                .build();

        channel.basicPublish("", requestQueueName, props,message.getBytes("UTF-8"));

        final BlockingQueue<String> response = new ArrayBlockingQueue<>(1);

       /*String ctag = channel.basicConsume(replyQueueName, true, (consumerTag, delivery) -> {
            if (delivery.getProperties().getCorrelationId().equals(corrId)) {
                response.offer(new String(delivery.getBody(), "UTF-8"));
            }
        }, consumerTag -> {
        });*/

        try {


            channel.basicConsume(replyQueueName, false,
                    new DefaultConsumer(channel) {

                        @Override
                        public void handleDelivery(String consumerTag,
                                                   Envelope envelope,
                                                   AMQP.BasicProperties properties, byte[] body)
                                throws IOException {

                            long deliveryTag = envelope.getDeliveryTag();
                            if(properties.getCorrelationId().equals(corrId)){
                                response.offer(new String(body, "UTF-8"));
                            }
                            System.out.println(" [x] Received '" + message
                                    + "'");

                            channel.basicAck(deliveryTag, false);

                        }


                        @Override
                        public void handleCancel(String consumerTag) throws IOException {
                            System.out.println("Cancelled");

                        }

                    });
        } catch (IOException e) {
            e.printStackTrace();
        }
        String result = response.take();
        //channel.basicCancel(ctag);
        return result;




    }

    public void close() throws IOException {
        System.out.println("connection is closed");
        connection.close();
    }
}
