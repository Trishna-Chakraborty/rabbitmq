package demo;

import java.util.HashMap;
import java.util.Map;
import java.io.IOException;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.*;

public class ConsumerCancel {

    private static final String QUEUE_NAME = "hello";

    public static void main(String[] args) throws java.io.IOException,
            java.lang.InterruptedException, TimeoutException {

        ConnectionFactory factory = new ConnectionFactory();
        //factory.setConnectionTimeout(60000);
        Map<String,Object> map=new HashMap<>();
        Map<String, Object> capabilities = new HashMap<String, Object>();
        capabilities.put("consumer_cancel_notify", true);
        map.put("capabilities", capabilities);
        factory.setClientProperties(map);

        factory.setHost("localhost");

        Connection connection = factory.newConnection();
        connection.addBlockedListener(new BlockedListener() {
            public void handleBlocked(String reason) throws IOException {
                System.out.println("connection is blocked");
            }

            public void handleUnblocked() throws IOException {
                System.out.println("connection is unblocked");
            }
        });
        Channel channel = connection.createChannel();
        channel.basicQos(1);

        Map<String, Object> mapArgs = new HashMap<String, Object>();
        mapArgs.put("x-expires", 15000);
        channel.queueDeclare(QUEUE_NAME, false, false, false,mapArgs);

        System.out.println(" [*] Waiting for messages. To exit press CTRL+C");



        try {


            channel.basicConsume(QUEUE_NAME, false,
                    new DefaultConsumer(channel) {

                        @Override
                        public void handleDelivery(String consumerTag,
                                                   Envelope envelope,
                                                   AMQP.BasicProperties properties, byte[] body)
                                throws IOException {

                            long deliveryTag = envelope.getDeliveryTag();

                            String message = new String(body);
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
    }

}