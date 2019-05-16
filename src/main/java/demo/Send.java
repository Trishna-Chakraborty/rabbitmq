package demo;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class Send {

    private final static String QUEUE_NAME = "hello";

    public static void main(String[] argv) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {
            Map<String, Object> mapArgs = new HashMap<String, Object>();
            mapArgs.put("x-expires", 15000);
            channel.queueDeclare(QUEUE_NAME, false, false, false,mapArgs);
            String message = "Hello World!";
            channel.basicPublish("", QUEUE_NAME, null, message.getBytes("UTF-8"));
            System.out.println(" [x] Sent '" + message + "'");
            while (true){

            }
            /*try {

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
            }*/
        }
    }
}