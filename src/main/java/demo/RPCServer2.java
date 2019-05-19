package demo;

import com.rabbitmq.client.*;

import java.util.HashMap;
import java.util.Map;

public class RPCServer2 {

    private static final String RPC_QUEUE_NAME = "two";

    private static int fib(int n) {
        if (n == 0) return 0;
        if (n == 1) return 1;
        return fib(n - 1) + fib(n - 2);
    }

    public static void main(String[] argv) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");

        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {
            //channel.queueDeclare(RPC_QUEUE_NAME, false, false, false, null);
            //channel.queuePurge(RPC_QUEUE_NAME);
            channel.exchangeDeclare("dead_exchange", "direct");
            channel.queueDeclare("dead_queue", false, false, false, null);
            channel.queueBind("dead_queue", "dead_exchange", "");


            Map<String, Object> args = new HashMap<String, Object>();
            args.put("x-dead-letter-exchange", "dead_exchange");
            args.put("x-message-ttl", 60000);
            channel.queueDeclare(RPC_QUEUE_NAME, false, false, false, args);
            channel.exchangeDeclare(RPC_QUEUE_NAME+"_exchange", "direct");;
            channel.queueBind(RPC_QUEUE_NAME, RPC_QUEUE_NAME+"_exchange", "");


            channel.basicQos(1);

            System.out.println(" [x] Awaiting RPC requests");

            Object monitor = new Object();
            DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                AMQP.BasicProperties replyProps = new AMQP.BasicProperties
                        .Builder()
                        .correlationId(delivery.getProperties().getCorrelationId())
                        .build();

                String response = "";

                try {
                    System.out.println("here");
                    String message = new String(delivery.getBody(), "UTF-8");
                    int n = Integer.parseInt(message);

                    System.out.println(" [.] fib(" + message + ")");
                    response += fib(n);
                } catch (RuntimeException e) {
                    System.out.println(" [.] " + e.toString());
                } finally {
                    channel.basicPublish("", delivery.getProperties().getReplyTo(), replyProps, response.getBytes("UTF-8"));
                    channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
                    // RabbitMq consumer worker thread notifies the RPC server owner thread
                    synchronized (monitor) {
                        monitor.notify();
                    }
                }
            };

            channel.basicConsume(RPC_QUEUE_NAME, false, deliverCallback, (consumerTag -> { }));
            // Wait and be prepared to consume the message from RPC client.
            while (true) {
                synchronized (monitor) {
                    try {
                        monitor.wait();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    }
}
