package demo;

import com.rabbitmq.client.*;

import java.util.HashMap;
import java.util.Map;

public class DeadLetterHandlingThread  implements Runnable{

    Thread t;
    String name;
    DeadLetterHandlingThread(String name){
         t= new Thread(this,name);
         t.start();
     }


    public void consume() throws Exception{
        System.out.println("here in dead letter");
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection=factory.newConnection();
        Channel channel=connection.createChannel();

        //channel.queueDeclare(name, false, false, false, null);
        channel.basicQos(1);

        Object monitor = new Object();
        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            AMQP.BasicProperties replyProps = new AMQP.BasicProperties
                    .Builder()
                    .correlationId(delivery.getProperties().getCorrelationId())
                    .build();

            String response = "exception";

            try {
                String message = new String(delivery.getBody(), "UTF-8");

                System.out.println("Got message from dead letter " + message);
            } catch (RuntimeException e) {
                System.out.println(" [.] " + e.toString());
            } finally {
                channel.basicPublish("","REPLY_QUEUE", replyProps, response.getBytes("UTF-8"));
                channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
                // RabbitMq consumer worker thread notifies the RPC server owner thread
                synchronized (monitor) {
                    monitor.notify();
                }
            }
        };

        channel.basicConsume(name, false, deliverCallback, (consumerTag -> { }));
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
     public void run() {
         try {
             System.out.println("in run method");
             consume();
         } catch (Exception e) {
             e.printStackTrace();
         }
     }
}
