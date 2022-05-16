package pisid;

import com.hivemq.client.mqtt.MqttClient;
import com.hivemq.client.mqtt.MqttClient;
import com.hivemq.client.mqtt.mqtt5.Mqtt5BlockingClient;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import static com.hivemq.client.mqtt.MqttGlobalPublishFilter.ALL;
import static java.nio.charset.StandardCharsets.UTF_8;

import java.util.function.Consumer;

import org.bson.Document;

public class Premigracao {
	
		final static String host = "9c0cafee45454ac597fe285b8bdf4b7e.s2.eu.hivemq.cloud";
        final static String username = "Quiterioi";
        final static String password = "Mqtt2022";
        static Mqtt5BlockingClient client;
        static int count;
	
	static void connect(){
		

        //create an MQTT client
        client = MqttClient.builder()
                .useMqttVersion5()
                .serverHost(host)
                .serverPort(8883)
                .sslWithDefaultConfig()
                .buildBlocking();

        //connect to HiveMQ Cloud with TLS and username/pw
        client.connectWith()
                .simpleAuth()
                .username(username)
                .password(UTF_8.encode(password))
                .applySimpleAuth()
                .send();
		
        System.out.println("Connected successfully");
		
		
	}
	
	
	static void sendinfo(){
		
		MongoClient mongoClient = new MongoClient(new MongoClientURI("mongodb://localhost:27019,localhost:27020,localhost:27021"));
		MongoDatabase mongoDB = mongoClient.getDatabase("labsensordata");
	
		
		final MongoCollection<Document> collection = mongoDB.getCollection("medicoes");
		MongoCollection<Document> collection2 = mongoDB.getCollection("medicoestemp");
		
		final FindIterable<Document> items = collection.find();
		final FindIterable<Document> items2 = collection2.find();
		
				
		Consumer<Document> printConsumer = new Consumer<Document>() {
			public void accept(final Document doc1) {				
	
				Consumer<Document> printConsumer2 = new Consumer<Document>() {
					public void accept(final Document doc2) {																	
							if(doc1.equals(doc2)) { 
								 client.publishWith()
							        .topic("PSID")
							        .payload(UTF_8.encode(doc1.toJson()))
							        .send();
								 	collection2.deleteOne(doc1);
							}

					}
				};

				items.forEach(printConsumer2);										
			
				  
			}
			
		};
		
		
		items2.forEach(printConsumer);
	
       
		
		
		
	}
	
	
	
	public static void main(String[] args) {
		connect();
		while(true) {
			sendinfo();
			try {
				Thread.sleep(2000);
			} catch (InterruptedException e) {
				
				e.printStackTrace();
			}
		}
		
	}

}
