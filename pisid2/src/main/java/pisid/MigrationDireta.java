package pisid;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.function.Consumer;

import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class MigrationDireta {
	 static Connection connection;
	
	static void connect() {
		try {
			connection = DriverManager.getConnection("jdbc:mariadb://localhost:3306/culturas_local?user=java&password=java");
		} catch (SQLException e) {
			
			e.printStackTrace();
		}
		
	}
	
	
	
	static void sendinfo(){
		
		MongoClient mongoClient = new MongoClient(new MongoClientURI("mongodb://localhost:27019,localhost:27020,localhost:27021"));
		MongoDatabase mongoDB = mongoClient.getDatabase("labsensordata");			
		final MongoCollection<Document> collection = mongoDB.getCollection("medicoes");
		MongoCollection<Document> collection2 = mongoDB.getCollection("medicoestemp");
		
		final FindIterable<Document> items = collection.find();
		final FindIterable<Document> items2 = collection2.find();
		
				
		Consumer<Document> printConsumer = new Consumer<Document>() {
			public void accept(final Document doc1){				
	
				Consumer<Document> printConsumer2 = new Consumer<Document>() {
					public void accept(final Document doc2) {																	
							if(doc1.equals(doc2)){ 
								String arrived = doc1.toString();								
								String converted[] = spliterdoquiterio(arrived);
								String query = "insert into medicao(Zona,Sensor,Hora,Leitura) values(?,?,?,?)";
							     try {							    	 	
							  			PreparedStatement Mypst = connection.prepareStatement(query);								  			
							  			Mypst.setInt(1, Integer.parseInt(converted[0]));
							  			Mypst.setString(2, converted[1]);
							  			Mypst.setString(3, converted[2]);
							  			Mypst.setString(4, converted[3]);							  			
							  			Mypst.execute();						  			
							  		} catch (SQLException e) {
							  			
							  			e.printStackTrace();
							  		}
							     
								collection2.deleteOne(doc1);
							}

					}
				};

				items.forEach(printConsumer2);										
			
				  
			}
			
		};
		
		
		items2.forEach(printConsumer);
	
       
		
		
		
	}
	
	private static String[] spliterdoquiterio(String arrived) {		
		 String newarrived = (arrived.replaceAll("\"", "")).replaceAll("}", "");	 
		 String[] arrived_split = newarrived.split(",");
		 String zona = arrived_split[1].split("=")[1].replaceAll("Z", "");
		 String sensor = arrived_split[2].split("=")[1];
		 String medicao = arrived_split[4].split("=")[1];
		 String data = arrived_split[3].split("=")[1].replaceAll("T", " ").replaceAll("L", " ").replaceAll("H", " ").replaceAll("Z","");
		 
		 String[] enviar = {zona, sensor, data, medicao};

		 return enviar;
	}
		
	

	public static void main(String[] args) {
		connect();
		while (true) {
			sendinfo();
			try {
				Thread.sleep(2000);
			} catch (InterruptedException e) {
				e.printStackTrace();

			}
		}

	}

}
