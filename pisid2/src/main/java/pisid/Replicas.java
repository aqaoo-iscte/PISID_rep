package pisid;

import com.mongodb.*;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;

import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.result.*;
import org.bson.Document;
import org.bson.types.ObjectId;

import java.util.List;
import java.util.function.Consumer;
import java.util.Arrays;
import java.time.LocalDateTime;
import java.util.ArrayList;

import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Updates.*;

public class Replicas {
	
	
	static int count;
	
	static void copiar() {		
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
								count++;	
							}
					}
				};

				items.forEach(printConsumer2);			
				
				if(count==0){
					collection.insertOne(doc1);					
				}
				count = 0;
				  
			}
			
		};
		
		
		items2.forEach(printConsumer);
	}

	public static void main(String[] args) {
			
		
		while(true){
			
			copiar();
			try {
				Thread.sleep(2000);
			} catch (InterruptedException e) {

				e.printStackTrace();
			}
			
		}

	}

}



