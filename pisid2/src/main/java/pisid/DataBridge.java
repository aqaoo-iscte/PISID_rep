package pisid;

import java.time.LocalDateTime;
import java.util.function.Consumer;

import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;

public class DataBridge {
	
	
	static void transfer() {
		//CLoud
		String time = LocalDateTime.now().toString().split("\\.")[0] + "Z" ;
		MongoClient mongoCloud = new MongoClient(new MongoClientURI("mongodb://aluno:aluno@194.210.86.10"));
		MongoDatabase mongoDBCloud = mongoCloud.getDatabase("sid2022");
		final MongoCollection<Document> collectionCloud = mongoDBCloud.getCollection("medicoes2022");		
		final FindIterable<Document> itemsCloud = collectionCloud.find(Filters.eq("Data", time));
		
		//Local
		MongoClient mongoLocal = new MongoClient(new MongoClientURI("mongodb://localhost:27019,localhost:27020,localhost:27021"));
		MongoDatabase mongoDBLocal = mongoLocal.getDatabase("labsensordata");
		MongoCollection<Document> itemsLocal = mongoDBLocal.getCollection("medicoestemp");
		
		Consumer<Document> printConsumer = new Consumer<>() {
		    public void accept(final Document doc) {
		    	itemsLocal.insertOne(doc);
		        try {
					Thread.sleep(2000);
				} catch (InterruptedException e) {}
		    }
		};
		itemsCloud.forEach(printConsumer);
	}
	public static void main(String[] args) {
		while (true) {
			transfer();
		}
		
		
	}

}
