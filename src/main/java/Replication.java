import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoException;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.lang.annotation.Documented;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static java.util.Arrays.asList;

/**
 * Created by vlfa on 19/12/2016.
 */
public class Replication {

    public static void main (String[] args) throws InterruptedException {
        MongoClient client = new MongoClient(asList(
                new ServerAddress("localhost", 27017),
                new ServerAddress("localhost", 27018),
                new ServerAddress("localhost", 27019)));
                                                // MongoClientOptions.builder()
                                                //          .requiredReplicaSetName("m101")
                                                //          .build());

        MongoDatabase db = client.getDatabase("test");
        MongoCollection<Document> collection = db.getCollection("example");

        collection.drop();

        for (int i = 0; i < Integer.MAX_VALUE; i++) {
            try {
                collection.insertOne(new Document("_id", i));
                System.out.println("Inserted document: " + i);
                Thread.sleep(1000);
            }
            catch (MongoException e) {
                System.out.println("Exception inserting document" + i + ": " + e.getMessage());
            }
        }
    }
}
