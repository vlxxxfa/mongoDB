import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Accumulators;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Sorts;
import org.bson.Document;
import org.bson.conversions.Bson;
import com.mongodb.Block;

import java.util.Arrays;
import java.util.List;

/**
 * Created by vlfa on 13/12/2016.
 */
public class ZipCodeAggregationTest {

    public static void main(String[] args) {

        Block<Document> printBlock = new Block<Document>() {
            @Override
            public void apply(final Document document) {
                System.out.println(document.toJson());
            }
        };

        // to connect to a replica set, with auto-discovery of the primary, supply a seed list of members
        // http://mongodb.github.io/mongo-java-driver/3.0/driver/getting-started/quick-tour/

        MongoClient mongoClient = new MongoClient("localhost", 27017);
        MongoDatabase db = mongoClient.getDatabase("test");
        MongoCollection<Document> collectionOfZips = db.getCollection("zips");

        // List<Document> results = collectionOfZips.find().into(new ArrayList<Document>());

        // The first method to aggregate pipeline to use the document class
        System.out.println("\n" + "The first method to aggregate pipeline to use the document class:" + "\n");

        List<Document> pipeline = Arrays.asList(
                new Document("$group",
                        new Document("_id", "$state").append("totalPop",
                                new Document("$sum", "$pop"))),
                new Document("$match",
                        new Document("totalPop",
                                new Document("$gte", 12018340))));

        collectionOfZips.aggregate(pipeline).forEach(printBlock);

        // The second method to aggregate pipeline to use the various builder classes
        // Since Java Driver 3.1 added mongoDB builders for the aggregation framework
        System.out.println("\n" + "The second method to aggregate pipeline to use the various builder (since Java Driver 3.1) classes:" + "\n");

        List<Bson> pipelineWithBuilder = Arrays.asList(Aggregates.group("$state", Accumulators.sum("totalPop", "$pop")),
                Aggregates.match(Filters.gte("totalPop", 12018340)));

        collectionOfZips.aggregate(pipelineWithBuilder).forEach(printBlock);

        // The third method to aggregate pipeline to use document parse for quickly creating pipelines
        // from shell syntax
        // Quickly copy and paste from shell syntax into Java. Got the same results
        System.out.println("\n" + "The third method to aggregate pipeline to" +
                "use document parse for quickly creating pipelines" + "\n" +
                "from shell syntax:"+ "\n");

        List<Document> pipelineWithShellSyntax = Arrays.asList(
                Document.parse("{$group: { _id: \"$state\", totalPop: { $sum: \"$pop\"}}}"),
                Document.parse("{$match: { totalPop: { $gte: 12018340}}}"));

        collectionOfZips.aggregate(pipelineWithShellSyntax).forEach(printBlock);

        System.out.println("\n" + "Examples:" + "\n");
        System.out.println("\t" + "1. to find the most frequent author of comments on your blog" +"\n");


        MongoCollection<Document> collectionForPosts = db.getCollection("posts");

        List<Bson> pipelineToFindTheMostFrequentAuthorOfComments = Arrays.asList(
                Aggregates.unwind("$comments"),
                Aggregates.group("$comments.author", Accumulators.sum("num_comments", 1)),
                Aggregates.sort(Sorts.descending("num_comments")),
                Aggregates.limit(10));

        /*List<Document> pipelineToFindTheMostFrequentAuthorOfComments = Arrays.asList(
                Document.parse("{$unwind: \"$comments\"}"),
                Document.parse("{$group: {_id:\"$comments.author\", num_comments:{$sum:1}}}"),
                Document.parse("{$sort: {num_comments:-1}}"),
                Document.parse("{$limit: 5}"));*/

        collectionForPosts.aggregate(pipelineToFindTheMostFrequentAuthorOfComments).forEach(printBlock);

        MongoCollection<Document> collectionForSmallZips = db.getCollection("small_zips");

        System.out.println("\n" + "\t" + "2. to calculate the average population of cities in California (abbreviation CA)" +
                "and New York (NY) (taken together) with populations over 25,000" +"\n");

        List<Bson> pipelineToCalculateTheAveragePopulationOfCAAndNY = Arrays.asList();

        collectionForSmallZips.aggregate(pipelineToCalculateTheAveragePopulationOfCAAndNY).forEach(printBlock);

    }
}

