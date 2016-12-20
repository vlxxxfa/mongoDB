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

    public static void main(String[] args) throws InterruptedException {

        Block<Document> printBlock = new Block<Document>() {

            public void apply(final Document document) {
                System.out.println(document.toJson());
            }
        };

        // to connect to a replica set, with auto-discovery of the primary, supply a seed list of members
        // http://mongodb.github.io/mongo-java-driver/3.0/driver/getting-started/quick-tour/

        MongoClient mongoClient = new MongoClient("localhost", 27017);
        MongoDatabase db = mongoClient.getDatabase("test");
        MongoCollection<Document> collectionOfZips = db.getCollection("zips");
        MongoCollection<Document> collectionForReplication = db.getCollection("replication");
        MongoCollection<Document> collectionForPosts = db.getCollection("posts");
        MongoCollection<Document> collectionForSmallZips = db.getCollection("small_zips");
        MongoCollection<Document> collectionForGrades = db.getCollection("grades");
        MongoCollection<Document> collectionForZipsCodeDataSampleZip = db.getCollection("zips_code_data_sample_zip");

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

        System.out.println("\n" + "Examples:");
        System.out.println("\n" + "1. to find the most frequent author of comments on your blog" +"\n");

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

        System.out.println("\n" + "2. to calculate the average population of cities in California (abbreviation CA)" +
                "and New York (NY) (taken together) with populations over 25,000" +"\n");

        /*List<Bson> pipelineToCalculateTheAveragePopulationOfCAAndNY = Arrays.asList();

        collectionForSmallZips.aggregate(pipelineToCalculateTheAveragePopulationOfCAAndNY).forEach(printBlock);
        */

        // The third method to aggregate pipeline
        List<Document> pipelineToCalculateTheAveragePopulationOfCAAndNY = Arrays.asList(
                Document.parse("{$match: {state: {$in: ['CA', 'NY']}}}"),
                // group by state and city
                Document.parse("{$group: { _id: {state: \"$state\", city: \"$city\"}, pop: {$sum: \"$pop\"}}}"),
                // only look at cities over 25.000
                Document.parse("{$match: {pop: {$gt: 25000}} }"),
                // get the average population across those cities
                Document.parse("{$group: { _id: null, pop: {$avg: \"$pop\"} } }"));

        collectionForSmallZips.aggregate(pipelineToCalculateTheAveragePopulationOfCAAndNY).forEach(printBlock);

        System.out.println("\n" + "3. to calculate the class with the best average student performance" +"\n");

        List<Document> pipelineForGrades = Arrays.asList(
                // We use $unwind to deconstruct the scores array into separate documents.
                Document.parse("{$unwind : \"$scores\" }"),
                // We use $match to filter out documents that aren't quizzes, leaving us with homeworks and exams.
                Document.parse("{$match : { \"scores.type\" : { $ne : \"quiz\" } } }"),
                // We use $group to calculate the GPA for each student in the class.
                Document.parse("{ $group : { _id : { student_id : \"$student_id\", class_id : \"$class_id\" }, avg : { $avg : \"$scores.score\" } } }"),
                // We process this result with another $group to compute the class average.
                Document.parse("{ $group : { _id : \"$_id.class_id\", avg : { $avg : \"$avg\" } } }"),
                // We sort the results by highest average descending.
                Document.parse("{ $sort : { \"avg\" : -1 } }"),
                // Finally, we limit our results to classes with the top 5 averages.
                Document.parse("{ $limit : 5 })"));

        collectionForGrades.aggregate(pipelineForGrades).forEach(printBlock);

        System.out.println("\n" + "4. to calculate the number of people who live in a zip code in the US where the city starts with a digit" +"\n");

        List<Document> pipelineForZipsCodeDataSampleZip = Arrays.asList(
                // using substring operator to pull the first character out of the city so that you can compare it
                Document.parse("{$project: { first_char: {$substr : [\"$city\",0,1]}," +
                        "pop:1," +
                        "city:\"$city\"," +
                        "zip:\"$_id\"," +
                        "state:1}}"),
                Document.parse("{$match:{first_char:{$in:['0','1','2','3','4','5','6','7','8','9']}}}"),
                Document.parse("{$group:{_id:null, population:{$sum:\"$pop\"}}}")
        );

        collectionForZipsCodeDataSampleZip.aggregate(pipelineForZipsCodeDataSampleZip).forEach(printBlock);

        // how to connect to a replica set using the MongoDB Java driver

        collectionForReplication.drop();

        for (int i = 0; i < Integer.MAX_VALUE; i++) {
            collectionForReplication.insertOne(new Document("_id", i));
            System.out.println("Inserted document " + i);
            Thread.sleep(500);
        }
    }
}

