package edu.csula.datascience.examples;

import com.google.gson.Gson;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import org.bson.Document;
import org.elasticsearch.action.bulk.*;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.node.Node;

import java.net.URISyntaxException;

import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

/**
PUT /bussiness-data
 {
     "mappings" : {
         "bussiness" : {
             "properties" : {
                 "name" : {
                     "type" : "string",
                     "index" : "not_analyzed"
                 },
                 "city" : {
                     "type" : "string",
                     "index" : "not_analyzed"
                 },
                 "state" : {
                     "type" : "string",
                     "index" : "not_analyzed"
                 },
                 "stars" : {
                     "type" : "double"
                 },
                 "reviewCount" : {
                     "type" : "integer"
                 }
             }
         }
     }
 }

 ```
 */
public class bussiness_data {
    private final static String indexName = "bussiness-data";
    private final static String typeName = "bussiness";
    
    static MongoClient mongoClient;
    static MongoDatabase database;
    static MongoCollection<Document> collection;

    public static void main(String[] args) throws URISyntaxException {
    	mongoClient = new MongoClient();
	    database = mongoClient.getDatabase("bussiness_data");
	    FindIterable<Document> cursor = null;
	    
        Node node = nodeBuilder().settings(Settings.builder()
            .put("cluster.name", "real-rakesh")
            .put("path.home", "elasticsearch-data")).node();
        Client client = node.client();

        /**
         *
         *
         * INSERT data to elastic search
         */


        // create bulk processor
        BulkProcessor bulkProcessor = BulkProcessor.builder(
            client,
            new BulkProcessor.Listener() {
                @Override
                public void beforeBulk(long executionId,
                                       BulkRequest request) {
                }

                @Override
                public void afterBulk(long executionId,
                                      BulkRequest request,
                                      BulkResponse response) {
                }

                @Override
                public void afterBulk(long executionId,
                                      BulkRequest request,
                                      Throwable failure) {
                    System.out.println("Facing error while importing data to elastic search");
                    failure.printStackTrace();
                }
            })
            .setBulkActions(10000)
            .setBulkSize(new ByteSizeValue(1, ByteSizeUnit.GB))
            .setFlushInterval(TimeValue.timeValueSeconds(5))
            .setConcurrentRequests(1)
            .setBackoffPolicy(
                BackoffPolicy.exponentialBackoff(TimeValue.timeValueMillis(100), 3))
            .build();

        // Gson library for sending json to elastic search
        Gson gson = new Gson();

        try {
            // after reading the csv file, we will use CSVParser to parse through
            // the csv files
        	collection = database.getCollection("offline_bussiness");
    		cursor = collection.find();

            // for each record, we will insert data into Elastic Search
    		for(Document doc : cursor){
    			Bussiness bussiness = new Bussiness(
                        doc.getString("type"),
                        doc.getString("bussinessID"),
                        doc.getString("name"),
                        doc.getString("city"),
                        doc.getString("state"),
                        doc.getDouble("stars"),
                        doc.getInteger("reviewCount"),
                        doc.get("category")
                    );
    			 bulkProcessor.add(new IndexRequest(indexName, typeName)
                 	.source(gson.toJson(bussiness))
    			);
    			System.out.println(doc.getString("bussinessID"));
    		}
            
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    static class Bussiness {
        private final String type;
        private final String bussinessID;
        private final String name;
        private final String city;
        private final String state;
        private final double stars;
        private final int reviewCount;
        private final Object category;
        
        
		public Bussiness( String type, String bussinessID,
				String name, String city, String state, double stars,
				int reviewCount, Object catehgory) {
			super();
			this.type = type;
			this.bussinessID = bussinessID;
			this.name = name;
			this.city = city;
			this.state = state;
			this.stars = stars;
			this.reviewCount = reviewCount;
			this.category = catehgory;
		}

		public String getType() {
			return type;
		}
		public String getBussinessID() {
			return bussinessID;
		}
		public String getName() {
			return name;
		}
		public String getCity() {
			return city;
		}
		public String getState() {
			return state;
		}
		public double getStars() {
			return stars;
		}
		public int getReviewCount() {
			return reviewCount;
		}
		public Object getCatehgory() {
			return category;
		} 
    }
}