package edu.csula.datascience.examples;

import com.google.gson.Gson;
import com.mongodb.DBObject;
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
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

/**
 * Quiz elastic search app to see Salaries.csv file better
 *
 * gradle command to run this app `gradle esQuiz`
 *
 * Before you send data, please run the following to update mapping first:
 *
 * ```
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
                 },
                 "category": {
                     "type": "nested"
                 }
             }
         }
     }
 }

 ```
 */
public class review_data {
    private final static String indexName = "review-data";
    private final static String typeName = "review";
    
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
        	System.out.println("1");
        	collection = database.getCollection("offline_review");
    		cursor = collection.find();
    		Document dbobj;
    		System.out.println(cursor.first());
    		Date reviewDate = null;
    		DateFormat sdf = new SimpleDateFormat("yyyy-mm-dd");
    		Integer Cnt = 0;
    		String reviewDateString = "";

            // for each record, we will insert data into Elastic Search
    		for(Document doc : cursor){
    			Cnt = Cnt + 1;
    			System.out.println(Cnt);
    			dbobj =  (Document) doc.get("votes");
    			votes vote = new votes(
    					Integer.parseInt(dbobj.get("cool").toString()),
    					Integer.parseInt(dbobj.get("useful").toString()),
    					Integer.parseInt(dbobj.get("funny").toString())
    					);
    			dbobj =  (Document) doc.get("bussiness");
    			Bussiness bussiness = new Bussiness(
    					dbobj.get("type").toString(),
    					dbobj.get("bussinessID").toString(),
    					dbobj.get("name").toString(),
    					dbobj.get("city").toString(),
    					dbobj.get("state").toString(),
    					Double.parseDouble(dbobj.get("stars").toString()),
    					Integer.parseInt(dbobj.get("reviewCount").toString()),
    					dbobj.get("category")
                    );
    			reviewDate = null;
    			try {
    				reviewDate = sdf.parse(doc.getString("date"));
    				reviewDateString = sdf.format(reviewDate);
				} catch (ParseException e) {
					e.printStackTrace();
				}
    			review review = new review(
    					doc.getString("type"),
    					doc.getString("bussinessID"),
    					doc.getString("text"),
    					reviewDateString,
    					doc.getDouble("stars"),
    					vote,
    					bussiness
    					);
    			 bulkProcessor.add(new IndexRequest(indexName, typeName)
                 	.source(gson.toJson(review))
    			);
    		}
    		System.out.println("3");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    static class review {
    	private final String type;
        private final String bussinessID;
        private final String text;
        private final String review_date;
        private final Double stars;
        private final votes vote;
        private final Bussiness bussiness;
		public review(String type, String bussinessID, String text,
				String review_date, Double stars, votes vote, Bussiness bussiness) {
			super();
			this.type = type;
			this.bussinessID = bussinessID;
			this.text = text;
			this.review_date = review_date;
			this.stars = stars;
			this.vote = vote;
			this.bussiness = bussiness;
		}
		public String getType() {
			return type;
		}
		public String getBussinessID() {
			return bussinessID;
		}
		public String getText() {
			return text;
		}
		public String getReview_date() {
			return review_date;
		}
		public Double getStars() {
			return stars;
		}
		public votes getVote() {
			return vote;
		}
		public Bussiness getBussiness() {
			return bussiness;
		}
        
    }
    static class votes {
    	private final Integer cool;
    	private final Integer useful;
    	private final Integer funny;
		public votes(Integer cool, Integer useful, Integer funny) {
			super();
			this.cool = cool;
			this.useful = useful;
			this.funny = funny;
		}
		public Integer getCool() {
			return cool;
		}
		public Integer getUseful() {
			return useful;
		}
		public Integer getFunny() {
			return funny;
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