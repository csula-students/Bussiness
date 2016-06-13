package edu.csula.datascience.examples;

import com.google.common.collect.Lists;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import edu.csula.datascience.examples.review_data.Bussiness;
import edu.csula.datascience.examples.review_data.review;
import edu.csula.datascience.examples.review_data.votes;

import org.bson.Document;

import io.searchbox.action.BulkableAction;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Bulk;
import io.searchbox.core.Index;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;

/**
 * A quick example app to send data to elastic search on AWS
 */
public class review_aws {
	static MongoClient mongoClient;
    static MongoDatabase database;
    static MongoCollection<Document> collection;
    
    public static void main(String[] args) throws URISyntaxException {
    	mongoClient = new MongoClient();
	    database = mongoClient.getDatabase("bussiness_data");
	    FindIterable<Document> cursor = null;
	    
	    String indexName = "review-data";
	    String typeName = "reviews";
        String awsAddress = "http://search-bussiness-data-rrjtj42k5taissn5bhlf3hubnu.us-west-2.es.amazonaws.com";
        JestClientFactory factory = new JestClientFactory();
        factory.setHttpClientConfig(new HttpClientConfig
            .Builder(awsAddress)
            .multiThreaded(true)
            .build());
        JestClient client = factory.getObject();

        // as usual process to connect to data source, we will need to set up
        // node and client// to read CSV file from the resource folder

        try {
            // after reading the csv file, we will use CSVParser to parse through
            // the csv files
        	collection = database.getCollection("offline_review");
    		cursor = collection.find();
            Collection<review> temperatures = Lists.newArrayList();
            Document dbobj;
            Date reviewDate = null;
    		DateFormat sdf = new SimpleDateFormat("yyyy-mm-dd");
            int Cnt = 0;
            String reviewDateString = "";
            Integer count = 0;
            // for each record, we will insert data into Elastic Search
//            parser.forEach(record -> {
            for (Document doc : cursor) {
                // cleaning up dirty data which doesn't have time or temperature
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
    			review temp = new review(
    					doc.getString("type"),
    					doc.getString("bussinessID"),
    					doc.getString("text"),
    					reviewDateString,
    					doc.getDouble("stars"),
    					vote,
    					bussiness
    					);

                    if (count < 500) {
                        temperatures.add(temp);
                        count ++;
                    } else {
                        try {
                            Collection<BulkableAction> actions = Lists.newArrayList();
                            temperatures.stream()
                                .forEach(tmp -> {
                                    actions.add(new Index.Builder(tmp).build());
                                });
                            Bulk.Builder bulk = new Bulk.Builder()
                                .defaultIndex(indexName)
                                .defaultType(typeName)
                                .addAction(actions);
                            client.execute(bulk.build());
                            count = 0;
                            temperatures = Lists.newArrayList();
                            System.out.println("Inserted 500 documents to cloud");
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
            }

            Collection<BulkableAction> actions = Lists.newArrayList();
            temperatures.stream()
                .forEach(tmp -> {
                    actions.add(new Index.Builder(tmp).build());
                });
            Bulk.Builder bulk = new Bulk.Builder()
                .defaultIndex(indexName)
                .defaultType(typeName)
                .addAction(actions);
            client.execute(bulk.build());
        } catch (IOException e) {
            e.printStackTrace();
        }

        System.out.println("We are done! Yay!");
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