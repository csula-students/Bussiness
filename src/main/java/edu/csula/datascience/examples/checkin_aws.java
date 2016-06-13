package edu.csula.datascience.examples;

import com.google.common.collect.Lists;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import edu.csula.datascience.examples.checkin_data.Bussiness;

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
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * A quick example app to send data to elastic search on AWS
 */
public class checkin_aws {
	static MongoClient mongoClient;
    static MongoDatabase database;
    static MongoCollection<Document> collection;
    
    public static void main(String[] args) throws URISyntaxException {
    	mongoClient = new MongoClient();
	    database = mongoClient.getDatabase("bussiness_data");
	    FindIterable<Document> cursor = null;
	    
	    String indexName = "checkin-data";
	    String typeName = "checkin";
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
        	collection = database.getCollection("offline_checkin");
    		cursor = collection.find();
    		Document dbobj;
    		List<Document> checkobj;
    		Collection<Object> listobj;
            Collection<checkin> temperatures = Lists.newArrayList();

            int count = 0;
            List<checkintime> checkintimes;

            // for each record, we will insert data into Elastic Search
//            parser.forEach(record -> {
            for (Document doc : cursor) {
                // cleaning up dirty data which doesn't have time or temperature
            	checkintimes = new ArrayList<checkintime>();
            	checkobj = (List<Document>) doc.get("checkinfo");
            	for(Document checkdoc : checkobj){
            		checkintimes.add(new checkintime(checkdoc.getString("weekDayName"),
            				checkdoc.getString("startTime"),checkdoc.getString("endTime")));
            	}
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
    			checkin temp = new checkin(
            			doc.getString("type"),
                        doc.getString("bussinessID"),
                        checkintimes,bussiness);
    				
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
        } catch (Exception e) {
            e.printStackTrace();
        }

        System.out.println("We are done! Yay!");
    }

    static class checkin {
    	private final String type;
        private final String bussinessID;
        private final List<checkintime> checkintimes;
        private final Bussiness bussiness;
        
		public checkin(String type, String bussinessID,
				List<checkintime> checkintimes, Bussiness bussiness) {
			super();
			this.type = type;
			this.bussinessID = bussinessID;
			this.checkintimes = checkintimes;
			this.bussiness = bussiness;
		}
		public String getType() {
			return type;
		}
		public String getBussinessID() {
			return bussinessID;
		}
		public List<checkintime> getCheckintimes() {
			return checkintimes;
		}
		public Bussiness getBussiness() {
			return bussiness;
		}
		
    }
    public static class checkintime{
		String weekName;
		String openTime;
		String closeTime;
		
		public checkintime(String weekName, String openTime, String closeTime) {
			super();
			this.weekName = weekName;
			this.openTime = openTime;
			this.closeTime = closeTime;
		}
		public String getWeekName() {
			return weekName;
		}
		public void setWeekName(String weekName) {
			this.weekName = weekName;
		}
		public String getOpenTime() {
			return openTime;
		}
		public void setOpenTime(String openTime) {
			this.openTime = openTime;
		}
		public String getCloseTime() {
			return closeTime;
		}
		public void setCloseTime(String closeTime) {
			this.closeTime = closeTime;
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