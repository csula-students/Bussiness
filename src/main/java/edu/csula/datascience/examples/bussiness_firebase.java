package edu.csula.datascience.examples;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.bson.Document;

import com.firebase.client.Firebase;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class bussiness_firebase {
	static MongoClient mongoClient;
    static MongoDatabase database;
    static MongoCollection<Document> collection;

	public static void main(String[] args) {
		mongoClient = new MongoClient();
	    database = mongoClient.getDatabase("bussiness_data");
	    FindIterable<Document> cursor = null;
	    
	    collection = database.getCollection("offline_bussiness");
		cursor = collection.find();
	    
		// TODO Auto-generated method stub
		Firebase ref = new Firebase("https://businessdata.firebaseio.com/").child("bussiness");
		System.out.println(ref.getPath());
		Firebase aref = ref.push();
		Map<String, Bussiness> map = new HashMap<String, Bussiness>();
		Integer count = 0;
		for (Document doc : cursor) {
			count = count +1;
			
			Bussiness temp = new Bussiness(
        			doc.getString("type"),
                    doc.getString("bussinessID"),
                    doc.getString("name"),
                    doc.getString("city"),
                    doc.getString("state"),
                    doc.getDouble("stars"),
                    doc.getInteger("reviewCount"),
                    doc.get("category")
            );
			//businesses.add(temp);
			map.put(temp.getBussinessID(), temp);
			ref.push().setValue(temp);
			map.clear();
			System.out.println(count);
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
