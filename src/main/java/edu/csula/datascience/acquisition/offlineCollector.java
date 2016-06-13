package edu.csula.datascience.acquisition;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Iterator;
 


















import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

import org.bson.Document;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class offlineCollector {

	private static final JSONArray[] JSONArray = null;
	static MongoClient mongoClient;
    static MongoDatabase database;
    static MongoCollection<Document> collection;
    static File file;
    static JSONParser parser;
    static FileReader fw;
    static BufferedReader bw;
    static Object obj;
    static JSONObject jsonObject;
    static Document doc;
    static Hashtable bussinessdata = new Hashtable();
    static Hashtable userdata = new Hashtable();
    

    public static void getData() {
		mongoClient = new MongoClient();
	    database = mongoClient.getDatabase("bussiness_data");
	    
	    OfflineBussinessData();
	    OfflineUserData();
	    putOnHashTable();
	    OfflineTipData();
	    OfflineReviewData();
	    OfflineCheckIn();
	}
	public static void putOnHashTable(){
		FindIterable<Document> cursor = null;
		
		collection = database.getCollection("offline_bussiness");
		cursor = collection.find();
		
		for(Document doc : cursor){
			bussinessdata.put(doc.getString("bussinessID"),doc);
			//System.out.println(doc.getString("bussinessID"));
		}
		//System.out.println(bussinessdata.get("McikHxxEqZ2X0joaRNKlaw"));
		
		/*cursor = null;
		collection = database.getCollection("offline_users");
		cursor = collection.find();
		
		for(Document doc : cursor){
			userdata.put(doc.getString("userID"),doc);
		}*/
		//System.out.println(userdata.size());
		
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
public static void OfflineCheckIn(){
		
	    try
	    {
	    	collection = database.getCollection("offline_checkin");
        
	    	file = new File("yelp_academic_dataset_checkin.json");
	    	
	    	File writefile = new File("yelp_academic_dataset_checkin.txt");
		
			parser = new JSONParser();
			
			fw = new FileReader(file);
			bw = new BufferedReader(fw);
			String str = "";
			
			String bussinessID = "";
			JSONObject checkinfo;
			
			Integer count = 0;
			Object bussiness;
			
			FileWriter writefw = new FileWriter(writefile.getAbsoluteFile(),true);
			BufferedWriter writebw = new BufferedWriter(writefw);
			String weekDayName = "";
			String startTime = "";
			String endTime = "";
			Integer end = 0;
			List<Document> doclist;
			
			while(true){
				str = bw.readLine();
				count = count + 1;
				System.out.println(count);
				if(str != null){
					obj = parser.parse(str);
					
					jsonObject = (JSONObject) obj;
					
					bussinessID = jsonObject.get("business_id").toString();
					checkinfo = (JSONObject) jsonObject.get("checkin_info");
					Set<JSONObject> setobj = checkinfo.keySet(); 
					
					Object[] objarr = setobj.toArray();
					doclist = new ArrayList<Document>();
					for(Object objone : objarr){
						String[] objstr = objone.toString().split("-");
						startTime = objstr[0];
						end = Integer.parseInt(objstr[0]) + 1;
						if(end == 24){
							endTime = "0";
						}else
						{
							endTime = end.toString();
						}
						switch(objstr[1])
						{
							case "6" :
								weekDayName = "sunday";
								break;
							case "0" :
								weekDayName = "monday";
								break;
							case "1" :
								weekDayName = "tuseday";
								break;
							case "2" :
								weekDayName = "wednesday";
								break;
							case "3" :
								weekDayName = "thursday";
								break;
							case "4" :
								weekDayName = "friday";
								break;
							case "5" :
								weekDayName = "saturday";
								break;
								
						}
						Document onedoc = new Document("weekDayName", weekDayName).
								  append("startTime", startTime).
				                  append("endTime", endTime);
						doclist.add(onedoc);
					}
					
					/*whereQuery = new BasicDBObject();
				    whereQuery.put("bussinessID", bussinessID);
				    cursor = bussinesscollection.find(whereQuery);*/
					
					bussiness = bussinessdata.get(bussinessID);
					
					Document doc = new Document("type", "checkin").
							  append("bussinessID", bussinessID).
			                  append("checkinfo", doclist).
			                  append("bussiness", bussiness);
					
					collection.insertOne(doc);
					writebw.write(doc.toString());
					writebw.newLine();
					
					//System.out.println(jsonObject.get("user_id").toString());
				}else
					break;
			}
			writebw.close();
	    }catch(IOException | ParseException e){
	    	e.printStackTrace();
	    }
	}
	public static void OfflineReviewData(){
		
	    try
	    {
	    	collection = database.getCollection("offline_review");
        
	    	file = new File("yelp_academic_dataset_review.json");
	    	
	    	File writefile = new File("yelp_academic_dataset_review.txt");
		
			parser = new JSONParser();
			
			fw = new FileReader(file);
			bw = new BufferedReader(fw);
			String str = "";
			
			String userID = "";
			String bussinessID = "";
			String textd = "";
			String dated = "";
			Double stars = 0.0;
			JSONObject votes;
			
			Integer count = 0;
			Object user;
			Object bussiness;
			
			FileWriter writefw = new FileWriter(writefile.getAbsoluteFile(),true);
			BufferedWriter writebw = new BufferedWriter(writefw);
			
			while(true){
				str = bw.readLine();
				count = count + 1;
				System.out.println(count);
				if(str != null){
					obj = parser.parse(str);
					
					jsonObject = (JSONObject) obj;
					
					userID = jsonObject.get("user_id").toString();
					bussinessID = jsonObject.get("business_id").toString();
					textd = jsonObject.get("text").toString();
					dated = jsonObject.get("date").toString();
					stars = Double.parseDouble(jsonObject.get("stars").toString());
					votes = (JSONObject) jsonObject.get("votes");
					
					/*whereQuery = new BasicDBObject();
				    whereQuery.put("bussinessID", bussinessID);
				    cursor = bussinesscollection.find(whereQuery);*/
				    bussiness = bussinessdata.get(bussinessID);
				    
				    /*whereQuery = new BasicDBObject();
				    whereQuery.put("userID", userID);
				    cursor = usercollection.find(whereQuery);*/
				    user = userdata.get(userID);
					
					Document doc = new Document("type", "review").
							  append("userID", userID).
							  append("bussinessID", bussinessID).
							  append("text", textd).
			                  append("date", dated).
			                  append("stars", stars).
			                  append("votes", votes).
			                  append("bussiness", bussiness).
			                  append("user",user);
					
					collection.insertOne(doc);
					writebw.write(doc.toString());
					writebw.newLine();
					
					//System.out.println(jsonObject.get("user_id").toString());
				}else
					break;
			}
			writebw.close();
	    }catch(IOException | ParseException e){
	    	e.printStackTrace();
	    }
	}
	public static void OfflineTipData(){

	    try
	    {
	    	collection = database.getCollection("offline_tip");
        
	    	file = new File("yelp_academic_dataset_tip.json");
	    	
	    	File writefile = new File("yelp_academic_dataset_tip.txt");
		
			parser = new JSONParser();
			
			fw = new FileReader(file);
			bw = new BufferedReader(fw);
			String str = "";
			
			String userID = "";
			String bussinessID = "";
			String textd = "";
			String dated = "";
			Integer likes = 0;
			Integer count = 0;
			Object user;
			Object bussiness;
			
			FileWriter writefw = new FileWriter(writefile.getAbsoluteFile(),true);
			BufferedWriter writebw = new BufferedWriter(writefw);
			
			while(true){
				str = bw.readLine();
				count = count + 1;
				System.out.println(count);
				if(str != null){
					obj = parser.parse(str);
					
					jsonObject = (JSONObject) obj;
					
					userID = jsonObject.get("user_id").toString();
					bussinessID = jsonObject.get("business_id").toString();
					textd = jsonObject.get("text").toString();
					dated = jsonObject.get("date").toString();
					likes = Integer.parseInt(jsonObject.get("likes").toString());
					
					/*whereQuery = new BasicDBObject();
				    whereQuery.put("bussinessID", bussinessID);
				    cursor = bussinesscollection.find(whereQuery);*/
				    bussiness = bussinessdata.get(bussinessID);
				    
				    /*whereQuery = new BasicDBObject();
				    whereQuery.put("userID", userID);
				    cursor = usercollection.find(whereQuery);*/
				    user = userdata.get(userID);
					
					Document doc = new Document("type", "tip").
							  append("userID", userID).
							  append("bussinessID", bussinessID).
							  append("text", textd).
			                  append("date", dated).
			                  append("likes", likes).
			                  append("bussiness", bussiness).
			                  append("user",user);
					
					collection.insertOne(doc);
					writebw.write(doc.toString());
					writebw.newLine();
					
					System.out.println(jsonObject.get("user_id").toString());
				}else
					break;
			}
			writebw.close();
	    }catch(IOException | ParseException e){
	    	e.printStackTrace();
	    }
	}
	public static void OfflineUserData(){

	    try
	    {
	    	collection = database.getCollection("offline_users");
        
	    	file = new File("yelp_academic_dataset_user.json");
	    	
	    	File writefile = new File("yelp_academic_dataset_user.txt");
		
			parser = new JSONParser();
			
			fw = new FileReader(file);
			bw = new BufferedReader(fw);
			String str = "";
			
			String userID = "";
			String name = "";
			Double stars = 0.0;
			Integer reviewCount = 0;
			JSONObject votes;
			
			FileWriter writefw = new FileWriter(writefile.getAbsoluteFile(),true);
			BufferedWriter writebw = new BufferedWriter(writefw);
			
			
			while(true){
				str = bw.readLine();
				if(str != null){
					obj = parser.parse(str);
					
					jsonObject = (JSONObject) obj;
					
					userID = jsonObject.get("user_id").toString();
					name = jsonObject.get("name").toString();
					stars = Double.parseDouble(jsonObject.get("average_stars").toString());
					reviewCount = Integer.parseInt(jsonObject.get("review_count").toString());
					votes = (JSONObject) jsonObject.get("votes");
					
					Document doc = new Document("type", "user").
							  append("userID", userID).
							  append("name", name).
			                  append("avgstars", stars).
			                  append("reviewCount", reviewCount).
			                  append("votes",votes);
					
					collection.insertOne(doc);
					writebw.write(doc.toString());
					writebw.newLine();
					
					System.out.println(jsonObject.get("user_id").toString());
				}else
					break;
			}
			writebw.close();
	    }catch(IOException | ParseException e){
	    	e.printStackTrace();
	    }
	}
	public static void OfflineBussinessData(){
		
	    try
	    {
	    	collection = database.getCollection("offline_bussiness");
        
	    	file = new File("yelp_academic_dataset_business.json");
	    	
	    	File writefile = new File("yelp_academic_dataset_business.txt");
		
			parser = new JSONParser();
			
			fw = new FileReader(file);
			bw = new BufferedReader(fw);
			String str = "";
			
			String bussinessID = "";
			String name = "";
			String city = "";
			String state = "";
			Double stars = 0.0;
			Integer reviewCount = 0;
			JSONArray category = new JSONArray();
			
			FileWriter writefw = new FileWriter(writefile.getAbsoluteFile(),true);
			BufferedWriter writebw = new BufferedWriter(writefw);
			
			while(true){
				str = bw.readLine();
				if(str != null){
					obj = parser.parse(str);
					
					jsonObject = (JSONObject) obj;
					
					bussinessID = jsonObject.get("business_id").toString();
					name = jsonObject.get("name").toString();
					city = jsonObject.get("city").toString();
					state = jsonObject.get("state").toString();
					stars = Double.parseDouble(jsonObject.get("stars").toString());
					reviewCount = Integer.parseInt(jsonObject.get("review_count").toString());
					category = (JSONArray) jsonObject.get("categories");
					
					Document doc = new Document("type", "Bussiness").
							  append("bussinessID", bussinessID).
							  append("name", name).
			                  append("city", city.replace(" ", "")).
			                  append("state", state).
			                  append("stars", stars).
			                  append("reviewCount", reviewCount).
			                  append("category",category);
					
					collection.insertOne(doc);
					writebw.write(doc.toString());
					writebw.newLine();
					
					//System.out.println(jsonObject.get("business_id").toString() + count);
				}else
					break;
			}
			writebw.close();
	    }catch(IOException | ParseException e){
	    	e.printStackTrace();
	    }
	}

}
