package edu.csula.datascience.acquisition;

import com.google.common.collect.Lists;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Collection;
import java.util.List;

/**
 * A mock source to provide data
 */
public class MockSource implements Source<MockData> {
    

	public Collection<MockData> provide() {
List<MockData> mockDatas = Lists.newArrayList();
    	
    	File file = new File("#Zootopia.txt");
    	
    	String moviename = "";
        String datasource = "";
        String location = "";
        String username = "";
        String usercreation = "";
        String userdesc = "";
        String userfav = "";
        String userfollow = "";
        String userfrnd = "";
        String userlang = "";
        String usertimezone = "";
        String userstatus = "";
        String createAt = "";
        String userRetweet = "";
        String favcount = "";
        String lang = "";
        String retweetcount = "";
        String tweetsource = "";
        String tweettext = "";
    	
    	FileReader fw;
		try {
			fw = new FileReader(file);
			BufferedReader bw = new BufferedReader(fw);
			String txtsingledata = "";
			while(bw.readLine() != null){

				String[] txtdata = null;
				txtsingledata = bw.readLine();
				if(txtsingledata != null && txtsingledata != ""){
					txtdata = txtsingledata.split("`");
				}else{
					txtdata = null;
				}
				if(txtdata != null)
				if(txtdata.length == 19){
				//System.out.print(bw.readLine() + txtdata.length);
				
				if(txtdata[0].split("~")[1] != "")
					moviename = txtdata[0].split("~")[1];
				else
					moviename = "";
				
				if(txtdata[1].split("~")[1] != "")
					datasource = txtdata[1].split("~")[1];
				else
					datasource = "";
				
				if(txtdata[2].indexOf("~") != txtdata[2].length()-1)
					location = txtdata[2].split("~")[1];
				else
					location = "";
				
				if(txtdata[3].split("~")[1] != "")
					username = txtdata[3].split("~")[1];
				else
					username = "";
				
				if(txtdata[4].split("~")[1] != "")
					usercreation = txtdata[4].split("~")[1];
				else
					usercreation = "";
				
				if(txtdata[5].indexOf("~") != txtdata[5].length()-1)
					userdesc = txtdata[5].split("~")[1];
				else
					userdesc = "";
				
				if(txtdata[6].split("~")[1] != "")
					userfav = txtdata[6].split("~")[1];
				else
					userfav = "";
				
				if(txtdata[7].split("~")[1] != "")
					userfollow = txtdata[7].split("~")[1];
				else
					userfollow = "";
				
				if(txtdata[8].split("~")[1] != "")
					userfrnd = txtdata[8].split("~")[1];
				else
					userfrnd = "";
				
				if(txtdata[9].split("~")[1] != "")
					userlang = txtdata[9].split("~")[1];
				else
					userlang = "";
				
				if(txtdata[10].split("~")[1] != "")
					usertimezone = txtdata[10].split("~")[1];
				else
					usertimezone = "";
				
				if(txtdata[11].split("~")[1] != "")
					userstatus = txtdata[11].split("~")[1];
				else
					userstatus = "";
				
				if(txtdata[12].split("~")[1] != "")
					createAt = txtdata[12].split("~")[1];
				else
					createAt = "";
				
				if(txtdata[13].split("~")[1] != "")
					retweetcount = txtdata[13].split("~")[1];
				else
					retweetcount = "";
				
				if(txtdata[14].split("~")[1] != "")
					favcount = txtdata[14].split("~")[1];
				else
					favcount = "";
				if(txtdata[15].split("~")[1] != "")
					lang = txtdata[15].split("~")[1];
				else
					lang = "";
				
				if(txtdata[16].split("~")[1] != "")
					retweetcount = txtdata[16].split("~")[1];
				else
					retweetcount = "";
				
				if(txtdata[17].split("~")[1] != "")
					tweetsource = txtdata[17].split("~")[1];
				else
					tweetsource = "";
				
				if(txtdata[18].split("~")[1] != "")
					tweettext = txtdata[18].split("~")[1];
				else
					tweettext = "";
				
				MockData mockData = new MockData(moviename,datasource,location,username,usercreation,userdesc,
						userfav,userfollow,userfrnd,userlang,usertimezone,userstatus,createAt,userRetweet,
						favcount,lang,retweetcount,tweetsource,tweettext);
				//System.out.print(mockData);
				mockDatas.add(mockData);
				}
			}
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        return mockDatas;
	}
}
