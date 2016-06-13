package edu.csula.datascience.acquisition;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.scribe.builder.ServiceBuilder;
import org.scribe.model.OAuthRequest;
import org.scribe.model.Response;
import org.scribe.model.Token;
import org.scribe.model.Verb;
import org.scribe.oauth.OAuthService;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

public class onlineCollector {

  private static final String API_HOST = "api.yelp.com";
  private static final String DEFAULT_TERM = "dinner";
  private static final String DEFAULT_LOCATION = "San Francisco, CA";
  private static final int SEARCH_LIMIT = 15;
  private static final String SEARCH_PATH = "/v2/search";
  private static final String BUSINESS_PATH = "/v2/business";


  private static final String CONSUMER_KEY = System.getenv("CONSUMER_KEY");
  private static final String CONSUMER_SECRET = System.getenv("CONSUMER_SECRET");
  private static final String TOKEN = System.getenv("TOKEN");
  private static final String TOKEN_SECRET = System.getenv("TOKEN_SECRET");

  OAuthService service;
  Token accessToken;


  public onlineCollector(String consumerKey, String consumerSecret, String token, String tokenSecret) {
    this.service =
        new ServiceBuilder().provider(TwoStepOAuth.class).apiKey(consumerKey)
            .apiSecret(consumerSecret).build();
    this.accessToken = new Token(token, tokenSecret);
  }

	  public onlineCollector() {
		// TODO Auto-generated constructor stub
	}


  public String searchForBusinessesByLocation(String term, String location) {
    OAuthRequest request = createOAuthRequest(SEARCH_PATH);
    request.addQuerystringParameter("term", term);
    request.addQuerystringParameter("location", location);
    request.addQuerystringParameter("limit", String.valueOf(SEARCH_LIMIT));
    return sendRequestAndGetResponse(request);
  }


  public String searchByBusinessId(String businessID) {
    OAuthRequest request = createOAuthRequest(BUSINESS_PATH + "/" + businessID);
    return sendRequestAndGetResponse(request);
  }

  private OAuthRequest createOAuthRequest(String path) {
    OAuthRequest request = new OAuthRequest(Verb.GET, "https://" + API_HOST + path);
    return request;
  }


  private String sendRequestAndGetResponse(OAuthRequest request) {
    System.out.println("Querying " + request.getCompleteUrl() + " ...");
    this.service.signRequest(this.accessToken, request);
    Response response = request.send();
    return response.getBody();
  }


  private static void queryAPI(onlineCollector yelpApi, YelpAPICLI yelpApiCli) {
    String searchResponseJSON =
        yelpApi.searchForBusinessesByLocation(yelpApiCli.term, yelpApiCli.location);

    JSONParser parser = new JSONParser();
    JSONObject response = null;
    try {
      response = (JSONObject) parser.parse(searchResponseJSON);
    } catch (ParseException pe) {
      System.out.println("Error: could not parse JSON response:");
      System.out.println(searchResponseJSON);
      System.exit(1);
    }

    JSONArray businesses = (JSONArray) response.get("businesses");
    for(int i=0;i<businesses.size(); i ++){
    JSONObject firstBusiness = (JSONObject) businesses.get(i);
    String firstBusinessID = firstBusiness.get("id").toString();
    System.out.println(String.format(
        "%s businesses found, querying business info for the top result \"%s\" ...",
        businesses.size(), firstBusinessID));

    // Select the first business and display business details
    String businessResponseJSON = yelpApi.searchByBusinessId(firstBusinessID.toString());
    System.out.println(String.format("Result for business \"%s\" found:", firstBusinessID));
    System.out.println(businessResponseJSON);
    }
  }

  private static class YelpAPICLI {
    @Parameter(names = {"-q", "--term"}, description = "Search Query Term")
    public String term = DEFAULT_TERM;

    @Parameter(names = {"-l", "--location"}, description = "Location to be Queried")
    public String location = DEFAULT_LOCATION;
  }


  public void getData() {
    YelpAPICLI yelpApiCli = new YelpAPICLI();
    new JCommander(yelpApiCli);

    onlineCollector yelpApi = new onlineCollector(CONSUMER_KEY, CONSUMER_SECRET, TOKEN, TOKEN_SECRET);
    queryAPI(yelpApi, yelpApiCli);
  }
}