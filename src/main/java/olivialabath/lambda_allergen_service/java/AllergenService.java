package olivialabath.lambda_allergen_service.java;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.text.ParseException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.model.ConditionalCheckFailedException;
import com.amazonaws.services.lambda.runtime.Context; 
import com.amazonaws.services.lambda.runtime.RequestHandler;

import twitter4j.Paging;
import twitter4j.Status;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import twitter4j.conf.ConfigurationBuilder;

public class AllergenService implements RequestHandler<Map<String, Integer[]>, Allergen[]>{

	private DynamoDB dynamoDb;
	private String DYNAMO_DB_TABLE_NAME = "Allergens";
	private Regions REGION = Regions.US_EAST_2;
	private AmazonDynamoDB client;
	
	private static DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("EEE MMM d HH:mm:ss z yyyy");
	public static final HashMap<String, String> AllergenMap;
	static {
		// marsh elder and red juniper berry are the only 2 where key != value
		AllergenMap = new HashMap<String, String>();
		AllergenMap.put("Mold", "Mold");
		AllergenMap.put("Grass", "Grass");
		AllergenMap.put("Pigweed", "Pigweed");
		AllergenMap.put("Ragweed", "Ragweed");
		AllergenMap.put("Marsh", "Marsh Elder");
		AllergenMap.put("Cedar", "Cedar");
		AllergenMap.put("Elm", "Elm");
		AllergenMap.put("Oak", "Oak");
		AllergenMap.put("Ash", "Ash");
		AllergenMap.put("Mesquite", "Mesquite");
		AllergenMap.put("Pecan", "Pecan");
		AllergenMap.put("Privet", "Privet");
		AllergenMap.put("Sycamore", "Sycamore");
		AllergenMap.put("Mulberry", "Mulberry");
		AllergenMap.put("Willow", "Willow");
		AllergenMap.put("Juniper", "Red Juniper Berry");
		AllergenMap.put("Sage", "Sage");
		AllergenMap.put("Acacia", "Acacia");
		AllergenMap.put("Birch", "Birch");
		AllergenMap.put("Hackberry", "Hackberry");
		AllergenMap.put("Poplar", "Poplar");
		AllergenMap.put("Cottonwood", "Cottonwood");
		AllergenMap.put("Pine", "Pine");
	}
	
	
	public Allergen[] handleRequest(Map<String, Integer[]> input, Context context) {
		Integer[] paging = input.get("paging");
		System.out.println("page: " + paging[0] + ", count: " + paging[1]);
		
		Twitter twitter = initTwitter();
		
		List<Allergen> allergens = getAllergens(twitter, paging[0], paging[1]);
		
		initDynamoDBClient();

		persistData(allergens);
		
		return allergens.toArray(new Allergen[allergens.size()]);
	}
	
	public List<Allergen> getAllergens(Twitter twitter, int page, int count){
		List<Allergen> allergens = new ArrayList<Allergen>();
		List<Status> statuses = new ArrayList<Status>();
		try {
			// gets the page, which contains count number of tweets
			statuses = twitter.getUserTimeline("ATXPollen", new Paging(page, count));
			System.out.println("number of statuses: " + statuses.size());
			// parse each status
			for(Status s : statuses) {
				allergens.addAll(parseStatus(s));
			}
		} catch (TwitterException e) {
			e.printStackTrace();
		}
		
		System.out.println("number of allergens: " + allergens.size());
		
		return allergens;
	}

	public Twitter initTwitter() {
		ConfigurationBuilder cb = new ConfigurationBuilder();
		cb.setDebugEnabled(true)
			.setTweetModeExtended(true)
			.setOAuthConsumerKey(Config.consumer_key)
			.setOAuthConsumerSecret(Config.consumer_secret)
			.setOAuthAccessToken(Config.access_token)
			.setOAuthAccessTokenSecret(Config.access_token_secret);
		TwitterFactory tf = new TwitterFactory(cb.build());
		return tf.getInstance();
	}
	
	public List<Allergen> parseStatus(Status status) {
		List<Allergen> allergens = new ArrayList<Allergen>();
		
		// get the status's date
		LocalDate date = LocalDate.now();
		date = LocalDate.parse(status.getCreatedAt().toString(), dateTimeFormatter);
		
		// parse the text
		String[] text = status.getText().replaceAll("\\p{P}", "").split("\\s"); // remove all punctuation and split at white spaces
//		System.out.println("split string = " + Arrays.toString(text));
		
		boolean traceFlag = false;
		boolean allergenFlag = false;
		int start = 0;
		int end = 0;
		for(int i = 0; i < text.length; ++i) {
			
			// if the word is "Traces", set the traceFlag and go to next word
			if(text[i].equals("Traces") || text[i].equals("Trace")) { 
				traceFlag = true;
			}
			else if(text[i].equals("H") || text[i].equals("M") || text[i].equals("L") || text[i].equals("of") || text[i].equals("and")) {
				// do nothing
			}
			// set allergenFlag and the starting index if the a potential allergen
			// or set of allergens is found
			else if(AllergenMap.containsKey(text[i]) && !traceFlag && !allergenFlag) {
//				System.out.println("start = " + i);
				start = i;
				allergenFlag = true;
			}
			// reset the allergenFlag and the ending index if a number is found
			else if(isInteger(text[i]) && allergenFlag) {
//				System.out.println("end = " + i);
				end = i;
				allergenFlag = false;
				List<Allergen> tempAllergens = parseSubset(Arrays.copyOfRange(text, start, end + 1), date);
				if(tempAllergens.size() > 0 && tempAllergens.get(0).count > 0)
					allergens.addAll(tempAllergens);
			}
			// if the traceFlag is set and an allergen is found, add it to the list
			else if(traceFlag && AllergenMap.containsKey(text[i])) {
				allergens.add(new Allergen(date.toEpochDay(), AllergenMap.get(text[i]), 5));
			}
		}
		
		return allergens;
	}
	
	public List<Allergen> parseSubset(String[] text, LocalDate subsetDate){
		List<Allergen> allergens = new ArrayList<Allergen>();
		int subsetCount = -1;
//		System.out.println(Arrays.toString(text));
		
		for(int i = 0; i < text.length; i++) {
			// if the word is a valid allergen, add it to allergens
			if(AllergenMap.containsKey(text[i])) {
				Allergen a = new Allergen();
				a.setName(AllergenMap.get(text[i]));
				allergens.add(a);
			}
			else if(text[i].equals("&") || text[i].equals("and")) {
				// do nothing
//				System.out.println("and or & detected");
			}
			// if the word is a number, set subset count as it
			else if(isInteger(text[i])) {
				Integer temp = Integer.parseInt(text[i]);
				subsetCount = temp.intValue();
			}
			// if the word is not an allergen or the word &/and, return prematurely
			// subset count will be -1, which signals that no valid count was found
			else if(!AllergenMap.containsKey(text[i])) {
//				System.out.println("exiting early");
				return allergens;
			}
		}
		
		for(Allergen a : allergens) {
			a.setDate(subsetDate.toEpochDay());
			a.setCount(subsetCount);
		}
		
//		System.out.println(Arrays.toString(allergens.toArray()));
		
		return allergens;
	}

	public static boolean isInteger(String s) {
	    if(s.isEmpty()) return false;
	    for(int i = 0; i < s.length(); i++) {
	        if(i == 0 && s.charAt(i) == '-') {
	            if(s.length() == 1) return false;
	            else continue;
	        }
	        if(Character.digit(s.charAt(i), 10) < 0) return false;
	    }
	    return true;
	}
	
	private void persistData(List<Allergen> allergens) throws ConditionalCheckFailedException {
		DynamoDBMapper mapper = new DynamoDBMapper(client);
		mapper.batchSave(allergens);
	}
	
	private void initDynamoDBClient() {
		client = AmazonDynamoDBClientBuilder.standard()
				.withRegion(REGION).build();
        this.dynamoDb = new DynamoDB(client);
	}

}