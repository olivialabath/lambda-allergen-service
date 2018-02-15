package olivialabath.lambda_allergen_service.java;

import java.util.ArrayList;
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
	private static final String[] NamePatterns = {"(?i)(.*mold.*)", "(?i)(.*grass.*)", "(?i)(pig ?+weed)", "(?i)(rag ?+weed)",
            "(?i)(.*marsh.*)", "(?i)(.*cedar.*)", "(?i)(.*elm.*)", "(?i)(.*oak.*)",
            "(?i)(.*ash.*)", "(?i)(.*mesquite.*)", "(?i)(.*pecan.*)", "(?i)(.*privet.*)",
            "(?i)(.*sycamore.*)", "(?i)(.*mulberry.*)", "(?i)(.*willow.*)", "(?i)(.*juniper.*)",
            "(?i)(.*sage.*)"};
	public static final String[] AllergenNames = {"Mold", "Grass", "Pigweed", "Ragweed", "Marsh Elder", "Cedar",
            "Elm", "Oak", "Ash", "Mesquite", "Pecan", "Privet", "Sycamore", "Mulberry", "Willow",
            "Red Juniper Berry", "Sage"};
	
	public Allergen[] handleRequest(Map<String, Integer[]> input, Context context) {
		Integer[] paging = input.get("paging");
		System.out.println("pages: " + paging[0] + ", count: " + paging[1]);
		
		Twitter twitter = initTwitter();
		
		List<Allergen> allergens = getAllergens(twitter, paging[0], paging[1]);
		
		initDynamoDBClient();

		persistData(allergens);
		
		return allergens.toArray(new Allergen[allergens.size()]);
	}
	
	public List<Allergen> getAllergens(Twitter twitter, int pages, int count){
		List<Allergen> allergens = new ArrayList<Allergen>();
		List<Status> statuses = new ArrayList<Status>();
		try {
			// get the most recent pagingCount number of tweets
			statuses = twitter.getUserTimeline("ATXPollen", new Paging(pages, count));
			
			// 
			for(Status s : statuses) {
				allergens.addAll(parseStatus(s));
			}
		} catch (TwitterException e) {
			e.printStackTrace();
		} catch (ParseException e) {
			e.printStackTrace();
		}
		
		return allergens;
	}

	public Twitter initTwitter() {
		ConfigurationBuilder cb = new ConfigurationBuilder();
		cb.setDebugEnabled(true)
			.setOAuthConsumerKey(Config.consumer_key)
			.setOAuthConsumerSecret(Config.consumer_secret)
			.setOAuthAccessToken(Config.access_token)
			.setOAuthAccessTokenSecret(Config.access_token_secret);
		TwitterFactory tf = new TwitterFactory(cb.build());
		return tf.getInstance();
	}
	
	public List<Allergen> parseStatus(Status status) throws ParseException{
		List<Allergen> allergens = new ArrayList<Allergen>();
		
		// get the status's date
		LocalDate date = LocalDate.now();
		date = LocalDate.parse(status.getCreatedAt().toString(), dateTimeFormatter);
		
		// parse the text
		String[] text = status.getText().replaceAll("\\p{P}", "").split("\\s"); // remove all punctuation and split at white spaces
		boolean traceFlag = false;
		for(int i = 0; i < text.length; ++i) {
			
			// if the word is "Traces, set the traceFlag and go to next word
			if(text[i].equals("Traces")) { 
				traceFlag = true;
			}
			
			// otherwise, check the word against a regex pattern, and create
			// a new Allergen if it's a valid allergen name
			else {
				for(int j = 0; j < NamePatterns.length; ++j) {
					if (text[i].matches(NamePatterns[j]) && traceFlag){
						allergens.add(new Allergen(date.toEpochDay(), AllergenNames[j], 5));
					} else if (text[i].matches(NamePatterns[j]) && i + 1 < text.length && isInteger(text[i + 1])) {
						allergens.add(new Allergen(date.toEpochDay(), AllergenNames[j], Integer.parseInt(text[++i])));
					}
				}
			}
		}
		
		return allergens;
	}
	
	public static boolean isInteger(String s) {
		try { 
	        Integer.parseInt(s); 
	    } catch(NumberFormatException e) { 
	        return false; 
	    } catch(NullPointerException e) {
	        return false;
	    }
	    // only got here if we didn't return false
	    return true;
	}

	
	private void persistData(List<Allergen> allergens) throws ConditionalCheckFailedException {
		DynamoDBMapper mapper = new DynamoDBMapper(client);
		for(Allergen a : allergens) {
			mapper.save(a);
		}
	}
	
	private void initDynamoDBClient() {
		client = AmazonDynamoDBClientBuilder.standard()
				.withRegion(REGION).build();
        this.dynamoDb = new DynamoDB(client);
	}

}