package org.elasticsearch.river.twitter.handlers;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Date;
import java.util.Locale;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicLong;

import org.codehaus.jackson.Version;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.module.SimpleModule;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.river.RiverSettings;
import org.elasticsearch.river.twitter.connection.TwitterConnectionControl;
import org.elasticsearch.river.twitter.utils.TwitterFiltering;

import twitter4j.FilterQuery;

/**
 *
 */
public class CollectTweets {

	private final String oauthConsumerKey;
	private final String oauthConsumerSecret;
	private final String oauthAccessToken;
	private final String oauthAccessTokenSecret;

	private final boolean ignoreRetweet;
	private final boolean geoAsArray;
	private final boolean autoGenerateGeoPointFromPlace;
	private final boolean collectOnlyGeoTweets;

	private String insertIndexAliasName;
	private String indexName;

	private String typeName;

	private volatile boolean closed = false;

	private BufferedWriter bw = null;
	private final boolean writeTweetsOnFile;
	private final long maxNumTweetsEachFile;
	private static final long DEFAULT_NUM_TWEETS_EACH_FILE = 1000000;
	private String outFilePath = null;
	private long numTweetsInFile = 0;
	private String boundRegion;
	private String url;
	private  String username;
	private String password;
	private String containerName;
	
	private TwitterConnectionControl conn;

	private TwitterFiltering twitterFiltering;
	
	private AtomicLong numTweetsCollected;
	private AtomicLong numTweetsNotCollected;
	private AtomicLong numRepeatedTweets;

	@SuppressWarnings({ "unchecked", "resource" })
	public static void main(String[] args) throws IOException {
		if (args.length != 4 && args.length != 1) {
			System.out
					.println("You should enter the following arguments: [elasticsearch ip] [elasticsearch index name] [elasticsearch cluster name] [properties file path] when you want index on ES");
			System.out
			.println("You should enter the following arguments: [properties file path] when you want to write the tweets json in a file.");
			System.exit(0);
		}
		
		System.out.println("Default Locale  : " + Locale.getDefault());

		System.out.println("File Enconding  : " + System.getProperty("file.encoding"));
		
		String filePath = null;
		Client client = null;
		
		filePath = args[0];

		BufferedReader br = new BufferedReader(new FileReader(filePath));

		String json = "";
		String line = null;
		while ((line = br.readLine()) != null) {
			json += line;
		}

		br.close();
		ObjectMapper mapper = new ObjectMapper();
		SimpleModule module = new SimpleModule("PropertiesSerializer",
				new Version(1, 0, 0, null));
		mapper.registerModule(module);
		Map<String, Object> settings = (Map<String, Object>) mapper.readValue(
				json, Map.class);

		RiverSettings riverSettings = new RiverSettings(null, settings);

		CollectTweets twitter = new CollectTweets(riverSettings, client);
		twitter.start();
	}

	@SuppressWarnings({ "unchecked" })
	public CollectTweets(RiverSettings riverSettings, Client client) {
		
        String riverStreamType = null;
        
        if (riverSettings.settings().containsKey("twitter")) {
            Map<String, Object> twitterSettings = (Map<String, Object>) riverSettings.settings().get("twitter");

            writeTweetsOnFile = XContentMapValues.nodeBooleanValue(twitterSettings.get("write_tweets_to_file"), false);
            if(writeTweetsOnFile){
            	maxNumTweetsEachFile = XContentMapValues.nodeLongValue(twitterSettings.get("num_tweets_each_file"), DEFAULT_NUM_TWEETS_EACH_FILE);
        		boundRegion = XContentMapValues.nodeStringValue(twitterSettings.get("bound_region_name"), "World");
            	outFilePath = boundRegion +"-tweets-" +System.currentTimeMillis();
            	
            	Map<String, Object> fileStorageSettings = (Map<String, Object>)twitterSettings.get("file_storage");
            	
            	if(fileStorageSettings != null) {
	            	url = XContentMapValues.nodeStringValue(fileStorageSettings.get("url"), null);
	            	username = XContentMapValues.nodeStringValue(fileStorageSettings.get("username"), null);
	            	password = XContentMapValues.nodeStringValue(fileStorageSettings.get("password"), null);
	            	containerName = XContentMapValues.nodeStringValue(fileStorageSettings.get("container_name"), null);
            	} else {
            		System.out.println("The file_storage was not setted. The output files with tweets will be generated only locally.");
            	}
            	
            	try {
                	bw = new BufferedWriter(new FileWriter(outFilePath));
				} catch (Exception e) {
					e.printStackTrace();
					System.exit(0);
				}
            } else {
            	maxNumTweetsEachFile = DEFAULT_NUM_TWEETS_EACH_FILE;
            }
            
            
            autoGenerateGeoPointFromPlace = XContentMapValues.nodeBooleanValue(twitterSettings.get("auto_generate_geo_point_from_place"), true);
            collectOnlyGeoTweets = XContentMapValues.nodeBooleanValue(twitterSettings.get("collect_only_geo_tweets"), false);
            ignoreRetweet = XContentMapValues.nodeBooleanValue(twitterSettings.get("ignore_retweet"), false);
            geoAsArray = XContentMapValues.nodeBooleanValue(twitterSettings.get("geo_as_array"), false);
            
            if (twitterSettings.containsKey("oauth")) {
                Map<String, Object> oauth = (Map<String, Object>) twitterSettings.get("oauth");
                
                if (oauth.containsKey("consumer_key")) {
                    oauthConsumerKey = XContentMapValues.nodeStringValue(oauth.get("consumer_key"), null);
                } else {
                	oauthConsumerKey = null;
                	System.err.println("Property river.twitter.oauth.consumer_key was not found");
                	System.exit(0);
                }
                
                if (oauth.containsKey("consumer_secret")) {
                    oauthConsumerSecret = XContentMapValues.nodeStringValue(oauth.get("consumer_secret"), null);
                } else {
                	oauthConsumerSecret = null;
                	System.err.println("Property river.twitter.oauth.consumer_secret was not found");
                	System.exit(0);
                }
                
                if (oauth.containsKey("access_token")) {
                    oauthAccessToken = XContentMapValues.nodeStringValue(oauth.get("access_token"), null);
                } else {
                	oauthAccessToken = null;
                	System.err.println("Property river.twitter.oauth.access_token was not found");
                	System.exit(0);
                }
                if (oauth.containsKey("access_token_secret")) {
                    oauthAccessTokenSecret = XContentMapValues.nodeStringValue(oauth.get("access_token_secret"), null);
                } else {
                	oauthAccessTokenSecret = null;
                	System.err.println("Property river.twitter.oauth.access_token_secret was not found");
                	System.exit(0);
                }
            } else {
            	oauthConsumerKey = null;
                oauthConsumerSecret = null;
                oauthAccessToken = null;
                oauthAccessTokenSecret = null;
            	System.err.println("Oauth properties was not found. Please, configure the properties with 'river.twitter.oauth.access_token_secret', "
            			+ "'river.twitter.oauth.access_token', 'river.twitter.oauth.consumer_secret' and river.twitter.oauth.consumer_key'");
            	System.exit(0);
            }

            riverStreamType = XContentMapValues.nodeStringValue(twitterSettings.get("type"), "sample");
            Map<String, Object> filterSettings = (Map<String, Object>) twitterSettings.get("filter");

            if (riverStreamType.equals("filter") && filterSettings == null) {
                System.exit(0);
            }

            if (filterSettings != null) {
                riverStreamType = "filter";
                
                conn = new TwitterConnectionControl(closed, riverStreamType,
            			oauthConsumerKey,
            			oauthConsumerSecret, oauthAccessToken,
            			oauthAccessTokenSecret);
                
                twitterFiltering = new TwitterFiltering(filterSettings, conn);
                boolean filterSet = false;
                
                //filter tracks
                Object tracks = filterSettings.get("tracks");
                if (tracks != null) {
                	twitterFiltering.filterTrack(tracks);
                    filterSet = true;
                }
                
                //filter follow
                Object follow = filterSettings.get("follow");
                if (follow != null) {
                	twitterFiltering.filterFollow(follow);
                    filterSet = true;
                }
                
                //filter locations
                Object locations = filterSettings.get("locations");
                if (locations != null) {
                	twitterFiltering.filterLocation(locations);
                    filterSet = true;
                }
                
                //filter user lists
                Object userLists = filterSettings.get("user_lists");
                if (userLists != null) {
                    twitterFiltering.filterUserLists(userLists);
                    filterSet = true;
                }

                // We should have something to filter
                if (!filterSet) {
                    System.exit(0);
                }

                //filter by language
                Object language = filterSettings.get("language");
                if (language != null) {
                    twitterFiltering.filterLanguage(language);
                }
                
            } else {
            	 conn = new TwitterConnectionControl(closed, riverStreamType,
             			oauthConsumerKey,
             			oauthConsumerSecret, oauthAccessToken,
             			oauthAccessTokenSecret);
            }
            
        } else {
        	autoGenerateGeoPointFromPlace = false;
            collectOnlyGeoTweets = false;
            ignoreRetweet = false;
            geoAsArray = false;
            writeTweetsOnFile = false;
            maxNumTweetsEachFile = DEFAULT_NUM_TWEETS_EACH_FILE;
            oauthConsumerKey = null;
            oauthConsumerSecret = null;
            oauthAccessToken = null;
            oauthAccessTokenSecret = null;
        	System.err.println("The 'twitter' property was not found.");
        	System.exit(0);
        }

        if (oauthAccessToken == null || oauthConsumerKey == null || oauthConsumerSecret == null || oauthAccessTokenSecret == null) {
        	System.err.println("Error to get the authentication properties");
        	System.exit(0);
        }

        numTweetsCollected = new AtomicLong(0);
        numTweetsNotCollected = new AtomicLong(0);
        numRepeatedTweets = new AtomicLong(0);
    }

	public void start() {
		//Collect twitter metrics about tweets collected and limited by Twitter.
		final int time = 60000;
		Timer timer = new Timer("metricsTimer");
		timer.schedule(new TimerTask() {
			@Override
			public void run() {
				System.out.println(new Date().toString() +".Tweets Collected: " +numTweetsCollected +". Tweets NOT Collected: " +numTweetsNotCollected +". Repeated tweets: " +numRepeatedTweets +" in a minute. ");
				numTweetsCollected.set(0);
				numTweetsNotCollected.set(0);
				numRepeatedTweets.set(0);
			}
		}, time, time);

		//If is only to write tweets on File, so start the twitter stream.
		FilterQuery filterQuery = null;
		if(twitterFiltering != null)
			filterQuery = twitterFiltering.getFilterQuery();
		
		if(writeTweetsOnFile) {
			FileStatusHandler fileStatusHandler = new FileStatusHandler(bw, maxNumTweetsEachFile, outFilePath, numTweetsInFile, 
					boundRegion, conn, numTweetsCollected, numTweetsNotCollected, numRepeatedTweets, url, username, password, containerName);
			if(!conn.startTwitterStream(fileStatusHandler, filterQuery)){
				System.err.println("Error to init twitter stream");
				System.exit(0);
			}
			return;
		}
		

		conn.setBulkProcessor(null);
		
		StatusHandler statusHandler = new StatusHandler(writeTweetsOnFile, collectOnlyGeoTweets, autoGenerateGeoPointFromPlace, 
				ignoreRetweet, geoAsArray, insertIndexAliasName, typeName, null, indexName, conn, numTweetsCollected, numTweetsNotCollected, numRepeatedTweets);
		
		
		if(!conn.startTwitterStream(statusHandler, filterQuery)){
			System.err.println("Error to init twitter stream");
			System.exit(0);
		}
	}

	
}
