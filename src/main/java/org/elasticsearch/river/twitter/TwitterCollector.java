package org.elasticsearch.river.twitter;

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
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.ImmutableSettings.Builder;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.river.RiverSettings;
import org.elasticsearch.river.twitter.connection.TwitterConnectionControl;
import org.elasticsearch.river.twitter.handlers.FileStatusHandler;
import org.elasticsearch.river.twitter.handlers.StatusHandler;
import org.elasticsearch.river.twitter.utils.TwitterFiltering;

/**
 *
 */
public class TwitterCollector {

	private Client client;

	private final String oauthConsumerKey;
	private final String oauthConsumerSecret;
	private final String oauthAccessToken;
	private final String oauthAccessTokenSecret;

	private final boolean ignoreRetweet;
	private final boolean geoAsArray;
	private final boolean autoGenerateGeoPointFromPlace;
	private final boolean collectOnlyGeoTweets;

	private String queryIndexAliasName;
	private String queryAllIndexAliasName;
	private String insertIndexAliasName;
	private String indexName;

	private String typeName;

	private static final long DEFAULT_MAX_EACH_INDEX_SIZE = 100000000;
	private static final long DEFAULT_MAX_INDEX_SIZE = 10000000000L;
	private static final int DEFAULT_BULK_SIZE = 1000;
	private static final int DEFAULT_REFRESH_TIME = 30;
	private static final int DEFAULT_NUM_SHARDS = 5;
	private static final int DEFAULT_REPLICATION_FACTOR = 1;
	private static final int DEFAULT_NUM_INDEXES_TO_QUERY = 3;
	private final long maxEachAliasIndexSize;
	private final long maxIndexSize;
	private final int bulkSize;
	private final int maxConcurrentBulk;
	private final TimeValue bulkFlushInterval;
	private final int numShards;
	private final int replicationFactor;
	private final int numIndexesToQuery;

	private final boolean masterNode;

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
	
	private TwitterElasticSearchIndexing elasticSearchIndexing;
	
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
		
		if(args.length == 4) {
			
			// WARN Configurações do welder
			String seed = args[0];
			String layerName = args[1];
			String elasticCluster = args[2];
			filePath = args[3];
			Builder builder = ImmutableSettings.settingsBuilder().put(
					"cluster.name", elasticCluster);
			client = new TransportClient(builder.build())
					.addTransportAddresses(new InetSocketTransportAddress(seed,
							9300));
			
			System.out.println("ElasticSearch IP: " + seed);
			System.out.println("Layer name: " + layerName);
			System.out.println("Elastic cluster: " + elasticCluster);
		} else
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

		TwitterCollector twitter = new TwitterCollector(riverSettings, client);
		twitter.start();
	}

	@SuppressWarnings({ "unchecked" })
	public TwitterCollector(RiverSettings riverSettings, Client client) {
		
        this.client = client;

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
            masterNode = XContentMapValues.nodeBooleanValue(twitterSettings.get("master_node"), false);
            
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
            masterNode = false;
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

        if (riverSettings.settings().containsKey("index")) {
            Map<String, Object> indexSettings = (Map<String, Object>) riverSettings.settings().get("index");
            indexName = XContentMapValues.nodeStringValue(indexSettings.get("index"), "twitter");
            queryIndexAliasName = XContentMapValues.nodeStringValue(indexSettings.get("query_alias"), indexName);
            queryAllIndexAliasName = XContentMapValues.nodeStringValue(indexSettings.get("query_all_index_alias"), indexName);
            insertIndexAliasName = queryAllIndexAliasName +"_index";
            maxEachAliasIndexSize = XContentMapValues.nodeLongValue(indexSettings.get("max_each_alias_index_size"), DEFAULT_MAX_EACH_INDEX_SIZE);
            maxIndexSize = XContentMapValues.nodeLongValue(indexSettings.get("max_index_size"), DEFAULT_MAX_INDEX_SIZE);
            typeName = XContentMapValues.nodeStringValue(indexSettings.get("type"), "attr");
            this.bulkSize = XContentMapValues.nodeIntegerValue(indexSettings.get("bulk_size"), 100);
            this.bulkFlushInterval = TimeValue.parseTimeValue(XContentMapValues.nodeStringValue(
                    indexSettings.get("flush_interval"), "5s"), TimeValue.timeValueSeconds(DEFAULT_REFRESH_TIME));
            this.maxConcurrentBulk = XContentMapValues.nodeIntegerValue(indexSettings.get("max_concurrent_bulk"), 1);
            numShards = XContentMapValues.nodeIntegerValue(indexSettings.get("num_shards"), DEFAULT_NUM_SHARDS);
            replicationFactor = XContentMapValues.nodeIntegerValue(indexSettings.get("replication_factor"), DEFAULT_REPLICATION_FACTOR);
            numIndexesToQuery = XContentMapValues.nodeIntegerValue(indexSettings.get("num_indexes_to_query"), DEFAULT_NUM_INDEXES_TO_QUERY);
        } else {
            indexName = "twitter";
            queryIndexAliasName = indexName;
            queryAllIndexAliasName = indexName;
            insertIndexAliasName = queryAllIndexAliasName +"_index";
            maxEachAliasIndexSize = DEFAULT_MAX_EACH_INDEX_SIZE;
            maxIndexSize = DEFAULT_MAX_INDEX_SIZE;
            typeName = "attr";
            bulkSize = DEFAULT_BULK_SIZE;
            numShards = DEFAULT_NUM_SHARDS;
            replicationFactor = DEFAULT_REPLICATION_FACTOR;
            this.maxConcurrentBulk = 1;
            this.bulkFlushInterval = TimeValue.timeValueSeconds(DEFAULT_REFRESH_TIME);
            numIndexesToQuery = DEFAULT_NUM_INDEXES_TO_QUERY;
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
		if(writeTweetsOnFile) {
			FileStatusHandler fileStatusHandler = new FileStatusHandler(bw, maxNumTweetsEachFile, outFilePath, numTweetsInFile, 
					boundRegion, conn, numTweetsCollected, numTweetsNotCollected, numRepeatedTweets, url, username, password, containerName);
			if(!conn.startTwitterStream(fileStatusHandler, twitterFiltering.getFilterQuery())){
				System.err.println("Error to init twitter stream");
				System.exit(0);
			}
			return;
		}
		
		try {
			// wait for cluster health status yellow until 2 minutes
			if (client.admin().cluster().health(new ClusterHealthRequest())
					.get().getStatus().equals(ClusterHealthStatus.RED)) {
				client.admin().cluster().prepareHealth()
						.setWaitForYellowStatus()
						.setTimeout(TimeValue.timeValueMinutes(2)).get();
			}

			elasticSearchIndexing = new TwitterElasticSearchIndexing(client,
					queryIndexAliasName, insertIndexAliasName, indexName,
					typeName, maxEachAliasIndexSize, maxIndexSize, numShards,
					replicationFactor, numIndexesToQuery, queryAllIndexAliasName);
			
			/*
			 * The master node controls the index size and the time to create a
			 * new index. The slave only insert twitter date in index.
			 */
			if (masterNode)
				elasticSearchIndexing.initializeMasterIndexControl();
			else
				elasticSearchIndexing.initializeSlaveIndexControl();

		} catch (Exception e) {
			e.printStackTrace();
			System.exit(0);
		}

		// Creating bulk processor
		BulkProcessor bulkProcessor = BulkProcessor
				.builder(client, new BulkProcessor.Listener() {
					@Override
					public void beforeBulk(long executionId, BulkRequest request) {
					}

					@Override
					public void afterBulk(long executionId,
							BulkRequest request, BulkResponse response) {
						if(masterNode) {
							// verify if the size of the index is greater than the allowed
							elasticSearchIndexing.verifyIndexSize(request.numberOfActions()); 
						}
					}

					@Override
					public void afterBulk(long executionId,
							BulkRequest request, Throwable failure) {
					}
				}).setBulkActions(bulkSize)
				.setConcurrentRequests(maxConcurrentBulk)
				.setFlushInterval(bulkFlushInterval).build();

		conn.setBulkProcessor(bulkProcessor);
		
		StatusHandler statusHandler = new StatusHandler(writeTweetsOnFile, collectOnlyGeoTweets, autoGenerateGeoPointFromPlace, 
				ignoreRetweet, geoAsArray, insertIndexAliasName, typeName, bulkProcessor, indexName, conn, numTweetsCollected, numTweetsNotCollected, numRepeatedTweets);
		
		if(!conn.startTwitterStream(statusHandler, twitterFiltering.getFilterQuery())){
			System.err.println("Error to init twitter stream");
			System.exit(0);
		}
	}

	
}
