package org.elasticsearch.river.twitter;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;

import org.codehaus.jackson.Version;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.module.SimpleModule;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.exists.AliasesExistResponse;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsRequest;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.hppc.cursors.ObjectCursor;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.ImmutableSettings.Builder;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.indices.IndexAlreadyExistsException;
import org.elasticsearch.river.RiverSettings;
import org.elasticsearch.river.twitter.connection.TwitterConnectionControl;
import org.elasticsearch.river.twitter.handlers.FileStatusHandler;
import org.elasticsearch.river.twitter.handlers.StatusHandler;
import org.elasticsearch.river.twitter.utils.TwitterFiltering;
import org.elasticsearch.river.twitter.utils.TwitterInfo;

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
	private String insertIndexAliasName;
	private String indexName;

	private String typeName;

	private static final long DEFAULT_MAX_EACH_INDEX_SIZE = 100000000;
	private static final long DEFAULT_MAX_INDEX_SIZE = 10000000000L;
	private static final int DEFAULT_BULK_SIZE = 1000;
	private static final int DEFAULT_REFRESH_TIME = 30;
	private static final int DEFAULT_NUM_SHARDS = 5;
	private static final int DEFAULT_REPLICATION_FACTOR = 1;
	private final long maxEachAliasIndexSize;
	private final long maxIndexSize;
	private final int bulkSize;
	private final int maxConcurrentBulk;
	private final TimeValue bulkFlushInterval;
	private final int numShards;
	private final int replicationFactor;

	private final boolean masterNode;

	private volatile boolean closed = false;

	private int indexVersion = 1;
	private long indexSize = 0;
	private long totalIndexSize = 0;

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

	@SuppressWarnings({ "unchecked", "resource" })
	public static void main(String[] args) throws IOException {
		if (args.length != 4 && args.length != 1) {
			System.out
					.println("You should enter the following arguments: [elasticsearch ip] [elasticsearch index name] [elasticsearch cluster name] [properties file path] when you want index on ES");
			System.out
			.println("You should enter the following arguments: [properties file path] when you want to write the tweets json in a file.");
			System.exit(0);
		}
		
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
            maxEachAliasIndexSize = XContentMapValues.nodeLongValue(indexSettings.get("max_each_alias_index_size"), DEFAULT_MAX_EACH_INDEX_SIZE);
            maxIndexSize = XContentMapValues.nodeLongValue(indexSettings.get("max_index_size"), DEFAULT_MAX_INDEX_SIZE);
            typeName = XContentMapValues.nodeStringValue(indexSettings.get("type"), "status");
            this.bulkSize = XContentMapValues.nodeIntegerValue(indexSettings.get("bulk_size"), 100);
            this.bulkFlushInterval = TimeValue.parseTimeValue(XContentMapValues.nodeStringValue(
                    indexSettings.get("flush_interval"), "5s"), TimeValue.timeValueSeconds(DEFAULT_REFRESH_TIME));
            this.maxConcurrentBulk = XContentMapValues.nodeIntegerValue(indexSettings.get("max_concurrent_bulk"), 1);
            numShards = XContentMapValues.nodeIntegerValue(indexSettings.get("num_shards"), DEFAULT_NUM_SHARDS);
            replicationFactor = XContentMapValues.nodeIntegerValue(indexSettings.get("replication_factor"), DEFAULT_REPLICATION_FACTOR);
        } else {
            indexName = "twitter";
            maxEachAliasIndexSize = DEFAULT_MAX_EACH_INDEX_SIZE;
            maxIndexSize = DEFAULT_MAX_INDEX_SIZE;
            typeName = "status";
            bulkSize = DEFAULT_BULK_SIZE;
            numShards = DEFAULT_NUM_SHARDS;
            replicationFactor = DEFAULT_REPLICATION_FACTOR;
            this.maxConcurrentBulk = 1;
            this.bulkFlushInterval = TimeValue.timeValueSeconds(DEFAULT_REFRESH_TIME);
        }

        insertIndexAliasName = indexName +"_index";
        queryIndexAliasName = indexName;
        
        numTweetsCollected = new AtomicLong(0);
        numTweetsNotCollected = new AtomicLong(0);
    }

	public void start() {
		//Collect twitter metrics about tweets collected and limited by Twitter.
		final int time = 60000;
		Timer timer = new Timer("metricsTimer");
		timer.schedule(new TimerTask() {
			@Override
			public void run() {
				System.out.println(new Date().toString() +".Tweets Collected: " +numTweetsCollected +". Tweets NOT Collected: " +numTweetsNotCollected +" in a minute.");
				numTweetsCollected.set(0);
				numTweetsNotCollected.set(0);
			}
		}, time, time);

		//If is only to write tweets on File, so start the twitter stream.
		if(writeTweetsOnFile) {
			FileStatusHandler fileStatusHandler = new FileStatusHandler(bw, maxNumTweetsEachFile, outFilePath, numTweetsInFile, 
					boundRegion, conn, numTweetsCollected, numTweetsNotCollected, url, username, password, containerName);
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

			/*
			 * The master node controls the index size and the time to create a
			 * new index. The slave only insert twitter date in index.
			 */
			if (masterNode)
				initializeMasterIndexControl();
			else
				initializeSlaveIndexControl();

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
							indexSize += request.numberOfActions();
							totalIndexSize += request.numberOfActions();
							// verify if the size of the index is greater than the allowed
							verifyIndexSize(); 
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
				ignoreRetweet, geoAsArray, insertIndexAliasName, typeName, bulkProcessor, indexName, conn, numTweetsCollected, numTweetsNotCollected);
		
		if(!conn.startTwitterStream(statusHandler, twitterFiltering.getFilterQuery())){
			System.err.println("Error to init twitter stream");
			System.exit(0);
		}
	}

	private void initializeSlaveIndexControl() throws InterruptedException,
			ExecutionException {
		boolean indiceExist = client.admin().indices()
				.exists(new IndicesExistsRequest(indexName)).get().isExists();

		if (!indiceExist) {
			String queryIndexAliasName = this.indexName;
			String indexName = this.indexName + "_1";
			Settings indexSettings = ImmutableSettings.settingsBuilder()
					.put("number_of_shards", numShards)
					.put("number_of_replicas", replicationFactor).build();
			
			client.admin().indices().prepareCreate(indexName)
					.setSettings(indexSettings).execute().actionGet();
			client.admin().indices().aliases(new IndicesAliasesRequest().addAlias(
									insertIndexAliasName, indexName)).actionGet();
			client.admin().indices().aliases(new IndicesAliasesRequest().addAlias(
									queryIndexAliasName, indexName)).actionGet();

			if (client.admin().indices().prepareGetMappings(indexName)
					.setTypes(typeName).get().getMappings().isEmpty()) {
				createMapping();
			}
		}
	}

	private void initializeMasterIndexControl() {
		try {
			int[] versions = getMinAndMaxVersion();
			if (versions == null) {
				indexName = indexName + "_" + indexVersion;
				Settings indexSettings = ImmutableSettings.settingsBuilder()
						.put("number_of_shards", numShards)
						.put("number_of_replicas", replicationFactor).build();
				client.admin().indices().prepareCreate(indexName)
						.setSettings(indexSettings).execute().actionGet();
				client.admin()
						.indices()
						.aliases(
								new IndicesAliasesRequest().addAlias(
										insertIndexAliasName, indexName))
						.actionGet();
				client.admin()
						.indices()
						.aliases(
								new IndicesAliasesRequest().addAlias(
										queryIndexAliasName, indexName))
						.actionGet();

				if (client.admin().indices().prepareGetMappings(indexName)
						.setTypes(typeName).get().getMappings().isEmpty()) {
					createMapping();
				}
			} else {
				indexVersion = getMinAndMaxVersion()[1]; // get latest index
															// version

				indexName = queryIndexAliasName + "_" + indexVersion;
				indexSize = getIndexSize(indexName);
				totalIndexSize = getIndexSize(queryIndexAliasName);
				verifyAlias();
			}
		} catch (Exception e) {
			if (ExceptionsHelper.unwrapCause(e) instanceof IndexAlreadyExistsException) {
				indexVersion = getMinAndMaxVersion()[1]; // get latest index
															// version

				indexName = queryIndexAliasName + "_" + indexVersion;
				indexSize = getIndexSize(indexName);
				totalIndexSize = getIndexSize(queryIndexAliasName);
				verifyAlias();

			} else if (ExceptionsHelper.unwrapCause(e) instanceof ClusterBlockException) {
			} else {
				e.printStackTrace();
				System.exit(0);
			}
		}
	}

	private void verifyAlias() {
		try {
			AliasesExistResponse response = client.admin().indices()
					.aliasesExist(new GetAliasesRequest(insertIndexAliasName)
									.indices(indexName)).get();
			if (!response.exists())
				client.admin().indices().aliases(new IndicesAliasesRequest().addAlias(
										insertIndexAliasName, indexName)).actionGet();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (ExecutionException e) {
			e.printStackTrace();
		}
	}

	private long getIndexSize(String indexName) {
		IndicesStatsResponse stats = client
				.admin()
				.indices()
				.stats(new IndicesStatsRequest().clear().docs(true)
						.indices(indexName)).actionGet();
		return stats.getPrimaries().getDocs().getCount();
	}

	private int[] getMinAndMaxVersion() {
		int[] versions = new int[2];

		GetAliasesResponse response = client.admin().indices()
				.getAliases(new GetAliasesRequest(queryIndexAliasName))
				.actionGet();
		int maxVersion = Integer.MIN_VALUE;
		int minVersion = Integer.MAX_VALUE;
		ImmutableOpenMap<String, List<AliasMetaData>> aliases = response
				.getAliases();
		Iterator<ObjectCursor<String>> it = aliases.keys().iterator();

		if (!it.hasNext())
			return null;

		// Find the index with the latest version
		while (it.hasNext()) {
			String key = it.next().value;
			String[] split = key.split("_");
			try {
				int version = Integer.parseInt(split[split.length - 1]);
				if (version > maxVersion)
					maxVersion = version;
				if (version < minVersion)
					minVersion = version;
			} catch (NumberFormatException e2) {
				continue;
			}
		}

		versions[0] = minVersion;
		versions[1] = maxVersion;
		return versions;
	}

	private synchronized void verifyIndexSize() {
		if (indexSize >= maxEachAliasIndexSize) {
			// remove previous insert alias to insert on new index
			client.admin().indices().aliases(new IndicesAliasesRequest().removeAlias(indexName,
									insertIndexAliasName)).actionGet();
			indexVersion++;
			indexName = queryIndexAliasName + "_" + indexVersion;

			Settings indexSettings = ImmutableSettings.settingsBuilder()
					.put("number_of_shards", numShards)
					.put("number_of_replicas", replicationFactor).build();

			// create new empty index
			client.admin().indices().prepareCreate(indexName)
					.setSettings(indexSettings).execute().actionGet();
			// point the query alias to new index
			client.admin().indices().aliases(new IndicesAliasesRequest().addAlias(
									queryIndexAliasName, indexName)).actionGet();
			// point the insert alias to new index
			client.admin().indices().aliases(new IndicesAliasesRequest().addAlias(
									insertIndexAliasName, indexName)).actionGet();

			// Delete the oldest index if the total size of the indexes is
			// greater than maxIndexSize to control the index size
			if (totalIndexSize > maxIndexSize) {
				int minVersion = getMinAndMaxVersion()[0];
				String indexToDelete = queryIndexAliasName + "_" + minVersion;
				client.admin().indices()
						.delete(new DeleteIndexRequest(indexToDelete));
				totalIndexSize -= maxEachAliasIndexSize;
			}

			indexSize = 0;
		}
	}

	private void createMapping() {
		String mapping = null;
		try {
			mapping = TwitterInfo.ES_MAPPING;
		} catch (Exception e) {
			return;
		}

		client.admin().indices().preparePutMapping(indexName).setType(typeName)
				.setSource(mapping).execute().actionGet();
	}
}
