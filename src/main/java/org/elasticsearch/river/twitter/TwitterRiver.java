/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.river.twitter;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.WriteConsistencyLevel;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.exists.AliasesExistResponse;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsRequest;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.support.replication.ReplicationType;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.hppc.cursors.ObjectCursor;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.indices.IndexAlreadyExistsException;
import org.elasticsearch.river.AbstractRiverComponent;
import org.elasticsearch.river.River;
import org.elasticsearch.river.RiverName;
import org.elasticsearch.river.RiverSettings;
import org.elasticsearch.threadpool.ThreadPool;

import twitter4j.FilterQuery;
import twitter4j.GeoLocation;
import twitter4j.HashtagEntity;
import twitter4j.PagableResponseList;
import twitter4j.Status;
import twitter4j.StatusAdapter;
import twitter4j.StatusDeletionNotice;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.URLEntity;
import twitter4j.User;
import twitter4j.UserMentionEntity;
import twitter4j.UserStreamAdapter;
import twitter4j.conf.Configuration;
import twitter4j.conf.ConfigurationBuilder;

/**
 *
 */
public class TwitterRiver extends AbstractRiverComponent implements River {

    private final ThreadPool threadPool;

    private final Client client;

    private final String oauthConsumerKey;
    private final String oauthConsumerSecret;
    private final String oauthAccessToken;
    private final String oauthAccessTokenSecret;

    private final String proxyHost;
    private final String proxyPort;
    private final String proxyUser;
    private final String proxyPassword;

    private final boolean raw;
    private final boolean ignoreRetweet;
    private final boolean geoAsArray;
    private final boolean autoGenerateGeoPointFromPlace;
    private final boolean collectOnlyGeoTweets;
    

    private final String queryIndexAliasName;
    private String insertIndexAliasName;
    private String indexName;

    private final String typeName;

    private static final long DEFAULT_MAX_EACH_INDEX_SIZE =  100000000;
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

    private final FilterQuery filterQuery;

    private final String streamType;


    private volatile TwitterStream stream;

    private volatile BulkProcessor bulkProcessor;

    private volatile boolean closed = false;

	private int indexVersion = 1;
	private long indexSize = 0;
	private long totalIndexSize = 0;

    @SuppressWarnings({"unchecked"})
    @Inject
    public TwitterRiver(RiverName riverName, RiverSettings riverSettings, Client client, ThreadPool threadPool, Settings settings) {
        super(riverName, riverSettings);
        this.client = client;
        this.threadPool = threadPool;

        String riverStreamType;
        
        logger.info("Configuring Twitter River...");

        if (riverSettings.settings().containsKey("twitter")) {
            Map<String, Object> twitterSettings = (Map<String, Object>) riverSettings.settings().get("twitter");

            raw = XContentMapValues.nodeBooleanValue(twitterSettings.get("raw"), false);
            autoGenerateGeoPointFromPlace = XContentMapValues.nodeBooleanValue(twitterSettings.get("auto_generate_geo_point_from_place"), true);
            collectOnlyGeoTweets = XContentMapValues.nodeBooleanValue(twitterSettings.get("collect_only_geo_tweets"), false);
            ignoreRetweet = XContentMapValues.nodeBooleanValue(twitterSettings.get("ignore_retweet"), false);
            geoAsArray = XContentMapValues.nodeBooleanValue(twitterSettings.get("geo_as_array"), false);

            if (twitterSettings.containsKey("oauth")) {
                Map<String, Object> oauth = (Map<String, Object>) twitterSettings.get("oauth");
                if (oauth.containsKey("consumer_key")) {
                    oauthConsumerKey = XContentMapValues.nodeStringValue(oauth.get("consumer_key"), null);
                } else {
                    oauthConsumerKey = settings.get("river.twitter.oauth.consumer_key");
                }
                if (oauth.containsKey("consumer_secret")) {
                    oauthConsumerSecret = XContentMapValues.nodeStringValue(oauth.get("consumer_secret"), null);
                } else {
                    oauthConsumerSecret = settings.get("river.twitter.oauth.consumer_secret");
                }
                if (oauth.containsKey("access_token")) {
                    oauthAccessToken = XContentMapValues.nodeStringValue(oauth.get("access_token"), null);
                } else {
                    oauthAccessToken = settings.get("river.twitter.oauth.access_token");
                }
                if (oauth.containsKey("access_token_secret")) {
                    oauthAccessTokenSecret = XContentMapValues.nodeStringValue(oauth.get("access_token_secret"), null);
                } else {
                    oauthAccessTokenSecret = settings.get("river.twitter.oauth.access_token_secret");
                }
            } else {
                oauthConsumerKey = settings.get("river.twitter.oauth.consumer_key");
                oauthConsumerSecret = settings.get("river.twitter.oauth.consumer_secret");
                oauthAccessToken = settings.get("river.twitter.oauth.access_token");
                oauthAccessTokenSecret = settings.get("river.twitter.oauth.access_token_secret");
            }

            if (twitterSettings.containsKey("proxy")) {
                Map<String, Object> proxy = (Map<String, Object>) twitterSettings.get("proxy");
                proxyHost = XContentMapValues.nodeStringValue(proxy.get("host"), null);
                proxyPort = XContentMapValues.nodeStringValue(proxy.get("port"), null);
                proxyUser = XContentMapValues.nodeStringValue(proxy.get("user"), null);
                proxyPassword = XContentMapValues.nodeStringValue(proxy.get("password"), null);
            } else {
                // Let's see if we have that in node settings
                proxyHost = settings.get("river.twitter.proxy.host");
                proxyPort = settings.get("river.twitter.proxy.port");
                proxyUser = settings.get("river.twitter.proxy.user");
                proxyPassword = settings.get("river.twitter.proxy.password");
            }

            riverStreamType = XContentMapValues.nodeStringValue(twitterSettings.get("type"), "sample");
            Map<String, Object> filterSettings = (Map<String, Object>) twitterSettings.get("filter");

            if (riverStreamType.equals("filter") && filterSettings == null) {
                filterQuery = null;
                stream = null;
                streamType = null;
                indexName = null;
                queryIndexAliasName = indexName;
                maxEachAliasIndexSize = DEFAULT_MAX_EACH_INDEX_SIZE;
                maxIndexSize = DEFAULT_MAX_INDEX_SIZE;
                numShards = DEFAULT_NUM_SHARDS;
                replicationFactor = DEFAULT_REPLICATION_FACTOR;
                typeName = "status";
                bulkSize = DEFAULT_BULK_SIZE;
                this.maxConcurrentBulk = 1;
                this.bulkFlushInterval = TimeValue.timeValueSeconds(DEFAULT_REFRESH_TIME);
                logger.warn("no filter defined for type filter. Disabling river...");
                return;
            }

            if (filterSettings != null) {
                riverStreamType = "filter";
                filterQuery = new FilterQuery();
                filterQuery.count(XContentMapValues.nodeIntegerValue(filterSettings.get("count"), 0));
                Object tracks = filterSettings.get("tracks");
                boolean filterSet = false;
                if (tracks != null) {
                    if (tracks instanceof List) {
                        List<String> lTracks = (List<String>) tracks;
                        filterQuery.track(lTracks.toArray(new String[lTracks.size()]));
                    } else {
                        filterQuery.track(Strings.commaDelimitedListToStringArray(tracks.toString()));
                    }
                    filterSet = true;
                }
                Object follow = filterSettings.get("follow");
                if (follow != null) {
                    if (follow instanceof List) {
                        List<?> lFollow = (List<?>) follow;
                        long[] followIds = new long[lFollow.size()];
                        for (int i = 0; i < lFollow.size(); i++) {
                            Object o = lFollow.get(i);
                            if (o instanceof Number) {
                                followIds[i] = ((Number) o).intValue();
                            } else {
                                followIds[i] = Long.parseLong(o.toString());
                            }
                        }
                        filterQuery.follow(followIds);
                    } else {
                        String[] ids = Strings.commaDelimitedListToStringArray(follow.toString());
                        long[] followIds = new long[ids.length];
                        for (int i = 0; i < ids.length; i++) {
                            followIds[i] = Long.parseLong(ids[i]);
                        }
                        filterQuery.follow(followIds);
                    }
                    filterSet = true;
                }
                Object locations = filterSettings.get("locations");
                if (locations != null) {
                    if (locations instanceof List) {
                        List<?> lLocations = (List<?>) locations;
                        double[][] dLocations = new double[lLocations.size()][];
                        for (int i = 0; i < lLocations.size(); i++) {
                            Object loc = lLocations.get(i);
                            double lat;
                            double lon;
                            if (loc instanceof List) {
                                List<?> lLoc = (List<?>) loc;
                                if (lLoc.get(0) instanceof Number) {
                                    lon = ((Number) lLoc.get(0)).doubleValue();
                                } else {
                                    lon = Double.parseDouble(lLoc.get(0).toString());
                                }
                                if (lLoc.get(1) instanceof Number) {
                                    lat = ((Number) lLoc.get(1)).doubleValue();
                                } else {
                                    lat = Double.parseDouble(lLoc.get(1).toString());
                                }
                            } else {
                                String[] sLoc = Strings.commaDelimitedListToStringArray(loc.toString());
                                lon = Double.parseDouble(sLoc[0]);
                                lat = Double.parseDouble(sLoc[1]);
                            }
                            dLocations[i] = new double[]{lon, lat};
                        }
                        filterQuery.locations(dLocations);
                    } else {
                        String[] sLocations = Strings.commaDelimitedListToStringArray(locations.toString());
                        double[][] dLocations = new double[sLocations.length / 2][];
                        int dCounter = 0;
                        for (int i = 0; i < sLocations.length; i++) {
                            double lon = Double.parseDouble(sLocations[i]);
                            double lat = Double.parseDouble(sLocations[++i]);
                            dLocations[dCounter++] = new double[]{lon, lat};
                        }
                        filterQuery.locations(dLocations);
                    }
                    filterSet = true;
                }
                Object userLists = filterSettings.get("user_lists");
                if (userLists != null) {
                    if (userLists instanceof List) {
                        List<String> lUserlists = (List<String>) userLists;
                        String[] tUserlists = lUserlists.toArray(new String[lUserlists.size()]);
                        filterQuery.follow(getUsersListMembers(tUserlists));
                    } else {
                        String[] tUserlists = Strings.commaDelimitedListToStringArray(userLists.toString());
                        filterQuery.follow(getUsersListMembers(tUserlists));
                    }
                    filterSet = true;
                }

                // We should have something to filter
                if (!filterSet) {
                    streamType = null;
                    indexName = null;
                    queryIndexAliasName = indexName;
                    maxEachAliasIndexSize = DEFAULT_MAX_EACH_INDEX_SIZE;
                    maxIndexSize = DEFAULT_MAX_INDEX_SIZE;
                    typeName = "status";
                    bulkSize = DEFAULT_BULK_SIZE;
                    numShards = DEFAULT_NUM_SHARDS;
                    replicationFactor = DEFAULT_REPLICATION_FACTOR;
                    this.maxConcurrentBulk = 1;
                    this.bulkFlushInterval = TimeValue.timeValueSeconds(DEFAULT_REFRESH_TIME);
                    logger.warn("can not set language filter without tracks, follow, locations or user_lists. Disabling river.");
                    return;
                }

                Object language = filterSettings.get("language");
                if (language != null) {
                    if (language instanceof List) {
                        List<String> lLanguage = (List<String>) language;
                        filterQuery.language(lLanguage.toArray(new String[lLanguage.size()]));
                    } else {
                        filterQuery.language(Strings.commaDelimitedListToStringArray(language.toString()));
                    }
                }
            } else {
                filterQuery = null;
            }
        } else {
            // No specific settings. We need to use some defaults
            riverStreamType = "sample";
            raw = false;
            autoGenerateGeoPointFromPlace = true;
            collectOnlyGeoTweets = false;
            ignoreRetweet = false;
            geoAsArray = false;
            oauthConsumerKey = settings.get("river.twitter.oauth.consumer_key");
            oauthConsumerSecret = settings.get("river.twitter.oauth.consumer_secret");
            oauthAccessToken = settings.get("river.twitter.oauth.access_token");
            oauthAccessTokenSecret = settings.get("river.twitter.oauth.access_token_secret");
            filterQuery = null;
            proxyHost = null;
            proxyPort = null;
            proxyUser = null;
            proxyPassword =null;
        }

        if (oauthAccessToken == null || oauthConsumerKey == null || oauthConsumerSecret == null || oauthAccessTokenSecret == null) {
            stream = null;
            streamType = null;
            indexName = null;
            queryIndexAliasName = indexName;
            maxEachAliasIndexSize = DEFAULT_MAX_EACH_INDEX_SIZE;
            maxIndexSize = DEFAULT_MAX_INDEX_SIZE;
            typeName = "status";
            bulkSize = DEFAULT_BULK_SIZE;
            numShards = DEFAULT_NUM_SHARDS;
            replicationFactor = DEFAULT_REPLICATION_FACTOR;
            this.maxConcurrentBulk = 1;
            this.bulkFlushInterval = TimeValue.timeValueSeconds(DEFAULT_REFRESH_TIME);
            logger.warn("no oauth specified, disabling river...");
            return;
        }

        if (riverSettings.settings().containsKey("index")) {
            Map<String, Object> indexSettings = (Map<String, Object>) riverSettings.settings().get("index");
            indexName = XContentMapValues.nodeStringValue(indexSettings.get("index"), riverName.name());
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
            indexName = riverName.name();
            maxEachAliasIndexSize = DEFAULT_MAX_EACH_INDEX_SIZE;
            maxIndexSize = DEFAULT_MAX_INDEX_SIZE;
            typeName = "status";
            bulkSize = DEFAULT_BULK_SIZE;
            numShards = DEFAULT_NUM_SHARDS;
            replicationFactor = DEFAULT_REPLICATION_FACTOR;
            this.maxConcurrentBulk = 1;
            this.bulkFlushInterval = TimeValue.timeValueSeconds(DEFAULT_REFRESH_TIME);
        }

        logger.info("creating twitter stream river");
        if (raw && logger.isDebugEnabled()) {
            logger.debug("will index twitter raw content...");
        }

        insertIndexAliasName = indexName +"_index";
        queryIndexAliasName = indexName;
        streamType = riverStreamType;
        stream = buildTwitterStream();
    }
    /**
     * Get users id of each list to stream them.
     * @param tUserlists List of user list. Should be a public list.
     * @return
     */
    private long[] getUsersListMembers(String[] tUserlists) {
        logger.debug("Fetching user id of given lists");
        List<Long> listUserIdToFollow = new ArrayList<Long>();
        Configuration cb = buildTwitterConfiguration();
        Twitter twitterImpl = new TwitterFactory(cb).getInstance();

        //For each list given in parameter
        for (String listId : tUserlists) {
            logger.debug("Adding users of list {} ",listId);
            String[] splitListId = listId.split("/");
            try {
                long cursor = -1;
                PagableResponseList<User> itUserListMembers;
                do {
                    itUserListMembers = twitterImpl.getUserListMembers(splitListId[0], splitListId[1], cursor);
                    for (User member : itUserListMembers) {
                        long userId = member.getId();
                        listUserIdToFollow.add(userId);
                    }
                } while ((cursor = itUserListMembers.getNextCursor()) != 0);

            } catch (TwitterException te) {
                logger.error("Failed to get list members for : {}", listId, te);
            }
        }


        //Just casting from Long to long
        long ret[] = new long[listUserIdToFollow.size()];
        int pos = 0;
        for (Long userId : listUserIdToFollow) {
            ret[pos] = userId;
            pos++;
        }
        return ret;
    }

    /**
     * Build configuration object with credentials and proxy settings
     * @return
     */
    private Configuration buildTwitterConfiguration() {
        logger.debug("creating TwitterConfigurationBuilder");
        ConfigurationBuilder cb = new ConfigurationBuilder();

        cb.setOAuthConsumerKey(oauthConsumerKey)
                .setOAuthConsumerSecret(oauthConsumerSecret)
                .setOAuthAccessToken(oauthAccessToken)
                .setOAuthAccessTokenSecret(oauthAccessTokenSecret);

        if (proxyHost != null) cb.setHttpProxyHost(proxyHost);
        if (proxyPort != null) cb.setHttpProxyPort(Integer.parseInt(proxyPort));
        if (proxyUser != null) cb.setHttpProxyUser(proxyUser);
        if (proxyPassword != null) cb.setHttpProxyPassword(proxyPassword);
        if (raw) cb.setJSONStoreEnabled(true);
        return cb.build();

    }

    /**
     * Twitter Stream Builder
     * @return
     */
    private TwitterStream buildTwitterStream() {
        logger.debug("creating TwitterStreamFactory");


        TwitterStream stream = new TwitterStreamFactory(buildTwitterConfiguration()).getInstance();
        if (streamType.equals("user")) 
        	stream.addListener(new UserStreamHandler());
        else 
        	stream.addListener(new StatusHandler());

        return stream;
    }

    /**
     * Start twitter stream
     */
    private void startTwitterStream() {
        logger.info("starting {} twitter stream", streamType);
        if (streamType.equals("filter") || filterQuery != null) {
            stream.filter(filterQuery);
        } else if (streamType.equals("firehose")) {
            stream.firehose(0);
        } else if (streamType.equals("user")) {
        	stream.user();
        } else {
            stream.sample();
        }
    }

    @Override
    public void start() {
    	logger.debug("Starting Twitter River...");
        if (stream == null) {
            return;
        }

        try {
        	//wait for cluster health status yellow until 2 minutes
        	if (client.admin().cluster()
					.health(new ClusterHealthRequest(queryIndexAliasName)).get()
					.getStatus().equals(ClusterHealthStatus.RED)) {
        		logger.info("Waiting for cluster health status yellow on index [{}] ...", queryIndexAliasName);
				client.admin().cluster().prepareHealth(queryIndexAliasName)
						.setWaitForYellowStatus().setTimeout(TimeValue.timeValueMinutes(2)).get();
			}
        	
        	int[] versions = getMinAndMaxVersion();
        	if(versions == null) {
	        	indexName = indexName +"_" +indexVersion ;
	            logger.debug("Trying to create index [{}]", indexName);
	            Settings indexSettings = ImmutableSettings.settingsBuilder()
	    				.put("number_of_shards", numShards)
	    				.put("number_of_replicas", replicationFactor).build();
	            client.admin().indices().prepareCreate(indexName).setSettings(indexSettings).execute().actionGet();
	            client.admin().indices().aliases(new IndicesAliasesRequest().addAlias(insertIndexAliasName, indexName)).actionGet();
	            client.admin().indices().aliases(new IndicesAliasesRequest().addAlias(queryIndexAliasName, indexName)).actionGet();
	            logger.info("Create new Index [{}] with version: [{}]", indexName, indexVersion);
        	} else {
        		indexVersion = getMinAndMaxVersion()[1]; //get latest index version
            	logger.info("Recovering Index [{}] with version: [{}]", indexName, indexVersion);
            	
            	indexName = queryIndexAliasName +"_" +indexVersion;
            	indexSize = getIndexSize(indexName);
            	totalIndexSize = getIndexSize(queryIndexAliasName);
            	verifyAlias();
        	}
        } catch (Exception e) {
            if (ExceptionsHelper.unwrapCause(e) instanceof IndexAlreadyExistsException) {
            	indexVersion = getMinAndMaxVersion()[1]; //get latest index version
            	logger.info("Index [{}] already exists. Get Index [{}] with version: [{}]", indexName, indexName, indexVersion);
            	
            	indexName = queryIndexAliasName +"_" +indexVersion;
            	indexSize = getIndexSize(indexName);
            	totalIndexSize = getIndexSize(queryIndexAliasName);
            	verifyAlias();
            	 
                // that's fine
                logger.debug("Index [{}] already exists, skipping...", indexName);
            } else if (ExceptionsHelper.unwrapCause(e) instanceof ClusterBlockException) {
                // ok, not recovered yet..., lets start indexing and hope we recover by the first bulk
                // TODO: a smarter logic can be to register for cluster event listener here, and only start sampling when the block is removed...
                logger.debug("Cluster is blocked for now. Index [{}] can not be created, skipping...", indexName);
            } else {
                logger.warn("failed to create index [{}], disabling river...", e, indexName);
                return;
            }
        }

        logger.info("Twitter data will be inserted on Index [{}] with size of [{}] and total size of [{}]", indexName, indexSize, totalIndexSize);
        if (client.admin().indices().prepareGetMappings(indexName).setTypes(typeName).get().getMappings().isEmpty()) {
        	createMapping();
        } else {
            logger.debug("Mapping already exists for [{}]/[{}], skipping...", indexName, typeName);
        }

        // Creating bulk processor
        this.bulkProcessor = BulkProcessor.builder(client, new BulkProcessor.Listener() {
            @Override
            public void beforeBulk(long executionId, BulkRequest request) {
                logger.debug("Going to execute new bulk composed of {} actions", request.numberOfActions());
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
            	indexSize += request.numberOfActions();
            	totalIndexSize += request.numberOfActions();
            	verifyIndexSize(); //verify if the size of the index is greater than the allowed
        		
                logger.debug("Executed bulk composed of {} actions", request.numberOfActions() +". Index size: " +indexSize);
                if (response.hasFailures()) {
                    logger.warn("There was failures while executing bulk", response.buildFailureMessage());
                    if (logger.isDebugEnabled()) {
                        for (BulkItemResponse item : response.getItems()) {
                            if (item.isFailed()) {
                                logger.debug("Error for {}/{}/{} for {} operation: {}", item.getIndex(),
                                        item.getType(), item.getId(), item.getOpType(), item.getFailureMessage());
                            }
                        }
                    }
                }
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
                logger.warn("Error executing bulk", failure);
            }
        })
                .setBulkActions(bulkSize)
                .setConcurrentRequests(maxConcurrentBulk)
                .setFlushInterval(bulkFlushInterval)
                .build();

        logger.debug("Bulk processor created with bulkSize [{}], bulkFlushInterval [{}]", bulkSize, bulkFlushInterval);
        startTwitterStream();
    }
    
    private void verifyAlias(){
    	try {
			AliasesExistResponse response = client.admin().indices().aliasesExist(new GetAliasesRequest(insertIndexAliasName).indices(indexName)).get();
			if(!response.exists())
				client.admin().indices().aliases(new IndicesAliasesRequest().addAlias(insertIndexAliasName, indexName)).actionGet();
		} catch (InterruptedException e) {
			logger.error(e.getMessage(), e);
		} catch (ExecutionException e) {
			logger.error(e.getMessage(), e);
		}
    }
    private long getIndexSize(String indexName){
    	IndicesStatsResponse stats = client
				.admin()
				.indices()
				.stats(new IndicesStatsRequest().clear().docs(true)
						.indices(indexName)).actionGet();
    	return stats.getPrimaries().getDocs().getCount();
    }
    
    private int[] getMinAndMaxVersion(){
    	int[] versions = new int[2];

    	GetAliasesResponse response = client.admin().indices().getAliases(new GetAliasesRequest(queryIndexAliasName)).actionGet();
    	int maxVersion = Integer.MIN_VALUE;
    	int minVersion = Integer.MAX_VALUE;
    	ImmutableOpenMap<String,List<AliasMetaData>> aliases = response.getAliases();
    	Iterator<ObjectCursor<String>> it = aliases.keys().iterator();
    	
    	if(!it.hasNext())
    		return null;
    	
    	//Find the index with the latest version
    	while(it.hasNext()) {
    		String key = it.next().value;
    		String[] split = key.split("_");
    		try {
    			int version = Integer.parseInt(split[split.length - 1]);
    			if(version > maxVersion)
    				maxVersion = version;
    			if(version < minVersion)
    				minVersion = version;
			} catch (NumberFormatException e2) {
				continue;
			}
    	}
    	
    	versions[0] = minVersion;
    	versions[1] = maxVersion;
    	return versions;
    }
    
    private synchronized void verifyIndexSize(){
		if(indexSize >= maxEachAliasIndexSize){
			logger.info("A new index will be created: " +indexName);
			//remove previous insert alias to insert on new index
			client.admin().indices().aliases(new IndicesAliasesRequest().removeAlias(indexName, insertIndexAliasName)).actionGet();
			indexVersion++;
			indexName = queryIndexAliasName +"_" +indexVersion;
			
			Settings indexSettings = ImmutableSettings.settingsBuilder()
					.put("number_of_shards", numShards)
					.put("number_of_replicas", replicationFactor).build();
			
			//create new empty index
			client.admin().indices().prepareCreate(indexName).setSettings(indexSettings).execute().actionGet();
			//point the query alias to new index
			client.admin().indices().aliases(new IndicesAliasesRequest().addAlias(queryIndexAliasName, indexName)).actionGet();
			//point the insert alias to new index
            client.admin().indices().aliases(new IndicesAliasesRequest().addAlias(insertIndexAliasName, indexName)).actionGet();
            
            logger.info("Created a new index " +indexName);
            
            //Delete the oldest index if the total size of the indexes is greater than maxIndexSize to control the index size
    		if(totalIndexSize > maxIndexSize){
				int minVersion = getMinAndMaxVersion()[0];
            	String indexToDelete = queryIndexAliasName +"_" +minVersion;
            	client.admin().indices().delete(new DeleteIndexRequest(indexToDelete));
            	logger.info("Index " +indexToDelete +" was deleted because the index size limit was exceeded.");
            	totalIndexSize -= maxEachAliasIndexSize;
			}
    		
    		indexSize = 0;
		}
    }

    private void createMapping(){
    	String mapping = null;
        //If raw is false, we create a customized mapping
        if (!raw) {
            try {
                mapping = XContentFactory.jsonBuilder().startObject().startObject(typeName).startObject("properties")
                        .startObject("location").field("type", "geo_point").endObject()
                        .startObject("language").field("type", "string").field("index", "not_analyzed").endObject()
                        .startObject("user").startObject("properties").startObject("screen_name").field("type", "string").field("index", "not_analyzed").endObject().endObject().endObject()
                        .startObject("mention").startObject("properties").startObject("screen_name").field("type", "string").field("index", "not_analyzed").endObject().endObject().endObject()
                        .startObject("in_reply").startObject("properties").startObject("user_screen_name").field("type", "string").field("index", "not_analyzed").endObject().endObject().endObject()
                        .startObject("retweet").startObject("properties").startObject("user_screen_name").field("type", "string").field("index", "not_analyzed").endObject().endObject().endObject()
                        .endObject().endObject().endObject().string();
            } catch (Exception e) {
                logger.warn("failed to apply default mapping [{}]/[{}], disabling river...", e, indexName, typeName);
                return;
            }
        } else {
        	//When raw is true, we create a mapping only with location like geo_point
            try {
            	mapping = TwitterInfo.ES_MAPPING;
            } catch(Exception e) {
            	logger.warn("failed to create geo mapping [{}]/[{}], disabling river...", e, indexName, typeName);
                return;
            }
        }
        
        logger.debug("Applying default mapping for [{}]/[{}]: {}", indexName, typeName, mapping);
        client.admin().indices().preparePutMapping(indexName).setType(typeName).setSource(mapping).execute().actionGet();
    }
    private void reconnect() {
        if (closed) {
            logger.debug("can not reconnect twitter on a closed river");
            return;
        }
        try {
            stream.cleanUp();
        } catch (Exception e) {
            logger.debug("failed to cleanup after failure", e);
        }
        try {
            stream.shutdown();
        } catch (Exception e) {
            logger.debug("failed to shutdown after failure", e);
        }
        if (closed) {
            return;
        }

        try {
            stream = buildTwitterStream();
            startTwitterStream();
        } catch (Exception e) {
            if (closed) {
                close();
                return;
            }
            // TODO, we can update the status of the river to RECONNECT
            logger.warn("failed to connect after failure, throttling", e);
            threadPool.schedule(TimeValue.timeValueSeconds(10), ThreadPool.Names.GENERIC, new Runnable() {
                @Override
                public void run() {
                    reconnect();
                }
            });
        }
    }

    @Override
    public void close() {
        this.closed = true;
        logger.info("closing twitter stream river");

        bulkProcessor.close();

        if (stream != null) {
            // No need to call stream.cleanUp():
            // - since it is done by the implementation of shutdown()
            // - it will lead to a thread leak (see TwitterStreamImpl.cleanUp() and TwitterStreamImpl.shutdown() )
            stream.shutdown();
        }
    }

    private class StatusHandler extends StatusAdapter {

        @Override
        public void onStatus(Status status) {
            try {
            	/*
            	 * Return when it should collect only tweets with geo location and:
            	 * 1 - Tweets not contains geo location, or
            	 * 2 - When the tweet contains the place, but autoGenerateGeoPointFromPlace is false
            	 */
            	if(collectOnlyGeoTweets && status.getGeoLocation() == null && (status.getPlace() == null || (status.getPlace() != null && !autoGenerateGeoPointFromPlace) ))
            		return;
            	
                // #24: We want to ignore retweets (default to false) https://github.com/elasticsearch/elasticsearch-river-twitter/issues/24
                if (status.isRetweet() && ignoreRetweet) {
                    if (logger.isTraceEnabled()) {
                        logger.trace("ignoring status cause retweet {} : {}", status.getUser().getName(), status.getText());
                    }
                } else {
                    if (logger.isTraceEnabled()) {
                        logger.trace("status {} : {}", status.getUser().getName(), status.getText());
                    }

                    // If we want to index tweets as is, we don't need to convert it to JSon doc
                    if (raw) {
                    	XContentBuilder builder = TwitterInsertBuilder.constructInsertBuilder(status, autoGenerateGeoPointFromPlace, geoAsArray);
                    	
						bulkProcessor.add(Requests.indexRequest(insertIndexAliasName).consistencyLevel(WriteConsistencyLevel.ONE).replicationType(ReplicationType.ASYNC).type(typeName).id(Long.toString(status.getId())).source(builder));
                        
                    } else {
                        XContentBuilder builder = XContentFactory.jsonBuilder().startObject();
                        builder.field("text", status.getText());
                        builder.field("created_at", status.getCreatedAt());
                        builder.field("source", status.getSource());
                        builder.field("truncated", status.isTruncated());
                        builder.field("language", status.getLang());

                        if (status.getUserMentionEntities() != null) {
                            builder.startArray("mention");
                            for (UserMentionEntity user : status.getUserMentionEntities()) {
                                builder.startObject();
                                builder.field("id", user.getId());
                                builder.field("name", user.getName());
                                builder.field("screen_name", user.getScreenName());
                                builder.field("start", user.getStart());
                                builder.field("end", user.getEnd());
                                builder.endObject();
                            }
                            builder.endArray();
                        }

                        if (status.getRetweetCount() != -1) {
                            builder.field("retweet_count", status.getRetweetCount());
                        }

                        if (status.isRetweet() && status.getRetweetedStatus() != null) {
                            builder.startObject("retweet");
                            builder.field("id", status.getRetweetedStatus().getId());
                            if (status.getRetweetedStatus().getUser() != null) {
                                builder.field("user_id", status.getRetweetedStatus().getUser().getId());
                                builder.field("user_screen_name", status.getRetweetedStatus().getUser().getScreenName());
                                if (status.getRetweetedStatus().getRetweetCount() != -1) {
                                    builder.field("retweet_count", status.getRetweetedStatus().getRetweetCount());
                                }
                            }
                            builder.endObject();
                        }

                        if (status.getInReplyToStatusId() != -1) {
                            builder.startObject("in_reply");
                            builder.field("status", status.getInReplyToStatusId());
                            if (status.getInReplyToUserId() != -1) {
                                builder.field("user_id", status.getInReplyToUserId());
                                builder.field("user_screen_name", status.getInReplyToScreenName());
                            }
                            builder.endObject();
                        }

                        if (status.getHashtagEntities() != null) {
                            builder.startArray("hashtag");
                            for (HashtagEntity hashtag : status.getHashtagEntities()) {
                                builder.startObject();
                                builder.field("text", hashtag.getText());
                                builder.field("start", hashtag.getStart());
                                builder.field("end", hashtag.getEnd());
                                builder.endObject();
                            }
                            builder.endArray();
                        }
                        if (status.getContributors() != null && status.getContributors().length > 0) {
                            builder.array("contributor", status.getContributors());
                        }
                        if (status.getGeoLocation() != null) {
                            if (geoAsArray) {
                                builder.startArray("location");
                                builder.value(status.getGeoLocation().getLongitude());
                                builder.value(status.getGeoLocation().getLatitude());
                                builder.endArray();
                            } else {
                                builder.startObject("location");
                                builder.field("lat", status.getGeoLocation().getLatitude());
                                builder.field("lon", status.getGeoLocation().getLongitude());
                                builder.endObject();
                            }
                        }
                        if (status.getPlace() != null) {
                        	if(status.getGeoLocation() == null && autoGenerateGeoPointFromPlace){
                        		builder.field("location", generateGeoPointFromPlace(status));
                        	}
                        	
                            builder.startObject("place");
                            builder.field("id", status.getPlace().getId());
                            builder.field("name", status.getPlace().getName());
                            builder.field("type", status.getPlace().getPlaceType());
                            builder.field("full_name", status.getPlace().getFullName());
                            builder.field("street_address", status.getPlace().getStreetAddress());
                            builder.field("country", status.getPlace().getCountry());
                            builder.field("country_code", status.getPlace().getCountryCode());
                            builder.field("url", status.getPlace().getURL());
                            builder.endObject();
                        }
                        if (status.getURLEntities() != null) {
                            builder.startArray("link");
                            for (URLEntity url : status.getURLEntities()) {
                                if (url != null) {
                                    builder.startObject();
                                    if (url.getURL() != null) {
                                        builder.field("url", url.getURL());
                                    }
                                    if (url.getDisplayURL() != null) {
                                        builder.field("display_url", url.getDisplayURL());
                                    }
                                    if (url.getExpandedURL() != null) {
                                        builder.field("expand_url", url.getExpandedURL());
                                    }
                                    builder.field("start", url.getStart());
                                    builder.field("end", url.getEnd());
                                    builder.endObject();
                                }
                            }
                            builder.endArray();
                        }

                        builder.startObject("user");
                        builder.field("id", status.getUser().getId());
                        builder.field("name", status.getUser().getName());
                        builder.field("screen_name", status.getUser().getScreenName());
                        builder.field("location", status.getUser().getLocation());
                        builder.field("description", status.getUser().getDescription());
                        builder.field("profile_image_url", status.getUser().getProfileImageURL());
                        builder.field("profile_image_url_https", status.getUser().getProfileImageURLHttps());

                        builder.endObject();

                        builder.endObject();
                        bulkProcessor.add(Requests.indexRequest(indexName).type(typeName).id(Long.toString(status.getId())).source(builder));
                    }
                }

            } catch (Exception e) {
                logger.warn("failed to construct index request", e);
            }
        }

        @Override
        public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
            if (statusDeletionNotice.getStatusId() != -1) {
                bulkProcessor.add(Requests.deleteRequest(indexName).type(typeName).id(Long.toString(statusDeletionNotice.getStatusId())));
            }
        }

        @Override
        public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
            logger.debug("received track limitation notice, number_of_limited_statuses {}", numberOfLimitedStatuses);
        }

        @Override
        public void onException(Exception ex) {
            logger.warn("stream failure, restarting stream...", ex);
            threadPool.generic().execute(new Runnable() {
                @Override
                public void run() {
                    reconnect();
                }
            });
        }
        
        private String generateGeoPointFromPlace(Status status){
        	GeoLocation[][] boundingBoxCoordinates = status.getPlace().getBoundingBoxCoordinates();
    		if(boundingBoxCoordinates != null && boundingBoxCoordinates.length > 0){
    			GeoLocation[] geoLocations = boundingBoxCoordinates[0];
    			double minx = geoLocations[0].getLongitude();
    			double miny = geoLocations[0].getLatitude();
    			double maxy = geoLocations[1].getLatitude();
    			double maxx = geoLocations[2].getLongitude();
    			
    			double x = minx + Math.random() * (maxx - minx);
    			double y = miny + Math.random() * (maxy - miny);
    			return y +"," + x;
    		}
    		return null;
        }
    }
    
    
    private class UserStreamHandler extends UserStreamAdapter {

    	private final StatusHandler statusHandler = new StatusHandler(); 
    	
		@Override
		public void onException(Exception ex) {
			statusHandler.onException(ex);
		}

		@Override
		public void onStatus(Status status) {
			statusHandler.onStatus(status);
		}

		@Override
		public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
			statusHandler.onDeletionNotice(statusDeletionNotice);
		}
    }
}
