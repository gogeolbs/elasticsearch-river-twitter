package org.elasticsearch.river.twitter.connection;

import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.river.twitter.handlers.FileStatusHandler;
import org.elasticsearch.river.twitter.handlers.StatusHandler;
import org.elasticsearch.river.twitter.handlers.UserStreamHandler;
import org.elasticsearch.threadpool.ThreadPool;

import twitter4j.FilterQuery;
import twitter4j.StatusAdapter;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.Configuration;
import twitter4j.conf.ConfigurationBuilder;

public class TwitterConnectionControl {

	private volatile boolean closed = false;
	private final String streamType;
	private final String oauthConsumerKey;
	private final String oauthConsumerSecret;
	private final String oauthAccessToken;
	private final String oauthAccessTokenSecret;
	private FilterQuery filterQuery;
	private ThreadPool threadPool;
	
	private volatile BulkProcessor bulkProcessor;
	
	private StatusAdapter statusHandler;
	
	private volatile TwitterStream stream;

	public TwitterConnectionControl(boolean closed, String streamType,
			String oauthConsumerKey,
			String oauthConsumerSecret, String oauthAccessToken,
			String oauthAccessTokenSecret) {
		this.closed = closed;
		this.streamType = streamType;
		this.oauthConsumerKey = oauthConsumerKey;
		this.oauthConsumerSecret = oauthConsumerSecret;
		this.oauthAccessToken = oauthAccessToken;
		this.oauthAccessTokenSecret = oauthAccessTokenSecret;
		
		this.threadPool = new ThreadPool("conn-twitter");
	}
	
	public void setBulkProcessor(BulkProcessor bulkProcessor) {
		this.bulkProcessor = bulkProcessor;
	}

	/**
	 * Build configuration object with credentials and proxy settings
	 * 
	 * @return
	 */
	public Configuration buildTwitterConfiguration() {
		ConfigurationBuilder cb = new ConfigurationBuilder();

		cb.setOAuthConsumerKey(oauthConsumerKey)
				.setOAuthConsumerSecret(oauthConsumerSecret)
				.setOAuthAccessToken(oauthAccessToken)
				.setOAuthAccessTokenSecret(oauthAccessTokenSecret);

		cb.setJSONStoreEnabled(true);
		return cb.build();

	}
	
	/**
	 * Twitter Stream Builder
	 * 
	 * @return
	 */
	private TwitterStream buildTwitterStream(StatusAdapter statusHandler) {
		this.statusHandler = statusHandler;
		TwitterStream stream = new TwitterStreamFactory(
				buildTwitterConfiguration()).getInstance();
		if (streamType.equals("user"))
			stream.addListener(new UserStreamHandler(statusHandler));
		else
			stream.addListener(statusHandler);

		return stream;
	}

	/**
	 * Start twitter stream
	 */
	public boolean startTwitterStream(StatusAdapter statusHandler, FilterQuery filterQuery) {
        stream = buildTwitterStream(statusHandler);
        
        if(stream == null)
        	return false;
        
        this.filterQuery = filterQuery;
        
		if (streamType.equals("filter") || filterQuery != null) {
			stream.filter(filterQuery);
		} else if (streamType.equals("firehose")) {
			stream.firehose(0);
		} else if (streamType.equals("user")) {
			stream.user();
		} else {
			stream.sample();
		}
		
		return true;
	}
	
	public void reconnect() {
		if (closed) {
			return;
		}
		try {
			stream.cleanUp();
		} catch (Exception e) {
		}
		try {
			stream.shutdown();
		} catch (Exception e) {
		}
		if (closed) {
			return;
		}

		try {
			StatusAdapter status = null;
			if(statusHandler instanceof FileStatusHandler){
				FileStatusHandler handler = (FileStatusHandler) statusHandler;
				status = handler.clone();
			}
			
			if(statusHandler instanceof StatusHandler) {
				StatusHandler handler = (StatusHandler) statusHandler;
				status = handler.clone();
			}
			
			startTwitterStream(status, this.filterQuery);
		} catch (Exception e) {
			if (closed) {
				close();
				return;
			}
			// TODO, we can update the status of the river to RECONNECT
			threadPool.schedule(TimeValue.timeValueSeconds(10),
					ThreadPool.Names.GENERIC, new Runnable() {
						@Override
						public void run() {
							reconnect();
						}
					});
		}
	}

	public void close() {
		this.closed = true;
		System.out.println("closing twitter stream river");

		bulkProcessor.close();

		if (stream != null) {
			// No need to call stream.cleanUp():
			// - since it is done by the implementation of shutdown()
			// - it will lead to a thread leak (see TwitterStreamImpl.cleanUp()
			// and TwitterStreamImpl.shutdown() )
			stream.shutdown();
		}
	}
	
}
