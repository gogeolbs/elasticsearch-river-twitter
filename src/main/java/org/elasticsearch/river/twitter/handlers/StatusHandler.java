package org.elasticsearch.river.twitter.handlers;

import java.util.concurrent.atomic.AtomicLong;

import org.elasticsearch.action.WriteConsistencyLevel;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.support.replication.ReplicationType;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.river.twitter.connection.TwitterConnectionControl;
import org.elasticsearch.river.twitter.utils.TwitterInsertBuilder;
import org.elasticsearch.threadpool.ThreadPool;

import twitter4j.Status;
import twitter4j.StatusAdapter;
import twitter4j.StatusDeletionNotice;

public class StatusHandler extends StatusAdapter {

	boolean writeTweetsOnFile;
	boolean collectOnlyGeoTweets;
	boolean autoGenerateGeoPointFromPlace;
	boolean ignoreRetweet;
	boolean geoAsArray;
	private String insertIndexAliasName;
	private String typeName;
	private volatile BulkProcessor bulkProcessor;
	private String indexName;
	private ThreadPool threadPool;
	private TwitterConnectionControl conn;
	private AtomicLong numTweetsCollected;
	private AtomicLong numTweetsNotCollected;

	public StatusHandler(boolean writeTweetsOnFile,
			boolean collectOnlyGeoTweets,
			boolean autoGenerateGeoPointFromPlace, boolean ignoreRetweet,
			boolean geoAsArray, String insertIndexAliasName, String typeName,
			BulkProcessor bulkProcessor, String indexName,
			TwitterConnectionControl conn, AtomicLong numTweetsCollected, AtomicLong numTweetsNotCollected) {
		this.writeTweetsOnFile = writeTweetsOnFile;
		this.collectOnlyGeoTweets = collectOnlyGeoTweets;
		this.autoGenerateGeoPointFromPlace = autoGenerateGeoPointFromPlace;
		this.ignoreRetweet = ignoreRetweet;
		this.geoAsArray = geoAsArray;
		this.insertIndexAliasName = insertIndexAliasName;
		this.typeName = typeName;
		this.bulkProcessor = bulkProcessor;
		this.indexName = indexName;
		this.conn = conn;
		this.numTweetsCollected = numTweetsCollected;
		this.numTweetsNotCollected = numTweetsNotCollected;
		
		this.threadPool = new ThreadPool("status-twitter");
	}
	
	@Override
	public void onStatus(Status status) {
		try {
			/*
			 * Return when it should collect only tweets with geo location
			 * and: 1 - Tweets not contains geo location, or 2 - When the
			 * tweet contains the place, but autoGenerateGeoPointFromPlace
			 * is false
			 */
			if (collectOnlyGeoTweets
					&& status.getGeoLocation() == null
					&& (status.getPlace() == null || (status.getPlace() != null && !autoGenerateGeoPointFromPlace)))
				return;

			numTweetsCollected.incrementAndGet();
			// #24: We want to ignore retweets (default to false)
			// https://github.com/elasticsearch/elasticsearch-river-twitter/issues/24
			if (status.isRetweet() && ignoreRetweet) {
			} else {
				// If we want to index tweets as is, we don't need to
				// convert it to JSon doc
				XContentBuilder builder = TwitterInsertBuilder
						.constructInsertBuilder(status,
								autoGenerateGeoPointFromPlace, geoAsArray);

				if(builder == null)
					return;
				
				bulkProcessor.add(Requests
						.indexRequest(insertIndexAliasName)
						.consistencyLevel(WriteConsistencyLevel.ONE)
						.replicationType(ReplicationType.ASYNC)
						.type(typeName).id(Long.toString(status.getId()))
						.source(builder));
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
		if (statusDeletionNotice.getStatusId() != -1) {
			bulkProcessor.add(Requests.deleteRequest(indexName)
					.type(typeName)
					.id(Long.toString(statusDeletionNotice.getStatusId())));
		}
	}

	@Override
	public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
		numTweetsNotCollected.incrementAndGet();
	}

	@Override
	public void onException(Exception ex) {
		threadPool.generic().execute(new Runnable() {
			@Override
			public void run() {
				conn.reconnect();
			}
		});
	}
}
