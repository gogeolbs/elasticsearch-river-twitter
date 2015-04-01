package org.elasticsearch.river.twitter.handlers;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.codehaus.jackson.map.util.LRUMap;
import org.elasticsearch.river.twitter.connection.TwitterConnectionControl;
import org.elasticsearch.threadpool.ThreadPool;

import twitter4j.Status;
import twitter4j.StatusAdapter;
import twitter4j.StatusDeletionNotice;
import twitter4j.TwitterObjectFactory;

import org.javaswift.joss.client.factory.AccountConfig;
import org.javaswift.joss.client.factory.AccountFactory;
import org.javaswift.joss.client.factory.AuthenticationMethod;
import org.javaswift.joss.model.Account;
import org.javaswift.joss.model.Container;
import org.javaswift.joss.model.StoredObject;

public class FileStatusHandler extends StatusAdapter {

	private LRUMap<String, Object> lruMap = new LRUMap<String, Object>(100000, 100000);
	
	private ThreadPool threadPool;
	private BufferedWriter bw = null;
	private long maxNumTweetsEachFile;
	private String outFilePath = null;
	private long numTweetsInFile = 0;
	private String boundRegion;
	private ThreadPoolExecutor pool;
	private TwitterConnectionControl conn;
	private AtomicLong numTweetsCollected;
	private AtomicLong numTweetsNotCollected;
	private AtomicLong numRepeatedTweets;
	private String url;
	private String username;
	private String password;
	private String containerName;

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public FileStatusHandler(BufferedWriter bw,
			long maxNumTweetsEachFile, String outFilePath,
			long numTweetsInFile, String boundRegion,
			TwitterConnectionControl conn, AtomicLong numTweetsCollected, AtomicLong numTweetsNotCollected,AtomicLong numRepeatedTweets,
			String url, String username, String password, String containerName) {
		this.bw = bw;
		this.maxNumTweetsEachFile = maxNumTweetsEachFile;
		this.outFilePath = outFilePath;
		this.numTweetsInFile = numTweetsInFile;
		this.boundRegion = boundRegion;
		this.conn = conn;
		this.numTweetsCollected = numTweetsCollected;
		this.numTweetsNotCollected = numTweetsNotCollected;
		this.numRepeatedTweets = numRepeatedTweets;
		this.url = url;
		this.username = username;
		this.password = password;
		this.containerName = containerName;
		
		this.threadPool = new ThreadPool("status-twitter");
		this.pool = new ThreadPoolExecutor(2, 2, 1, TimeUnit.DAYS, new LinkedBlockingQueue());
	}
	
	@Override
	public void onStatus(Status status) {
		try {
			String id = Long.toString(status.getId());
			//Verify if the tweet was already inserted
			if(lruMap.containsKey(id)) {
				numRepeatedTweets.incrementAndGet();
				return;
			}
			lruMap.put(id, null);
			
			numTweetsCollected.incrementAndGet();
			String rawJSON = TwitterObjectFactory.getRawJSON(status);
			bw.write(rawJSON +"\n");
			numTweetsInFile++;
			
			if(numTweetsInFile >= maxNumTweetsEachFile){
				bw.flush();
				//compact the file
				pool.execute(new TweetsFileWriter(outFilePath, url, username, password, containerName));
				
				outFilePath = boundRegion +"-tweets-" +System.currentTimeMillis();
				bw.close();
				
				bw = new BufferedWriter(new FileWriter(outFilePath));
				numTweetsInFile = 0;
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
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
	
	private class TweetsFileWriter implements Runnable {
		private String filePath;
		
		private String url;
		private  String username;
		private String password;
		private String containerName;

		public TweetsFileWriter(String filePath, String url, String username,
				String password, String containerName) {
			this.filePath = filePath;
			this.url = url;
			this.username = username;
			this.password = password;
			this.containerName = containerName;
		}

		@Override
		public void run() {
			try {
				Process p = Runtime.getRuntime().exec(
						"tar cfvj " + filePath + ".tar.bz2 " + filePath);
				p.waitFor();

				if(url == null || username == null || password == null || containerName == null)
					return;
				
				//delete the file with tweets
				new File(filePath).delete();
				
				AccountConfig config = new AccountConfig();
				config.setUsername(username);
				config.setPassword(password);
				config.setAuthUrl(url);
				config.setAuthenticationMethod(AuthenticationMethod.BASIC);

				Account account = new AccountFactory(config).createAccount();

				// Listando os dados do container
				Container container = account.getContainer(containerName);

				filePath = filePath + ".tar.bz2";
				StoredObject object = container.getObject(filePath);
				
				File file = new File(filePath);
				object.uploadObject(file);
				
				//delete the compacted file.
				file.delete();
			} catch (IOException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

}
