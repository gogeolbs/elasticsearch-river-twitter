package org.elasticsearch.river.twitter.utils;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Date;
import java.util.Iterator;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.elasticsearch.action.WriteConsistencyLevel;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.ImmutableSettings.Builder;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.river.twitter.TwitterInsertBuilder;
import org.json.JSONException;
import org.json.JSONObject;

import twitter4j.Status;
import twitter4j.TwitterObjectFactory;

public class DirectInsertES {
	static final String ATTR = "attr";

	public static int WGS84_srid = 4326;

	private Client client;

	@SuppressWarnings("resource")
	public DirectInsertES(String[] seeds, String layerName,
			String elasticCluster) throws Exception {

		TransportAddress[] seedAddresses = new TransportAddress[seeds.length];
		for(int i =0; i < seeds.length;i++)
			seedAddresses[i] = new InetSocketTransportAddress(seeds[i], 9300);

		Builder builder = ImmutableSettings.settingsBuilder().put(
				"cluster.name", elasticCluster);
		client = new TransportClient(builder.build())
				.addTransportAddresses(seedAddresses);
		
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		System.out.println("Inserting using the stream from System.in");
		long t = System.currentTimeMillis();
		long inserted = 0;
		int sizeBulk = 0;
		long numErrors = 0;
		ThreadPoolExecutor pool = new ThreadPoolExecutor(4, 4, 0,
				TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>(8));

		long lineWithErrorsOnFile = 0;
		BulkRequestBuilder bulkRequest = client.prepareBulk();
		String line = br.readLine();
		while (line != null) {
			
			if(line.equals("[") || line.equals("]")) {
				line = br.readLine();
				continue;
			}
			
			//remove the last character if it is comma
			if(line.endsWith(","))
				line = line.substring(0, line.length() - 1);
			
			
			XContentBuilder xBuilder = null;
			String id = null;
			try {
				JSONObject json = new JSONObject(line);
				JSONObject in = new JSONObject(line);
				parse(in, json);
				Status status = TwitterObjectFactory.createStatus(json.toString());
				id = Long.toString(status.getId());
				xBuilder = TwitterInsertBuilder.constructInsertBuilder(status, true, false);
				handleJson(json);
			}catch (Exception e) {
				lineWithErrorsOnFile++;
				line = br.readLine();
				continue;
			}
			
			IndexRequestBuilder insert = client.prepareIndex(layerName, ATTR)
					.setSource(xBuilder).setRefresh(false).setId(id)
					.setConsistencyLevel(WriteConsistencyLevel.ONE);

			bulkRequest.add(insert);
			sizeBulk++;

			line = br.readLine();
			if (sizeBulk > 1500 || line == null || line.equals("]")) {
				while(true) {
					try {
						pool.execute(new ThreadInsert(bulkRequest));
						break;
					}catch(RejectedExecutionException e){
						continue;
					}
				}
				bulkRequest = client.prepareBulk();
				sizeBulk = 0;
			}

			inserted++;
			if (inserted % 1000000 == 0 || line == null) {
				System.out.println("Inserting... " + inserted + "  - Errors: "
						+ numErrors);
			}

		}
		
		pool.shutdown();
		pool.awaitTermination(1, TimeUnit.DAYS);
		this.close();

		System.out.println("Time to insert: "
				+ (System.currentTimeMillis() - t));
		System.out.println("Number of lines with error: " +lineWithErrorsOnFile);
		System.exit(0);
	}
	
	@SuppressWarnings("unchecked")
	private static void parse(JSONObject json, JSONObject out) throws JSONException{
	    Iterator<String> keys = json.keys();
	    while(keys.hasNext()){
	        String key = keys.next();
	        try{
	             JSONObject value = json.getJSONObject(key);
	             parse(value, out.getJSONObject(key));
	        }catch(Exception e){
	        	Object object = json.get(key);
	        	if(object != null && object instanceof String){
	        		String s = (String) object;
	        		if(s.isEmpty() || s.contains(";")){
	        			out.remove(key);
	        		}
	        	}
	        }

	    }
	}

	public static void handleJson(JSONObject json) throws JSONException, java.text.ParseException{
		Object object = json.get("created_at");
		if(object != null && object instanceof String){
			Date date = null;
			try {
			 date = DateUtil.parse((String) object);
			} catch(Exception e) {
			}
			
			json.remove("created_at");
			if(date != null)
				json.put("created_at", date);
		}
		
		JSONObject userJson = json.getJSONObject("user");
		if(userJson != null) {
			object = userJson.get("created_at");
			if(object != null && object instanceof String){
				Date date = null;
				try {
				 date = DateUtil.parse((String) object);
				} catch(Exception e) {
				}
				
				userJson.remove("created_at");
				if(date != null)
					userJson.put("created_at", date);
			}
			
			object = userJson.get("location");
			if(object != null && object instanceof String){
				String location = (String) object;
				if(location.contains(";"))
					userJson.remove("location");
			}
			
			json.remove("user");
			json.put("user", userJson);
		}
	}

	public void close() {
		client.close();
	}

	private static class ThreadInsert implements Runnable {
		BulkRequestBuilder bulkRequest;
		
		public ThreadInsert(BulkRequestBuilder bulkRequest) {
			super();
			this.bulkRequest = bulkRequest;
		}

		@Override
		public void run() {
			BulkResponse bulkResponse = null;
			while (true) {
				try {
					bulkResponse = bulkRequest.execute().actionGet();
					break;
				} catch (Exception e) {
					try {
						Thread.sleep(1000);
					} catch (InterruptedException e1) {
						e1.printStackTrace();
					}
					System.out.println("timed out after [5001ms].\n"
							+ e.getMessage());
				}
			}
			if (bulkResponse.hasFailures()) {
				System.out.println("ERROR ON BULK");
			}
			bulkResponse = null;
		}
	}

	public static void main(String[] args) {
		try {
			if(args.length != 3) {
				System.out.println("You should enter the following arguments: [elasticsearch ip] [elasticsearch index name] [elasticsearch cluster name]");
			}
			//WARN Configurações do welder 
			String seed = args[0];
			String layerName = args[1];
			String elasticCluster = args[2];

			System.out.println("ElasticSearch IP: " + seed);
			System.out.println("Layer name: " + layerName);
			System.out.println("Elastic cluster: " + elasticCluster);

			new DirectInsertES(seed.split(";"), layerName, elasticCluster);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}