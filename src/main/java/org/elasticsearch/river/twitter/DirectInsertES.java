package org.elasticsearch.river.twitter;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Properties;
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
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.river.twitter.utils.LowerCaseKeyDeserializer;
import org.elasticsearch.river.twitter.utils.TwitterInsertBuilder;
import org.javaswift.joss.client.factory.AccountConfig;
import org.javaswift.joss.client.factory.AccountFactory;
import org.javaswift.joss.client.factory.AuthenticationMethod;
import org.javaswift.joss.model.Account;
import org.javaswift.joss.model.Container;
import org.javaswift.joss.model.StoredObject;

import twitter4j.Status;
import twitter4j.TwitterObjectFactory;

public class DirectInsertES {
	static final String ATTR = "attr";

	public static int WGS84_srid = 4326;

	private Client client;
	
	@SuppressWarnings("resource")
	public DirectInsertES(String seed, String elasticCluster) throws Exception {
		Builder builder = ImmutableSettings.settingsBuilder().put(
				"cluster.name", elasticCluster);
		client = new TransportClient(builder.build())
				.addTransportAddresses(new InetSocketTransportAddress(seed, 9300));
	}
	
	public static void main(String[] args) {
		try {
			if(args.length != 1) {
				System.out.println("You should enter properties file");
				System.exit(0);
			}
			
			String propertiesFile = args[0]; 
			Properties properties = new Properties();
			properties.load(new FileReader(propertiesFile));
			
			String seed =  properties.getProperty("seed", null);
			String elasticCluster = properties.getProperty("elastic_cluster", null);
			
			if(seed == null || elasticCluster == null){
				System.err.println("Set the ElasticSearch seed and cluster name on properties file " +propertiesFile);
				System.exit(0);
			}
			
			System.out.println("ElasticSearch IP: " + seed +". Elastic cluster: " + elasticCluster);
			
			DirectInsertES insertES = new DirectInsertES(seed, elasticCluster);
			
			String layerName = properties.getProperty("layer_name", null);
			
			if(layerName == null){
				System.err.println("Set the layer name name on properties file " +propertiesFile);
				System.exit(0);
			}
			
			boolean twitter4jFormat = Boolean.parseBoolean(properties.getProperty("files_twitter4j_format", "false"));
			
			//Remote storage properties
			String url = properties.getProperty("url", null);
			String username = properties.getProperty("username", null);
			String password = properties.getProperty("password", null);
			String containerName = properties.getProperty("container_name", null);
			String from = properties.getProperty("from_date", null);
			String until = properties.getProperty("until_date", null);
			String fileNameContains = properties.getProperty("file_name_contains", null);
			
			if(url != null && username != null && password != null && containerName != null){
				ThreadPoolExecutor pool = new ThreadPoolExecutor(8, 8, 0,
						TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>());
				List<StoredObject> validObjects = getValidObjects(username, password, url, containerName, from, until, fileNameContains);
				
				//Download each file and insert
				for(StoredObject object: validObjects) {
					String name = object.getName();
					object.downloadObject(new File(name));
					pool.execute(new ESInsert(insertES, name, layerName, twitter4jFormat));
				}
			}
			
			//User can insert a local file too
			String filePath = properties.getProperty("file_path");
			
			if(filePath == null) {
				System.err.println("No remote object storage neither local File Path was setted on properties file " +propertiesFile);
				System.exit(0);
			}
			
			insertES.insertFile(filePath, layerName, twitter4jFormat);
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private static List<StoredObject> getValidObjects(String username, String password, String url, String containerName, String from, String until, String fileNameContains) throws ParseException{
		List<StoredObject> validObjects = new ArrayList<StoredObject>();
		
		AccountConfig config = new AccountConfig();
		config.setUsername(username);
		config.setPassword(password);
		config.setAuthUrl(url);
		config.setAuthenticationMethod(AuthenticationMethod.BASIC);

		Account account = new AccountFactory(config).createAccount();

		// Listando os dados do container
		Container container = account.getContainer(containerName);
		
		Collection<StoredObject> list = container.list();
		for(StoredObject object: list){
			Date lastModifiedAsDate = object.getLastModifiedAsDate();
			DateFormat format = DateFormat.getDateInstance();
			
			if(from != null) {
				Date fromDate = format.parse(from);
				if(fromDate.after(lastModifiedAsDate))
					continue;
			}
			
			if(until != null) {
				Date untilDate = format.parse(until);
				if(untilDate.before(lastModifiedAsDate))
					continue;
			}
			
			if(fileNameContains != null) {
				if(!object.getName().contains(fileNameContains))
					continue;
			}
			
			validObjects.add(object);
		}
		
		return validObjects;
	}
	
	private void insertFile(String filePath, String layerName, boolean twitter4jFormat) throws IOException, InterruptedException{
		BufferedReader br = new BufferedReader(new FileReader(filePath));
		long t = System.currentTimeMillis();
		long inserted = 0;
		int sizeBulk = 0;
		long numErrors = 0;
		ThreadPoolExecutor pool = new ThreadPoolExecutor(4, 4, 0,
				TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>());

		long lineWithErrorsOnFile = 0;
		BulkRequestBuilder bulkRequest = client.prepareBulk();
		String line = br.readLine();
		while (line != null) {
			XContentBuilder xBuilder = null;
			String id = null;
			try {
				//If is not twitter4j format, so it should parse the json to twitter4j format.
				if(!twitter4jFormat)
					line = LowerCaseKeyDeserializer.formatToES(line);
				
				Status status = TwitterObjectFactory.createStatus(line);
				id = Long.toString(status.getId());
				xBuilder = TwitterInsertBuilder.constructInsertBuilder(status, true, false);
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
						pool.execute(new BulkInsert(bulkRequest));
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
		
		br.close();
		pool.shutdown();
		pool.awaitTermination(1, TimeUnit.DAYS);
		this.close();

		System.out.println("Time to insert: "
				+ (System.currentTimeMillis() - t));
		System.out.println("Number of lines with error: " +lineWithErrorsOnFile);
		System.exit(0);
	}
	
	public void close() {
		client.close();
	}

	private static class BulkInsert implements Runnable {
		BulkRequestBuilder bulkRequest;
		
		public BulkInsert(BulkRequestBuilder bulkRequest) {
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
	
	private static class ESInsert implements Runnable {
		private DirectInsertES insertES;
		private String filePath;
		private String layerName;
		private boolean twitter4jFormat;
		
		public ESInsert(DirectInsertES insertES, String filePath,
				String layerName, boolean twitter4jFormat) {
			this.insertES = insertES;
			this.filePath = filePath;
			this.layerName = layerName;
			this.twitter4jFormat = twitter4jFormat;
		}

		@Override
		public void run() {
			try {
				insertES.insertFile(filePath, layerName, twitter4jFormat);
			} catch (IOException | InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
	
	
}