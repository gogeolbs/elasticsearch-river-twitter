package org.elasticsearch.river.twitter;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.util.Collection;
import java.util.Comparator;
import java.util.Date;
import java.util.Locale;
import java.util.Properties;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.elasticsearch.action.WriteConsistencyLevel;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.support.replication.ReplicationType;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.ImmutableSettings.Builder;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
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

	private static Client client;
	private static TwitterElasticSearchIndexing elasticIndexing;
	private static final long DEFAULT_MAX_EACH_INDEX_SIZE = 100000000;
	private static final long DEFAULT_MAX_INDEX_SIZE = 10000000000L;
	private static final int DEFAULT_NUM_SHARDS = 5;
	private static final int DEFAULT_REPLICATION_FACTOR = 1;
	private static final int BULK_SIZE = 1500;
	
	@SuppressWarnings("resource")
	public DirectInsertES(String[] seeds, String elasticCluster, String indexName, long maxEachAliasIndexSize, long maxIndexSize, int numShards, int replicationFactor) throws Exception {
		TransportAddress[] transportAddresses = new InetSocketTransportAddress[seeds.length];
		for(int i = 0; i < seeds.length; i++)
			transportAddresses[i] = new InetSocketTransportAddress(seeds[i], 9300);
		
		Builder builder = ImmutableSettings.settingsBuilder().put(
				"cluster.name", elasticCluster);
		client = new TransportClient(builder.build())
				.addTransportAddresses(transportAddresses);
		
		//FIXME Fix this alias code
		String insertIndexAliasName = indexName +"_index";
        String queryIndexAliasName = indexName;
        String queryAllIndexAliasName = indexName +"_all";
        String typeName = "attr";
        
		elasticIndexing = new TwitterElasticSearchIndexing(client, queryIndexAliasName, insertIndexAliasName, indexName, typeName, maxEachAliasIndexSize, maxIndexSize, numShards, replicationFactor, 0, queryAllIndexAliasName);
	}
	
	public static void main(String[] args) {
		try {
			if(args.length != 1) {
				System.out.println("You should enter properties file");
				System.exit(0);
			}
			
			System.out.println("Default Locale  : " + Locale.getDefault());

			System.out.println("File Enconding  : " + System.getProperty("file.encoding"));
			
			String propertiesFile = args[0]; 
			Properties properties = new Properties();
			properties.load(new FileReader(propertiesFile));
			
			String seedList=  properties.getProperty("seed", null);
			String elasticCluster = properties.getProperty("elastic_cluster", null);
			
			if(seedList == null || elasticCluster == null){
				System.err.println("Set the ElasticSearch seed and cluster name on properties file " +propertiesFile);
				System.exit(0);
			}
			
			System.out.println("ElasticSearch IPs: " + seedList +". Elastic cluster: " + elasticCluster);

			String[] seeds = seedList.split(",");
			String layerName = properties.getProperty("layer_name", null);
			
			if(layerName == null){
				System.err.println("Set the layer name name on properties file " +propertiesFile);
				System.exit(0);
			}

			long maxEachAliasIndexSize = Long.parseLong(properties.getProperty("max_each_alias_index_size", String.valueOf(DEFAULT_MAX_EACH_INDEX_SIZE)));
			long maxIndexSize = Long.parseLong(properties.getProperty("max_index_size", String.valueOf(DEFAULT_MAX_INDEX_SIZE)));
			int numShards = Integer.parseInt(properties.getProperty("num_shards", String.valueOf(DEFAULT_NUM_SHARDS)));
			int replicationFactor = Integer.parseInt(properties.getProperty("replication_factor", String.valueOf(DEFAULT_REPLICATION_FACTOR)));
			
			DirectInsertES insertES = new DirectInsertES(seeds, elasticCluster, layerName, maxEachAliasIndexSize, maxIndexSize, numShards, replicationFactor);
			
			boolean twitter4jFormat = Boolean.parseBoolean(properties.getProperty("files_twitter4j_format", "false"));
			boolean indexSizeControl = Boolean.parseBoolean(properties.getProperty("index_size_control", "true"));
			
			//initialize the ES index if it 
			if(indexSizeControl)
				elasticIndexing.initializeSlaveIndexControl();
			else
				elasticIndexing.createIndex();
			
			//User can insert a local file too
			String filePath = properties.getProperty("file_path");
			if(filePath != null) 
				insertES.insertFile(filePath, layerName, twitter4jFormat);
			
			//Remote storage properties
			String url = properties.getProperty("url", null);
			String username = properties.getProperty("username", null);
			String password = properties.getProperty("password", null);
			String containerName = properties.getProperty("container_name", null);
			String from = properties.getProperty("from_date", null);
			String until = properties.getProperty("until_date", null);
			String fileNameContains = properties.getProperty("file_name_contains", null);
			int numMaxFilesToInsert = Integer.parseInt(properties.getProperty("num_max_files", String.valueOf(Integer.MAX_VALUE)));
			int numThreads = Integer.parseInt(properties.getProperty("num_threads_to_insert", String.valueOf("1")));

			
			if(url != null && username != null && password != null && containerName != null) {
				ThreadPoolExecutor pool = new ThreadPoolExecutor(numThreads, numThreads, 1, TimeUnit.DAYS, new LinkedBlockingQueue<Runnable>());
				SortedSet<StoredObject> validObjects = getValidObjects(username, password, url, containerName, from, until, fileNameContains);
				
				int numFilesInserted = 0;
				//Download each file and insert
				for(StoredObject object: validObjects) {
					pool.execute(new ThreadInsert(insertES, layerName, twitter4jFormat, object));
					
					//Control the number of files to insert
					numFilesInserted++;
					if(numFilesInserted >= numMaxFilesToInsert) {
						System.out.println("Inserting " +numMaxFilesToInsert +" files.");
						break;
					}
				}
				pool.shutdown();
				pool.awaitTermination(10, TimeUnit.DAYS);
			}
			
			client.close();
			System.exit(0);
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private static SortedSet<StoredObject> getValidObjects(String username, String password, String url, String containerName, String from, String until, String fileNameContains) throws ParseException{
		//sorted set by date in a decreasing order.
		SortedSet<StoredObject> validObjects = new TreeSet<StoredObject>(new StoreObjectComp());
		
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
				if(!object.getName().contains(fileNameContains) && !object.getName().matches(fileNameContains))
					continue;
			}
			
			validObjects.add(object);
		}
		
		return validObjects;
	}
	
	private void insertFile(String filePath, String layerName, boolean twitter4jFormat) throws IOException, InterruptedException, ExecutionException{
		File file = new File(filePath);
		if(!file.exists()) {
			System.out.println("File " +file.getAbsolutePath() +" doesnt exists.");
			return;
		}
		BufferedReader br = new BufferedReader(new FileReader(file));
		long t = System.currentTimeMillis();
		long inserted = 0;
		int sizeBulk = 0;
		long numErrors = 0;

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
				
				if(xBuilder == null)
					continue;
			}catch (Exception e) {
				lineWithErrorsOnFile++;
				line = br.readLine();
				continue;
			}
			
			IndexRequestBuilder insert = client.prepareIndex(layerName, ATTR)
					.setSource(xBuilder).setRefresh(false).setId(id)
					.setConsistencyLevel(WriteConsistencyLevel.ONE).setReplicationType(ReplicationType.ASYNC);

			bulkRequest.add(insert);
			sizeBulk++;

			line = br.readLine();
			if (sizeBulk > BULK_SIZE || line == null || line.equals("]")) {
				while(true) {
					try {
						insert(bulkRequest);
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

		System.out.println("Time to insert: "
				+ (System.currentTimeMillis() - t));
		System.out.println("Number of lines with error: " +lineWithErrorsOnFile);
	}
	
	private void insert(BulkRequestBuilder bulkRequest){
		BulkResponse bulkResponse = null;
		while (true) {
			try {
				bulkResponse = bulkRequest.execute().actionGet();
				break;
			} catch(NoNodeAvailableException e1){
				waitNodeBeAvailable();
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
	private void waitNodeBeAvailable(){
		try {
			for(int i = 0;i < 10; i++) {
				Thread.sleep(1000 * i);
				try {
					if (client.admin().cluster()
							.health(new ClusterHealthRequest()).get()
							.getStatus().equals(ClusterHealthStatus.RED)) {
						System.out.println("WAIT YELLOW");
						client.admin().cluster().prepareHealth()
								.setWaitForYellowStatus().get();
						continue;
					}
				} catch(NoNodeAvailableException e) {
					continue;
				}
			}
		} catch(Exception e3) {
			System.err.println("Error to wait node be available. Error: " +e3.getMessage());
			e3.printStackTrace();
			client.close();
			System.exit(0);
		}
	}
	
	private static class StoreObjectComp implements Comparator<StoredObject>{
		 
	    @Override
	    public int compare(StoredObject e1, StoredObject e2) {
	        return e2.getLastModifiedAsDate().compareTo(e1.getLastModifiedAsDate());
	    }
	}
	
	private static class ThreadInsert implements Runnable {
		private DirectInsertES insertES;
		private String layerName;
		private boolean twitter4jFormat;
		private StoredObject object;
		
		public ThreadInsert(DirectInsertES insertES, String layerName,
				boolean twitter4jFormat, StoredObject object) {
			super();
			this.insertES = insertES;
			this.layerName = layerName;
			this.twitter4jFormat = twitter4jFormat;
			this.object = object;
		}

		@Override
		public void run() {
			try {
				String name = object.getName();
				File file = new File(name);
				System.out.println("Downloading file " +name +" ...");
				object.downloadObject(file);
				System.out.println("Downloading file " +name +" FINISHED");
				System.out.println("Decompressing file " +name +" ...");
				Process p = Runtime.getRuntime().exec(
						"tar -jxvf " + file.getName());
				p.waitFor();
				System.out.println("File " +name +" DECOMPRESSED");
				//delete compacted file
				file.delete();
				if(name.endsWith(".tar.bz2"))
					name = name.replace(".tar.bz2", "");
				
				//Some files are enclosed in data directory
				if(!new File(name).exists())
					name = "data/" +name +".json";
				
				System.out.println("Inserting file " +name +" from " +object.getLastModified() +" ...");
				insertES.insertFile(name, layerName, twitter4jFormat);
				//delete json file
				new File(name).delete();
				System.out.println("Insert file " +name +" FINISHED");
			} catch(Exception e) {
				e.printStackTrace();
			}
		}
		
	}
}