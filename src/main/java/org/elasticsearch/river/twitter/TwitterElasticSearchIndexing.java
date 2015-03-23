package org.elasticsearch.river.twitter;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.exists.AliasesExistResponse;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsRequest;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.hppc.cursors.ObjectCursor;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.IndexAlreadyExistsException;
import org.elasticsearch.river.twitter.utils.TwitterInfo;

public class TwitterElasticSearchIndexing {
	//How many real time twitter clients are inserting? Put here.
	private static final int NUM_INSERT_CLIENTS = 5;
	private static final String REFRESH_INTERVAL = "30s";
	private Client client;

	private String queryIndexAliasName;
	private String insertIndexAliasName;
	private String indexName;
	private String originalIndexName;
	private String queryAllIndexAliasName;

	private String typeName;

	private final long maxEachAliasIndexSize;
	private final long maxIndexSize;
	private final int numShards;
	private final int replicationFactor;

	private int indexVersion = 1;
	private long indexSize = 0;
	private long totalIndexSize = 0;
	private int numIndexesToPoint;
	private int numIndexesActuallyPointed = 0;
	
	
	public TwitterElasticSearchIndexing(Client client, String queryIndexAliasName,
			String insertIndexAliasName, String indexName, String typeName,
			long maxEachAliasIndexSize, long maxIndexSize, int numShards,
			int replicationFactor, int numIndexesToPoint, String queryAllIndexAliasName) {
		this.client = client;
		this.queryIndexAliasName = queryIndexAliasName;
		this.queryAllIndexAliasName = queryAllIndexAliasName;
		this.insertIndexAliasName = insertIndexAliasName;
		this.indexName = indexName;
		this.originalIndexName = indexName;
		this.typeName = typeName;
		this.maxEachAliasIndexSize = maxEachAliasIndexSize;
		this.maxIndexSize = maxIndexSize;
		this.numShards = numShards;
		this.replicationFactor = replicationFactor;
		this.numIndexesToPoint = numIndexesToPoint;
	}
	
	public void createIndex() throws InterruptedException, ExecutionException {
		boolean indiceExist = client.admin().indices()
				.exists(new IndicesExistsRequest(indexName)).get().isExists();

		if (!indiceExist) {
			Settings indexSettings = ImmutableSettings.settingsBuilder()
					.put("number_of_shards", numShards)
					.put("number_of_replicas", replicationFactor)
					.put("refresh_interval", REFRESH_INTERVAL).build();

			client.admin().indices().prepareCreate(indexName)
					.setSettings(indexSettings).execute().actionGet();

			if (client.admin().indices().prepareGetMappings(indexName)
					.setTypes(typeName).get().getMappings().isEmpty()) {
				createMapping();
			}
		}
	}

	public void initializeSlaveIndexControl() throws InterruptedException,
			ExecutionException {
		int[] versions = getMinAndMaxVersion(true);

		if (versions == null) {
			indexName = indexName + "_" + indexVersion;
			Settings indexSettings = ImmutableSettings.settingsBuilder()
					.put("number_of_shards", numShards)
					.put("number_of_replicas", replicationFactor)
					.put("refresh_interval", REFRESH_INTERVAL).build();

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
			client.admin()
			.indices()
			.aliases(
					new IndicesAliasesRequest().addAlias(
							queryAllIndexAliasName, indexName))
			.actionGet();

			if (client.admin().indices().prepareGetMappings(indexName)
					.setTypes(typeName).get().getMappings().isEmpty()) {
				createMapping();
			}
		}
	}

	public void initializeMasterIndexControl() {
		try {
			int[] versions = getMinAndMaxVersion(true);
			if (versions == null) {
				indexName = indexName + "_" + indexVersion;
				Settings indexSettings = ImmutableSettings.settingsBuilder()
						.put("number_of_shards", numShards)
						.put("number_of_replicas", replicationFactor)
						.put("refresh_interval", REFRESH_INTERVAL).build();
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
				client.admin()
				.indices()
				.aliases(
						new IndicesAliasesRequest().addAlias(
								queryAllIndexAliasName, indexName))
				.actionGet();

				if (client.admin().indices().prepareGetMappings(indexName)
						.setTypes(typeName).get().getMappings().isEmpty()) {
					createMapping();
				}
				
				numIndexesActuallyPointed++;
			} else {
				indexVersion = versions[1]; // get latest index version
				indexName = originalIndexName + "_" + indexVersion;
				indexSize = getIndexSize(indexName) / NUM_INSERT_CLIENTS;
				totalIndexSize = getIndexSize(queryAllIndexAliasName);
				verifyAlias();
			}
		} catch (Exception e) {
			if (ExceptionsHelper.unwrapCause(e) instanceof IndexAlreadyExistsException) {
				indexVersion = getMinAndMaxVersion(true)[1]; // get latest index version

				indexName = originalIndexName + "_" + indexVersion;
				indexSize = getIndexSize(indexName) / NUM_INSERT_CLIENTS;
				totalIndexSize = getIndexSize(queryAllIndexAliasName);
				verifyAlias();

			} else if (ExceptionsHelper.unwrapCause(e) instanceof ClusterBlockException) {
			} else {
				e.printStackTrace();
			}
		}
	}

	public void verifyAlias() {
		try {
			AliasesExistResponse response = client
					.admin()
					.indices()
					.aliasesExist(
							new GetAliasesRequest(insertIndexAliasName)
									.indices(indexName)).get();
			if (!response.exists())
				client.admin()
						.indices()
						.aliases(
								new IndicesAliasesRequest().addAlias(
										insertIndexAliasName, indexName))
						.actionGet();
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

	private int[] getMinAndMaxVersion(boolean entireIndex) {
		int[] versions = new int[2];

		GetAliasesResponse response = client.admin().indices()
				.getAliases(new GetAliasesRequest(queryIndexAliasName))
				.actionGet();
		numIndexesActuallyPointed = response.getAliases().size();
		
		//get min max version from entire index
		if(entireIndex)
			response = client.admin().indices()
					.getAliases(new GetAliasesRequest(queryAllIndexAliasName))
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

	public synchronized void verifyIndexSize(int numTweetsReceived) {
		indexSize += numTweetsReceived;
		totalIndexSize += numTweetsReceived;
		if (indexSize >= maxEachAliasIndexSize) {
			
			String oldIndexName = indexName;
			indexName = originalIndexName + "_" + ++indexVersion;

			Settings indexSettings = ImmutableSettings.settingsBuilder()
					.put("number_of_shards", numShards)
					.put("number_of_replicas", replicationFactor)
					.put("refresh_interval", REFRESH_INTERVAL).build();

			// create new empty index
			client.admin().indices().prepareCreate(indexName)
					.setSettings(indexSettings).execute().actionGet();
			createMapping();
			
			//update the insert alias
			client.admin().indices().aliases(
					new IndicesAliasesRequest().addAlias(
							insertIndexAliasName, indexName));
			client.admin().indices().aliases(
							new IndicesAliasesRequest().removeAlias(oldIndexName,
									insertIndexAliasName));
			
			// point the query alias to new index
			client.admin()
					.indices()
					.aliases(
							new IndicesAliasesRequest().addAlias(
									queryIndexAliasName, indexName))
					.actionGet();
			// point the query alias to entire index
			client.admin()
					.indices()
					.aliases(
							new IndicesAliasesRequest().addAlias(
									queryAllIndexAliasName, indexName))
					.actionGet();
			
			if(numIndexesToPoint <= numIndexesActuallyPointed){
				int minVersion = getMinAndMaxVersion(false)[0];
				
				// remove previous query alias
				client.admin()
						.indices()
						.aliases(
								new IndicesAliasesRequest().removeAlias(originalIndexName +"_" +minVersion,
										queryIndexAliasName)).actionGet();
			} else
				numIndexesActuallyPointed++;

			// Delete the oldest index if the total size of the indexes is
			// greater than maxIndexSize to control the index size
			if (totalIndexSize > maxIndexSize) {
				int minVersion = getMinAndMaxVersion(true)[0];
				String indexToDelete = originalIndexName + "_" + minVersion;
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
