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
	private Client client;

	private String queryIndexAliasName;
	private String insertIndexAliasName;
	private String indexName;

	private String typeName;

	private final long maxEachAliasIndexSize;
	private final long maxIndexSize;
	private final int numShards;
	private final int replicationFactor;

	private int indexVersion = 1;
	private long indexSize = 0;
	private long totalIndexSize = 0;
	
	public TwitterElasticSearchIndexing(Client client, String queryIndexAliasName,
			String insertIndexAliasName, String indexName, String typeName,
			long maxEachAliasIndexSize, long maxIndexSize, int numShards,
			int replicationFactor) {
		this.client = client;
		this.queryIndexAliasName = queryIndexAliasName;
		this.insertIndexAliasName = insertIndexAliasName;
		this.indexName = indexName;
		this.typeName = typeName;
		this.maxEachAliasIndexSize = maxEachAliasIndexSize;
		this.maxIndexSize = maxIndexSize;
		this.numShards = numShards;
		this.replicationFactor = replicationFactor;
	}

	public void initializeSlaveIndexControl() throws InterruptedException,
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
		}
	}

	public void initializeMasterIndexControl() {
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

	public synchronized void verifyIndexSize(int numTweetsReceived) {
		indexSize += numTweetsReceived;
		totalIndexSize += numTweetsReceived;
		if (indexSize >= maxEachAliasIndexSize) {
			// remove previous insert alias to insert on new index
			client.admin()
					.indices()
					.aliases(
							new IndicesAliasesRequest().removeAlias(indexName,
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
			client.admin()
					.indices()
					.aliases(
							new IndicesAliasesRequest().addAlias(
									queryIndexAliasName, indexName))
					.actionGet();
			// point the insert alias to new index
			client.admin()
					.indices()
					.aliases(
							new IndicesAliasesRequest().addAlias(
									insertIndexAliasName, indexName))
					.actionGet();

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
