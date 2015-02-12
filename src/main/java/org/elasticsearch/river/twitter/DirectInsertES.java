package org.elasticsearch.river.twitter;

import java.io.BufferedReader;
import java.io.FileReader;
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
import org.elasticsearch.river.twitter.utils.LowerCaseKeyDeserializer;
import org.elasticsearch.river.twitter.utils.TwitterInsertBuilder;

import twitter4j.Status;
import twitter4j.TwitterObjectFactory;

public class DirectInsertES {
  static final String ATTR = "attr";

  public static int WGS84_srid = 4326;

  private Client client;

  @SuppressWarnings("resource")
  public DirectInsertES(String[] seeds, int[] ports, String layerName,
      String elasticCluster, String filePath) throws Exception {

    TransportAddress[] seedAddresses = new TransportAddress[seeds.length];
    for(int i =0; i < seeds.length;i++)
      seedAddresses[i] = new InetSocketTransportAddress(seeds[i], ports[i]);

    Builder builder = ImmutableSettings.settingsBuilder().put(
        "cluster.name", elasticCluster);
    client = new TransportClient(builder.build())
        .addTransportAddresses(seedAddresses);
    
    BufferedReader br = new BufferedReader(new FileReader(filePath));
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
      XContentBuilder xBuilder = null;
      String id = null;
      try {
        String json = LowerCaseKeyDeserializer.formatToES(line);
        Status status = TwitterObjectFactory.createStatus(json);
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
      if(args.length != 4) {
        System.out.println("You should enter the following arguments: [elasticsearch ip]:[elasticsearch port] [elasticsearch index name] [elasticsearch cluster name] [json file path]");
        System.exit(0);
      }
      //WARN Configurações do welder 
      String seed = args[0];
      String layerName = args[1];
      String elasticCluster = args[2];
      String filePath = args[3];

      System.out.println("ElasticSearch IP: " + seed);
      System.out.println("Layer name: " + layerName);
      System.out.println("Elastic cluster: " + elasticCluster);

      int defaultPort = 9300;

      String[] seeds = new String[seed.split(";").length];
      int[] ports = new int[seed.split(";").length]; 
      
      for (int i = 0; i < seeds.length; i++) {
        String[] s = seed.split(";")[i].split(":");
        if (s.length == 2) {
          seeds[i] = s[0];
          ports[i] = Integer.parseInt(s[1]);
        } else {
          ports[i] = defaultPort;
        }
      }

      new DirectInsertES(seeds, ports, layerName, elasticCluster, filePath);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}