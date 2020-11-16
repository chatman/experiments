import java.io.File;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FileUtils;
import org.apache.solr.client.solrj.embedded.JettyConfig;
import org.apache.solr.cloud.MiniSolrCloudCluster;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.ZKUtil;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.json.JSONException;
import org.json.JSONObject;

public class StateListVsCASSpinlock {

	public static MiniSolrCloudCluster cluster = null;
	public static ZooKeeper zk = null;

	public final static String STATE_JSON = "/state.json";
	public final static String STATE_LIST = "/states";

	public static Random rnd = new Random();
	
	public static void main(String[] args) throws KeeperException, InterruptedException, Exception {

		// Start a ZK Server and connect to it
		connect();

		testCASSpinlock(500);
		testStatesList(500);
		
		cluster.shutdown();
		FileUtils.deleteDirectory(new File("solrdir"));
	}

	public static void testCASSpinlock(int numReplicas) throws Exception {
		try {
			zk.create(STATE_JSON, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		} catch (KeeperException.NodeExistsException ex) {
			// no worries, node already existed
		}
		try {
			zk.create(STATE_LIST, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		} catch (KeeperException.NodeExistsException ex) {
			// no worries, node already existed
		}

		// initialize state.json
		{
			long start = System.nanoTime();

			JSONObject clusterstate = new JSONObject();
			for (int i=1; i<=numReplicas; i++) {
				clusterstate.put("shard"+i+"_replica1", "DOWN");
			}
			zk.setData(STATE_JSON, clusterstate.toString(1).getBytes(), -1);
			System.out.println("Time to initialize (CAS): "+(System.nanoTime()-start)/1000000.0+"ms");
		}

		long start = System.nanoTime();
		ExecutorService executors = Executors.newFixedThreadPool(12);
		for (int i=1; i<=numReplicas; i++) {
			executors.submit(new Thread(new UpdateZNode("shard"+i+"_replica1")));
		}
		executors.shutdown();
		executors.awaitTermination(1000, TimeUnit.SECONDS);
		System.out.println("Time to update (CAS): "+(System.nanoTime()-start)/1000000.0+"ms");

	}

	public static void testStatesList(int numReplicas) throws Exception {
		// initialize states list
		{
			long start = System.nanoTime();
			ZKUtil.deleteRecursive(zk, STATE_LIST);
			zk.create(STATE_LIST, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

			for (int i=1; i<=numReplicas; i++) {
				zk.create(STATE_LIST + "/shard"+i+"_replica1::DOWN", null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
				if (i%2000==0) System.err.println(i + " children created...");
			}
			System.out.println("Time to initialize (States List): "+(System.nanoTime()-start)/1000000.0+"ms");
		}

		long start = System.nanoTime();
		ExecutorService executors = Executors.newFixedThreadPool(12);
		for (int i=1; i<=numReplicas; i++) {
			executors.submit(new UpdateStateList("shard"+i+"_replica1"));
		}
		executors.shutdown();
		executors.awaitTermination(1000, TimeUnit.SECONDS);
		System.out.println("Time to update (States List): "+(System.nanoTime()-start)/1000000.0+"ms");
	}

	public static class UpdateStateList implements Runnable {
		public final String replica;

		public UpdateStateList(String replica) {
			this.replica = replica;
		}
		public void run() {

			/*String previousNode = null;
			try {
				for (String child: zk.getChildren(STATE_LIST, false)) {
					if (child.startsWith(replica+"::")) { previousNode = child; break; }
				}
			} catch (KeeperException e1) {
				e1.printStackTrace();
			} catch (InterruptedException e1) {
				e1.printStackTrace();
			}*/
			List<Op> ops = Arrays.asList(
					Op.create(STATE_LIST + "/" + replica + "::ACTIVE", null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT),
					Op.delete(STATE_LIST + "/" + replica + "::DOWN", -1));
			//Op.delete(previousNode, -1));
			try {
				zk.multi(ops);
			} catch (InterruptedException e) {
				e.printStackTrace();
			} catch (KeeperException e) {
				e.printStackTrace();
			}

		}		
	}

	public static class UpdateZNode implements Runnable {
		public final String replica;

		public UpdateZNode(String replica) {
			this.replica = replica;
		}
		public void run() {
			Stat stat = new Stat();
			JSONObject clusterstate = null;
			while (true) { 
				try {
					clusterstate = new JSONObject(new String(zk.getData(STATE_JSON, false, stat)));
				} catch (JSONException e) {
					e.printStackTrace();
				} catch (KeeperException e) {
					e.printStackTrace();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}

				clusterstate.put(replica, "ACTIVE");

				try {
					int updateVersion = stat.getVersion();
					zk.setData(STATE_JSON, clusterstate.toString(1).getBytes(), updateVersion);
				} catch (JSONException e) {
					e.printStackTrace();
				} catch (KeeperException e) {
					if (e instanceof KeeperException.BadVersionException) {
						try {
							Thread.sleep(rnd.nextInt(20));
						} catch (InterruptedException e1) {
							e1.printStackTrace();
						}
						continue;
					}
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				break;
			}
		}		
	}
	
	private static void connect() throws Exception {
		//String zkString = "localhost:2181";
		FileUtils.deleteDirectory(new File("solrdir"));
		cluster = new MiniSolrCloudCluster(1, Paths.get("solrdir"), JettyConfig.builder().setPort(58983).build());
		String zkString = "localhost:"+cluster.getZkServer().getPort();
		
		System.out.println("ZK url : "+ zkString);
		zk = new ZooKeeper(zkString, 15000, null);
		
		System.out.println("Connected..");
	}
}
