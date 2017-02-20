package com.mycom.zookeeper;

import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

public class ZkClient {
	private String connectString;
	private int timeout;
	private ZooKeeper zookeeper;

	public ZkClient(String connectString, int timeout) {
		this.connectString = connectString;
		this.timeout = timeout;
		connect();
	}

//	@Override
//	public void process(WatchedEvent event) {
//		switch (event.getState()) {
//		case SyncConnected:// 自动重连
//			// synchronized (this) {
//			// isConnected = true;
//			// notifyAll();
//			// }
//			System.out.println(Thread.currentThread().getName() + " Connected...");
//			break;
//		case Expired:
//			isConnected = false;
//			System.out.println(Thread.currentThread().getName() + " Expired(重连)...");
//			// connect();
//			break;
//		case Disconnected:
//			isConnected = false;
//			System.out.println(Thread.currentThread().getName() + " 链接断开，或session迁移....");
//			// connect();
//			break;
//		case AuthFailed:
//			isConnected = false;
//			close();
//			throw new RuntimeException("ZK Connection auth failed...");
//		default:
//			break;
//		}
//
//	}

	public void connect() {
		ZooKeeper zk = null;
		final CountDownLatch latch = new CountDownLatch(1);
		try {
			zk = new ZooKeeper(connectString, timeout, new Watcher() {
				@Override
				public void process(WatchedEvent event) {
					System.out.println("current event :"+ event.getState().toString());
					if (KeeperState.SyncConnected == event.getState()) {
						latch.countDown();
						System.out.println(Thread.currentThread().getName() + " connect to:" + connectString + " success!");
					}
				}

			});
			latch.await();
			
			//System.out.println(Thread.currentThread().getName() + " connect to:" + connectString + " success!");
			this.zookeeper = zk;

		} catch (Exception e) {
			System.out.println("connect to:" + connectString + " failed!");
			e.printStackTrace();
		}

	}

	public boolean exists(String path) throws Exception {
		Stat stat = zookeeper.exists(path, false);
		return stat != null;
	}

	public String getData(String path) throws Exception {
		byte[] data = zookeeper.getData(path, false, null);
		return new String(data, "UTF-8");
	}

	public boolean setData(String path, String data) throws Exception {
		Stat stat = zookeeper.setData(path, data.getBytes("UTF-8"), -1);
		return stat != null;
	}

	public String create(String path, byte[] data) throws Exception {
		String result = zookeeper.create(path, data, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		// 创建永久性节点
		return result;
	}

	public void delete(String path) throws Exception {
		if (!exists(path)) {
			System.out.println("path :" + path + " is not exist!");
			return;
		}
		zookeeper.delete(path, -1);
	}

	public List<String> getChildren(String path) throws Exception {
		List<String> childrenNodes = zookeeper.getChildren(path, false);
		return childrenNodes;
	}

	public boolean hasChildren(String path) throws Exception {
		List<String> childrenNodes = this.getChildren(path);
		if (childrenNodes == null || childrenNodes.size() == 0) {
			return false;
		}
		return true;
	}

	public void close() {
		try {
			if (zookeeper != null) {
				zookeeper.close();
			}
			System.out.println("zookeeper disconnect.");
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) throws Exception {
		ZkClient client = new ZkClient("localhost:2181", 5000);

		// List<String> dirs = client.getChildren("/");
		// System.out.println("sub path :");
		// for (String s : dirs)
		// System.out.println(s);

		String data = client.getData("/demo1");
		System.out.println("data in demo1 :" + data);

		// System.out.println("/zk_test exists:" + client.exists("/zk_test"));
		// System.out.println("/zk_test/aaa exists:" +
		// client.exists("/zk_test/aaa"));

		// System.out.println("/zk_test hasChildren:" +
		// client.hasChildren("/zk_test"));
		// client.create("/zk_test/aaa", "aaa's data".getBytes());
		//
		// client.setData("/demo1", "demo1 new data");

		client.delete("/zk_test/aaa");
		// client.close();
		for (;;)
			Thread.sleep(Integer.MAX_VALUE);

	}

}
