package io.uve.yypush.zookeeper;

import io.uve.yypush.Sailing;
import io.uve.yypush.config.Config;
import io.uve.yypush.json.JsonReader;
import io.uve.yypush.model.ChangeNode;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.joda.time.DateTime;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * 
 * @author yangyang21@staff.weibo.com(yangyang)
 * 
 */
public class ZkConfig implements Watcher {
	private static Logger log = Logger.getLogger(ZkConfig.class);
	public final static String zkBase = "/logpush";
	public final static String zkBaseMonitor = "/logpushMonitor";
	public ZooKeeper zk = null;
	public Stat stat = null;
	public final ReentrantLock fatherLock = new ReentrantLock();

	public ZkConfig(String connectStr) {
		try {
			this.zk = new ZooKeeper(connectStr, 6000, this);
		} catch (IOException e) {
			e.printStackTrace();
			log.info("init zookeeper failed");
		}
	}

	@Override
	public void process(WatchedEvent event) {
		if (KeeperState.SyncConnected == event.getState()) {
			if (EventType.None == event.getType() && null == event.getPath()) {

			} else if (event.getType() == EventType.NodeChildrenChanged) {
				try {
					ZookeeperNodeLock.instance.lock();
					List<String> ss = zk.getChildren(event.getPath(), true);
					if (ss == null) {
						ss = Lists.newArrayList();
					}
					log.info("reget config name:" + ss);

					ChangeNode node = new ChangeNode();
					Map<String, String> map = Maps.newHashMap();
					for (String s : ss) {
						String son = event.getPath() + "/" + s;
						String configStr = new String(zk.getData(son, true, stat));
						log.info(son + " : " + configStr);
						map.put(son, configStr);
					}
					node.setChilds(ss);
					node.setUseMap(false);
					node.setMap(map);

					ZookeeperNodeLock.instance.addchange(node);
				} catch (Exception e) {
					log.info("notify load config error!");
				} finally {
					ZookeeperNodeLock.instance.signalAll();
					ZookeeperNodeLock.instance.unlock();
				}
			} else if (event.getType() == EventType.NodeDataChanged) {
				try {
					ZookeeperNodeLock.instance.lock();
					String name = event.getPath();
					String configStr;
					configStr = new String(zk.getData(event.getPath(), true, stat));
					log.info("path data change:" + event.getPath());

					ChangeNode node = new ChangeNode();
					Map<String, String> map = Maps.newHashMap();
					map.put(name, configStr);
					node.setChilds(null);
					node.setUseMap(true);
					node.setMap(map);

					ZookeeperNodeLock.instance.addchange(node);
				} catch (KeeperException e) {
					e.printStackTrace();
				} catch (InterruptedException e) {
					e.printStackTrace();
				} finally {
					ZookeeperNodeLock.instance.signalAll();
					ZookeeperNodeLock.instance.unlock();
				}
			}
		} else if (KeeperState.Expired == event.getState()) {
			log.info("zookeeper expared and begin to restart");
			ZkFactory.resetZkConfig();
			log.info("zookeeper restart finished");
		}
	}

	public Map<String, Config> LoadingConfig() throws KeeperException, InterruptedException {
		final Map<String, Config> configs = Maps.newHashMap();
		this.stat = new Stat();
		List<String> cl = zk.getChildren(zkBase, true);

		if (cl == null || cl.size() == 0) {
			log.info("load config from zookeeper null");
		} else {
			for (String k : cl) {
				String son = zkBase + '/' + k;
				String confStr = new String(zk.getData(son, true, stat));

				if (confStr != null && !confStr.isEmpty()) {
					try {
						Config config = JsonReader.getObjectMapper().readValue(confStr, Config.class);
						config.name = son;
						configs.put(son, config);
						log.info("load config success: " + k);
					} catch (IOException e) {
						log.info("load config error:" + k);
						e.printStackTrace();
					}
				}
			}
		}
		log.info("finished init config");
		return configs;
	}

	public void createFather() throws InterruptedException, KeeperException{
		final String ip = Sailing.acceptIp.get();
		fatherLock.lock();
		try {
			Stat tmpstat = this.zk.exists(zkBaseMonitor, false);
			if (tmpstat == null) {
				this.zk.create(zkBaseMonitor, "".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			}

			tmpstat = this.zk.exists(zkBaseMonitor + "/" + ip, false);
			if (tmpstat == null) {
				this.zk.create(zkBaseMonitor + "/" + ip, "".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			}
		}finally {
			fatherLock.unlock();
		}
	}

	public boolean register(String name) throws InterruptedException {
		final String ip = Sailing.acceptIp.get();
		try {
			createFather();
			String namePath = zkBaseMonitor + "/" + ip + "/" + name.replace('/', '.').substring(zkBase.length() + 1);
			Stat tmpstat = this.zk.exists(namePath, false);
			if (tmpstat != null) {
				Thread.sleep(8000L);
				tmpstat = this.zk.exists(namePath, false);
			}
			if (tmpstat == null) {
				this.zk.create(namePath, "".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
				log.info("thread register successful:" + namePath);
			}
			return true;
		} catch (KeeperException e) {
			e.printStackTrace();
			return false;
		}
	}

	public boolean cancel(String name) throws InterruptedException {
		final String ip = Sailing.acceptIp.get();
		try {
			createFather();
			String namePath = zkBaseMonitor + "/" + ip + "/" + name.replace('/', '.').substring(zkBase.length() + 1);
			Stat tmpstat = this.zk.exists(namePath, false);
			if (tmpstat != null) {
				this.zk.delete(namePath, -1);
			}
			return true;
		} catch (KeeperException e) {
			e.printStackTrace();
			return false;
		}
	}

    /**
     * the zookeeper is first create,if interrunpted, I will retry,
     * if throws the zookeeper exception, I will re new the zkconfig
     * @return
     */
	public boolean handleExpare() throws InterruptedException {
		// add watch
		try {
			log.info("handling expare stat");
			this.LoadingConfig();
			for (String key : Sailing.threadMap.keySet()) {
				if(!this.register(key)){
                    return false;
                }
			}
            return true;
		} catch (KeeperException e) {
            e.printStackTrace();
            return false;
        }
	}

    /**
     * not care succc or not!
     * @param name
     */
	public void heart(String name) {
		final String ip = Sailing.acceptIp.get();
		String namePath = zkBaseMonitor + "/" + ip + "/" + name.replace('/', '.').substring(zkBase.length() + 1);
		try {
			this.zk.setData(namePath, new DateTime().toString().getBytes(), -1);
		} catch (KeeperException e1) {
			e1.printStackTrace();
		} catch (InterruptedException e) {
			// fix bug, keep the inter state
			log.info("when heart I recv a inter!");
			Thread.currentThread().interrupt();
		}
	}
}
