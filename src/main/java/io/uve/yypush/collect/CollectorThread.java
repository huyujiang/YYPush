package io.uve.yypush.collect;

import io.uve.yypush.config.Config;
import io.uve.yypush.zookeeper.ZkMonitorPath;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * 
 * @author yangyang21@staff.weibo.com(yangyang)
 * 
 */
public class CollectorThread implements Runnable {
	private static Logger log = Logger.getLogger(CollectorThread.class);
	private final CountDownLatch countDownLatch = new CountDownLatch(1);
	private Future<?> future = null;

	public void setFuture(Future<?> future) {
		this.future = future;
	}

	private Config config;

	public Config getConfig() {
		return config;
	}

	public void setConfig(Config config) {
		this.config = config;
	}

	public CollectorThread(Config config) {
		this.config = config;
	}

	/**
	 * every thread only been inter once!
	 */
	@Override
	public void run() {
		Collector lc = null;
		try {
			lc = Collector.build(config);
			if (lc == null) {
				log.info("thread collector failed: " + config.name);
			} else {
				log.info("thread collector process success: " + config.name);
				if (ZkMonitorPath.instance.register(config.name)) {
					lc.process();
				}else{
					log.info("register failed and exit:" + config.name);
				}
				lc.close();
			}
		} catch (ExecutionException | IOException e) {
			e.printStackTrace();
		} finally {
			ZkMonitorPath.instance.cancel(config.name);
			countDownLatch.countDown();
		}
	}

	public void stop() {
		try {
			future.cancel(true);
			log.info("send interrupte to thread:" + config.name + ", waiting for stop");
			this.countDownLatch.await();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}
