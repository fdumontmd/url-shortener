package jp.wakatta.client;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.JedisShardInfo;
import redis.clients.jedis.ShardedUniJedis;
import redis.clients.jedis.UniJedis;
import redis.clients.jedis.UniJedisShardInfo;

public class JedisClient {
	private final static Logger LOGGER = LoggerFactory.getLogger(JedisClient.class);
	private final ShardedUniJedis client;

	public JedisClient(String password, Map<String, List<String>> tree) {
		List<UniJedisShardInfo> shards = new ArrayList<UniJedisShardInfo>();
		for (String master: tree.keySet()) {
			LOGGER.info("Parsing master: " + master);
			UniJedis uniJeds = new UniJedis(getHost(master), getPort(master), password);
			List<JedisShardInfo> shardInfos = new ArrayList<JedisShardInfo>();
			for (String slave: tree.get(master)) {
				LOGGER.info("Parsing slave: " + slave);
				shardInfos.add(new JedisShardInfo(getHost(slave), getPort(slave)));
			}
			uniJeds.setShards(shardInfos);
			shards.add(new UniJedisShardInfo(uniJeds));
		}
		this.client = new ShardedUniJedis(shards);
	}

	public ShardedUniJedis getClient() {
		return client;
	}
	
	public static String getHost(String server) {
		return server.substring(0, server.indexOf(':'));
	}
	
	public static int getPort(String server) {
		return Integer.parseInt(server.substring(server.indexOf(':') + 1));
	}
}
