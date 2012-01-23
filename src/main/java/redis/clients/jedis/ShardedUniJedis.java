package redis.clients.jedis;

import java.io.IOException;
import java.util.List;
import java.util.regex.Pattern;

import redis.clients.util.Hashing;

public class ShardedUniJedis extends ShardedGJedis<UniJedis, UniJedisShardInfo> {

	public ShardedUniJedis(List<UniJedisShardInfo> shards, Hashing algo,
			Pattern tagPattern) {
		super(shards, algo, tagPattern);
	}

	public ShardedUniJedis(List<UniJedisShardInfo> shards, Hashing algo) {
		super(shards, algo);
		// TODO Auto-generated constructor stub
	}

	public ShardedUniJedis(List<UniJedisShardInfo> shards, Pattern tagPattern) {
		super(shards, tagPattern);
		// TODO Auto-generated constructor stub
	}

	public ShardedUniJedis(List<UniJedisShardInfo> shards) {
		super(shards);
		// TODO Auto-generated constructor stub
	}
	
    public void disconnect() throws IOException {
        for (UniJedis jedis : getAllShards()) {
            jedis.disconnect();
        }
    }
    
    public void flushAll() throws IOException {
    	for (UniJedis jedis: getAllShards())
    		jedis.flushAll();
    }
}
