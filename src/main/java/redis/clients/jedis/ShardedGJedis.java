/*
 * Generic version of ShardedJedis
 * © 2012 Frédéric Dumont
 */

package redis.clients.jedis;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import redis.clients.jedis.BinaryClient.LIST_POSITION;
import redis.clients.jedis.BinaryJedisCommands;
import redis.clients.jedis.JedisCommands;
import redis.clients.jedis.SortingParams;
import redis.clients.jedis.Tuple;
import redis.clients.util.Hashing;
import redis.clients.util.ShardInfo;

public class ShardedGJedis<J extends JedisCommands & BinaryJedisCommands, S extends ShardInfo<J>> extends BinaryShardedGJedis<J, S> implements JedisCommands{
	public ShardedGJedis(List<S> shards, Hashing algo, Pattern tagPattern) {
		super(shards, algo, tagPattern);
	}

	public ShardedGJedis(List<S> shards, Hashing algo) {
		super(shards, algo);
	}

	public ShardedGJedis(List<S> shards, Pattern tagPattern) {
		super(shards, tagPattern);
	}

	public ShardedGJedis(List<S> shards) {
		super(shards);
	}

	@Override
	public String set(String key, String value) {
		J jedis = getShard(key);
		return jedis.set(key, value);
	}

	@Override
	public String get(String key) {
		J jedis = getShard(key);
		return jedis.get(key);
	}

	@Override
	public Boolean exists(String key) {
		J jedis = getShard(key);
		return jedis.exists(key);
	}

	@Override
	public String type(String key) {
		J jedis = getShard(key);
		return jedis.type(key);

	}

	@Override
	public Long expire(String key, int seconds) {
		J jedis = getShard(key);
		return jedis.expire(key, seconds);
	}

	@Override
	public Long expireAt(String key, long unixTime) {
		J jedis = getShard(key);
		return jedis.expireAt(key, unixTime);
	}

	@Override
	public Long ttl(String key) {
		J jedis = getShard(key);
		return jedis.ttl(key);
	}

	@Override
	public boolean setbit(String key, long offset, boolean value) {
		J jedis = getShard(key);
		return jedis.setbit(key, offset, value);
	}

	@Override
	public boolean getbit(String key, long offset) {
		J jedis = getShard(key);
		return jedis.getbit(key, offset);
	}

	@Override
	public long setrange(String key, long offset, String value) {
		J jedis = getShard(key);
		return jedis.setrange(key, offset, value);
	}

	@Override
	public String getrange(String key, long startOffset, long endOffset) {
		J jedis = getShard(key);
		return jedis.getrange(key, startOffset, endOffset);
	}

	@Override
	public String getSet(String key, String value) {
		J jedis = getShard(key);
		return jedis.getSet(key, value);
	}

	@Override
	public Long setnx(String key, String value) {
		J jedis = getShard(key);
		return jedis.setnx(key, value);
	}

	@Override
	public String setex(String key, int seconds, String value) {
		J jedis = getShard(key);
		return jedis.setex(key, seconds, value);
	}

	@Override
	public Long decrBy(String key, long integer) {
		J jedis = getShard(key);
		return jedis.decrBy(key, integer);
	}

	@Override
	public Long decr(String key) {
		J jedis = getShard(key);
		return jedis.decr(key);
	}

	@Override
	public Long incrBy(String key, long integer) {
		J jedis = getShard(key);
		return jedis.incrBy(key, integer);
	}

	@Override
	public Long incr(String key) {
		J jedis = getShard(key);
		return jedis.incr(key);
	}

	@Override
	public Long append(String key, String value) {
		J jedis = getShard(key);
		return jedis.append(key, value);
	}

	@Override
	public String substr(String key, int start, int end) {
		J jedis = getShard(key);
		return jedis.substr(key, start, end);
	}

	@Override
	public Long hset(String key, String field, String value) {
		J jedis = getShard(key);
		return jedis.hset(key, field, value);
	}

	@Override
	public String hget(String key, String field) {
		J jedis = getShard(key);
		return jedis.hget(key, field);
	}

	@Override
	public Long hsetnx(String key, String field, String value) {
		J jedis = getShard(key);
		return jedis.hsetnx(key, field, value);
	}

	@Override
	public String hmset(String key, Map<String, String> hash) {
		J jedis = getShard(key);
		return jedis.hmset(key, hash);
	}

	@Override
	public List<String> hmget(String key, String... fields) {
		J jedis = getShard(key);
		return jedis.hmget(key, fields);
	}

	@Override
	public Long hincrBy(String key, String field, long value) {
		J jedis = getShard(key);
		return jedis.hincrBy(key, field, value);
	}

	@Override
	public Boolean hexists(String key, String field) {
		J jedis = getShard(key);
		return jedis.hexists(key, field);
	}

	@Override
	public Long hdel(String key, String field) {
		J jedis = getShard(key);
		return jedis.hdel(key, field);
	}

	@Override
	public Long hlen(String key) {
		J jedis = getShard(key);
		return jedis.hlen(key);
	}

	@Override
	public Set<String> hkeys(String key) {
		J jedis = getShard(key);
		return jedis.hkeys(key);
	}

	@Override
	public List<String> hvals(String key) {
		J jedis = getShard(key);
		return jedis.hvals(key);
	}

	@Override
	public Map<String, String> hgetAll(String key) {
		J jedis = getShard(key);
		return jedis.hgetAll(key);
	}

	@Override
	public Long rpush(String key, String string) {
		J jedis = getShard(key);
		return jedis.rpush(key, string);
	}

	@Override
	public Long lpush(String key, String string) {
		J jedis = getShard(key);
		return jedis.lpush(key, string);
	}

	@Override
	public Long llen(String key) {
		J jedis = getShard(key);
		return jedis.llen(key);
	}

	@Override
	public List<String> lrange(String key, long start, long end) {
		J jedis = getShard(key);
		return jedis.lrange(key, start, end);
	}

	@Override
	public String ltrim(String key, long start, long end) {
		J jedis = getShard(key);
		return jedis.ltrim(key, start, end);
	}

	@Override
	public String lindex(String key, long index) {
		J jedis = getShard(key);
		return jedis.lindex(key, index);
	}

	@Override
	public String lset(String key, long index, String value) {
		J jedis = getShard(key);
		return jedis.lset(key, index, value);
	}

	@Override
	public Long lrem(String key, long count, String value) {
		J jedis = getShard(key);
		return jedis.lrem(key, count, value);
	}

	@Override
	public String lpop(String key) {
		J jedis = getShard(key);
		return jedis.lpop(key);
	}

	@Override
	public String rpop(String key) {
		J jedis = getShard(key);
		return jedis.rpop(key);
	}

	@Override
	public Long sadd(String key, String member) {
		J jedis = getShard(key);
		return jedis.sadd(key, member);
	}

	@Override
	public Set<String> smembers(String key) {
		J jedis = getShard(key);
		return jedis.smembers(key);
	}

	@Override
	public Long srem(String key, String member) {
		J jedis = getShard(key);
		return jedis.srem(key, member);
	}

	@Override
	public String spop(String key) {
		J jedis = getShard(key);
		return jedis.spop(key);
	}

	@Override
	public Long scard(String key) {
		J jedis = getShard(key);
		return jedis.scard(key);
	}

	@Override
	public Boolean sismember(String key, String member) {
		J jedis = getShard(key);
		return jedis.sismember(key, member);
	}

	@Override
	public String srandmember(String key) {
		J jedis = getShard(key);
		return jedis.srandmember(key);
	}

	@Override
	public Long zadd(String key, double score, String member) {
		J jedis = getShard(key);
		return jedis.zadd(key, score, member);
	}

	@Override
	public Set<String> zrange(String key, int start, int end) {
		J jedis = getShard(key);
		return jedis.zrange(key, start, end);
	}

	@Override
	public Long zrem(String key, String member) {
		J jedis = getShard(key);
		return jedis.zrem(key, member);
	}

	@Override
	public Double zincrby(String key, double score, String member) {
		J jedis = getShard(key);
		return jedis.zincrby(key, score, member);
	}

	@Override
	public Long zrank(String key, String member) {
		J jedis = getShard(key);
		return jedis.zrank(key, member);
	}

	@Override
	public Long zrevrank(String key, String member) {
		J jedis = getShard(key);
		return jedis.zrevrank(key, member);
	}

	@Override
	public Set<String> zrevrange(String key, int start, int end) {
		J jedis = getShard(key);
		return jedis.zrevrange(key, start, end);
	}

	@Override
	public Set<Tuple> zrangeWithScores(String key, int start, int end) {
		J jedis = getShard(key);
		return jedis.zrangeWithScores(key, start, end);
	}

	@Override
	public Set<Tuple> zrevrangeWithScores(String key, int start, int end) {
		J jedis = getShard(key);
		return jedis.zrevrangeWithScores(key, start, end);
	}

	@Override
	public Long zcard(String key) {
		J jedis = getShard(key);
		return jedis.zcard(key);
	}

	@Override
	public Double zscore(String key, String member) {
		J jedis = getShard(key);
		return jedis.zscore(key, member);
	}

	@Override
	public List<String> sort(String key) {
		J jedis = getShard(key);
		return jedis.sort(key);
	}

	@Override
	public List<String> sort(String key, SortingParams sortingParameters) {
		J jedis = getShard(key);
		return jedis.sort(key, sortingParameters);
	}

	@Override
	public Long zcount(String key, double min, double max) {
		J jedis = getShard(key);
		return jedis.zcount(key, min, max);
	}

	@Override
	public Set<String> zrangeByScore(String key, double min, double max) {
		J jedis = getShard(key);
		return jedis.zrangeByScore(key, min, max);
	}

	@Override
	public Set<String> zrevrangeByScore(String key, double max, double min) {
		J jedis = getShard(key);
		return jedis.zrevrangeByScore(key, max, min);
	}

	@Override
	public Set<String> zrangeByScore(String key, double min, double max,
			int offset, int count) {
		J jedis = getShard(key);
		return jedis.zrangeByScore(key, min, max, offset, count);
	}

	@Override
	public Set<String> zrevrangeByScore(String key, double max, double min,
			int offset, int count) {
		J jedis = getShard(key);
		return jedis.zrevrangeByScore(key, max, min, offset, count);
	}

	@Override
	public Set<Tuple> zrangeByScoreWithScores(String key, double min, double max) {
		J jedis = getShard(key);
		return jedis.zrangeByScoreWithScores(key, min, max);
	}

	@Override
	public Set<Tuple> zrevrangeByScoreWithScores(String key, double max,
			double min) {
		J jedis = getShard(key);
		return jedis.zrangeByScoreWithScores(key, min, max);
	}

	@Override
	public Set<Tuple> zrangeByScoreWithScores(String key, double min,
			double max, int offset, int count) {
		J jedis = getShard(key);
		return jedis.zrangeByScoreWithScores(key, min, max, offset, count);
	}

	@Override
	public Set<Tuple> zrevrangeByScoreWithScores(String key, double max,
			double min, int offset, int count) {
		J jedis = getShard(key);
		return jedis.zrevrangeByScoreWithScores(key, max, min, offset, count);
	}

	@Override
	public Long zremrangeByRank(String key, int start, int end) {
		J jedis = getShard(key);
		return jedis.zremrangeByRank(key, start, start);
	}

	@Override
	public Long zremrangeByScore(String key, double start, double end) {
		J jedis = getShard(key);
		return jedis.zremrangeByScore(key, start, end);
	}

	@Override
	public Long linsert(String key, LIST_POSITION where, String pivot,
			String value) {
		J jedis = getShard(key);		
		return jedis.linsert(key, where, pivot, value);
	}
}
