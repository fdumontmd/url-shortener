/*
 * Generic version of BinaryShardedJedis
 * © 2012 Frédéric Dumont
 */

package redis.clients.jedis;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import redis.clients.jedis.BinaryClient.LIST_POSITION;
import redis.clients.jedis.BinaryJedisCommands;
import redis.clients.jedis.SortingParams;
import redis.clients.jedis.Tuple;
import redis.clients.util.Hashing;
import redis.clients.util.ShardInfo;
import redis.clients.util.Sharded;

public class BinaryShardedGJedis<J extends BinaryJedisCommands, S extends ShardInfo<J>> extends Sharded<J, S> implements BinaryJedisCommands {

	public BinaryShardedGJedis(List<S> shards, Hashing algo,
			Pattern tagPattern) {
		super(shards, algo, tagPattern);
	}

	public BinaryShardedGJedis(List<S> shards, Hashing algo) {
		super(shards, algo);
	}

	public BinaryShardedGJedis(List<S> shards,
			Pattern tagPattern) {
		super(shards, tagPattern);
	}

	public BinaryShardedGJedis(List<S> shards) {
		super(shards);
	}
	
	@Override
	public String set(byte[] key, byte[] value) {
		J jedis = getShard(key);
		return jedis.set(key, value);
	}

	@Override
	public byte[] get(byte[] key) {
		J jedis = getShard(key);
		return jedis.get(key);
	}

	@Override
	public Boolean exists(byte[] key) {
		J jedis = getShard(key);
		return jedis.exists(key);
	}

	@Override
	public String type(byte[] key) {
		J jedis = getShard(key);
		return jedis.type(key);
	}

	@Override
	public Long expire(byte[] key, int seconds) {
		J jedis = getShard(key);
		return jedis.expire(key, seconds);
	}

	@Override
	public Long expireAt(byte[] key, long unixTime) {
		J jedis = getShard(key);
		return jedis.expireAt(key, unixTime);
	}

	@Override
	public Long ttl(byte[] key) {
		J jedis = getShard(key);
		return jedis.ttl(key);
	}

	@Override
	public byte[] getSet(byte[] key, byte[] value) {
		J jedis = getShard(key);
		return jedis.getSet(key, value);
	}

	@Override
	public Long setnx(byte[] key, byte[] value) {
		J jedis = getShard(key);
		return jedis.setnx(key, value);
	}

	@Override
	public String setex(byte[] key, int seconds, byte[] value) {
		J jedis = getShard(key);
		return jedis.setex(key, seconds, value);
	}

	@Override
	public Long decrBy(byte[] key, long integer) {
		J jedis = getShard(key);
		return jedis.decrBy(key, integer);
	}

	@Override
	public Long decr(byte[] key) {
		J jedis = getShard(key);
		return jedis.decr(key);
	}

	@Override
	public Long incrBy(byte[] key, long integer) {
		J jedis = getShard(key);
		return jedis.incrBy(key, integer);
	}

	@Override
	public Long incr(byte[] key) {
		J jedis = getShard(key);
		return jedis.incr(key);
	}

	@Override
	public Long append(byte[] key, byte[] value) {
		J jedis = getShard(key);
		return jedis.append(key, value);
	}

	@Override
	public byte[] substr(byte[] key, int start, int end) {
		J jedis = getShard(key);
		return jedis.substr(key, start, end);
	}

	@Override
	public Long hset(byte[] key, byte[] field, byte[] value) {
		J jedis = getShard(key);
		return jedis.hset(key, field, value);
	}

	@Override
	public byte[] hget(byte[] key, byte[] field) {
		J jedis = getShard(key);
		return jedis.hget(key, field);
	}

	@Override
	public Long hsetnx(byte[] key, byte[] field, byte[] value) {
		J jedis = getShard(key);
		return jedis.hsetnx(key, field, value);
	}

	@Override
	public String hmset(byte[] key, Map<byte[], byte[]> hash) {
		J jedis = getShard(key);
		return jedis.hmset(key, hash);
	}

	@Override
	public List<byte[]> hmget(byte[] key, byte[]... fields) {
		J jedis = getShard(key);
		return jedis.hmget(key, fields);
	}

	@Override
	public Long hincrBy(byte[] key, byte[] field, long value) {
		J jedis = getShard(key);
		return jedis.hincrBy(key, field, value);
	}

	@Override
	public Boolean hexists(byte[] key, byte[] field) {
		J jedis = getShard(key);
		return jedis.hexists(key, field);
	}

	@Override
	public Long hdel(byte[] key, byte[] field) {
		J jedis = getShard(key);
		return jedis.hdel(key, field);
	}

	@Override
	public Long hlen(byte[] key) {
		J jedis = getShard(key);
		return jedis.hlen(key);
	}

	@Override
	public Set<byte[]> hkeys(byte[] key) {
		J jedis = getShard(key);
		return jedis.hkeys(key);
	}

	@Override
	public Collection<byte[]> hvals(byte[] key) {
		J jedis = getShard(key);
		return jedis.hvals(key);
	}

	@Override
	public Map<byte[], byte[]> hgetAll(byte[] key) {
		J jedis = getShard(key);
		return jedis.hgetAll(key);
	}

	@Override
	public Long rpush(byte[] key, byte[] string) {
		J jedis = getShard(key);
		return jedis.rpush(key, string);
	}

	@Override
	public Long lpush(byte[] key, byte[] string) {
		J jedis = getShard(key);
		return jedis.lpush(key, string);
	}

	@Override
	public Long llen(byte[] key) {
		J jedis = getShard(key);
		return jedis.llen(key);
	}

	@Override
	public List<byte[]> lrange(byte[] key, int start, int end) {
		J jedis = getShard(key);
		return jedis.lrange(key, start, end);
	}

	@Override
	public String ltrim(byte[] key, int start, int end) {
		J jedis = getShard(key);
		return jedis.ltrim(key, start, end);
	}

	@Override
	public byte[] lindex(byte[] key, int index) {
		J jedis = getShard(key);
		return jedis.lindex(key, index);
	}

	@Override
	public String lset(byte[] key, int index, byte[] value) {
		J jedis = getShard(key);
		return jedis.lset(key, index, value);
	}

	@Override
	public Long lrem(byte[] key, int count, byte[] value) {
		J jedis = getShard(key);
		return jedis.lrem(key, count, value);
	}

	@Override
	public byte[] lpop(byte[] key) {
		J jedis = getShard(key);
		return jedis.lpop(key);
	}

	@Override
	public byte[] rpop(byte[] key) {
		J jedis = getShard(key);
		return jedis.rpop(key);
	}

	@Override
	public Long sadd(byte[] key, byte[] member) {
		J jedis = getShard(key);
		return jedis.sadd(key, member);
	}

	@Override
	public Set<byte[]> smembers(byte[] key) {
		J jedis = getShard(key);
		return jedis.smembers(key);
	}

	@Override
	public Long srem(byte[] key, byte[] member) {
		J jedis = getShard(key);
		return jedis.srem(key, member);
	}

	@Override
	public byte[] spop(byte[] key) {
		J jedis = getShard(key);
		return jedis.spop(key);
	}

	@Override
	public Long scard(byte[] key) {
		J jedis = getShard(key);
		return jedis.scard(key);
	}

	@Override
	public Boolean sismember(byte[] key, byte[] member) {
		J jedis = getShard(key);
		return jedis.sismember(key, member);
	}

	@Override
	public byte[] srandmember(byte[] key) {
		J jedis = getShard(key);
		return jedis.srandmember(key);
	}

	@Override
	public Long zadd(byte[] key, double score, byte[] member) {
		J jedis = getShard(key);
		return jedis.zadd(key, score, member);
	}

	@Override
	public Set<byte[]> zrange(byte[] key, int start, int end) {
		J jedis = getShard(key);
		return jedis.zrange(key, start, end);
	}

	@Override
	public Long zrem(byte[] key, byte[] member) {
		J jedis = getShard(key);
		return jedis.zrem(key, member);
	}

	@Override
	public Double zincrby(byte[] key, double score, byte[] member) {
		J jedis = getShard(key);
		return jedis.zincrby(key, score, member);
	}

	@Override
	public Long zrank(byte[] key, byte[] member) {
		J jedis = getShard(key);
		return jedis.zrank(key, member);
	}

	@Override
	public Long zrevrank(byte[] key, byte[] member) {
		J jedis = getShard(key);
		return jedis.zrevrank(key, member);
	}

	@Override
	public Set<byte[]> zrevrange(byte[] key, int start, int end) {
		J jedis = getShard(key);
		return jedis.zrevrange(key, start, end);
	}

	@Override
	public Set<Tuple> zrangeWithScores(byte[] key, int start, int end) {
		J jedis = getShard(key);
		return jedis.zrangeWithScores(key, start, end);
	}

	@Override
	public Set<Tuple> zrevrangeWithScores(byte[] key, int start, int end) {
		J jedis = getShard(key);
		return jedis.zrevrangeWithScores(key, start, end);
	}

	@Override
	public Long zcard(byte[] key) {
		J jedis = getShard(key);
		return jedis.zcard(key);
	}

	@Override
	public Double zscore(byte[] key, byte[] member) {
		J jedis = getShard(key);
		return jedis.zscore(key, member);
	}

	@Override
	public List<byte[]> sort(byte[] key) {
		J jedis = getShard(key);
		return jedis.sort(key);
	}

	@Override
	public List<byte[]> sort(byte[] key, SortingParams sortingParameters) {
		J jedis = getShard(key);
		return jedis.sort(key, sortingParameters);
	}

	@Override
	public Long zcount(byte[] key, double min, double max) {
		J jedis = getShard(key);
		return jedis.zcount(key, min, max);
	}

	@Override
	public Set<byte[]> zrangeByScore(byte[] key, double min, double max) {
		J jedis = getShard(key);
		return jedis.zrangeByScore(key, min, max);
	}

	@Override
	public Set<byte[]> zrangeByScore(byte[] key, double min, double max,
			int offset, int count) {
		J jedis = getShard(key);
		return jedis.zrangeByScore(key, min, max, offset, count);
	}


	@Override
	public Set<Tuple> zrangeByScoreWithScores(byte[] key, double min, double max) {
		J jedis = getShard(key);
		return jedis.zrangeByScoreWithScores(key, min, max);
	}

	@Override
	public Set<Tuple> zrangeByScoreWithScores(byte[] key, double min,
			double max, int offset, int count) {
		J jedis = getShard(key);
		return jedis.zrangeByScoreWithScores(key, min, max, offset, count);
	}

	@Override
	public Set<byte[]> zrevrangeByScore(byte[] key, double max, double min) {
		J jedis = getShard(key);
		return jedis.zrevrangeByScore(key, min, max);
	}

	@Override
	public Set<byte[]> zrevrangeByScore(byte[] key, double max, double min,
			int offset, int count) {
		J jedis = getShard(key);
		return jedis.zrevrangeByScore(key, min, max, offset, count);
	}

	@Override
	public Set<Tuple> zrevrangeByScoreWithScores(byte[] key, double max,
			double min) {
		J jedis = getShard(key);
		return jedis.zrevrangeByScoreWithScores(key, max, min);
	}

	@Override
	public Set<Tuple> zrevrangeByScoreWithScores(byte[] key, double max,
			double min, int offset, int count) {
		J jedis = getShard(key);
		return jedis.zrevrangeByScoreWithScores(key, max, min, offset, count);
	}

	@Override
	public Long zremrangeByRank(byte[] key, int start, int end) {
		J jedis = getShard(key);
		return jedis.zremrangeByRank(key, start, end);
	}

	@Override
	public Long zremrangeByScore(byte[] key, double start, double end) {
		J jedis = getShard(key);
		return jedis.zremrangeByScore(key, start, end);
	}

	@Override
	public Long linsert(byte[] key, LIST_POSITION where, byte[] pivot,
			byte[] value) {
		J jedis = getShard(key);
		return jedis.linsert(key, where, pivot, value);
	}
}
