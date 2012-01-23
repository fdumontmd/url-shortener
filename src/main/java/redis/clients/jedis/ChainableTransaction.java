/*
 * derived from http://groups.google.com/group/jedis_redis/msg/c8c76371cf543e36
 * Initial implementation by Ingvar Bogdahn
 */
package redis.clients.jedis;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import redis.clients.jedis.BinaryClient.LIST_POSITION;
import redis.clients.util.Pool;

public class ChainableTransaction {
    protected Pool<Jedis> pool = null;
    protected Client client = null;
    protected Jedis jedis = null;
    protected boolean inTransaction = true;

    public ChainableTransaction(final Client client) {
        this.client = client;
    }

    public ChainableTransaction() {
    }

    public ChainableTransaction(Pool<Jedis> writePool, Client c, Jedis j) {
        this.pool = writePool;
        this.client = c;
        this.jedis = j;
    }

    public List<Object> exec() {
        client.exec();
        client.getAll(1); // Discard all but the last reply

        List<Object> unformatted = client.getObjectMultiBulkReply();
        if(this.jedis != null && this.pool != null)
            pool.returnResource(jedis);

        return cutOk(unformatted);
    }

    private boolean baeq(byte [] left, byte [] right)
	{
		if (left == right)
			return true;
		else if (left == null || right == null || left.length != right.length)
			return false;
		for (int i = 0; i < left.length; i++)
			if (left[i] != right[i])
				return false;
		return true;
	}

    private List<Object> cutOk(List<Object> unformatted) {
        List<Object> result = new ArrayList<Object>(unformatted.size());
        byte[] ok =  {79, 75};
        byte[] ba = null;
        for(Object o: unformatted){
            try { ba = ((byte[])o); } catch (ClassCastException cce) {result.add(o);}
            if(baeq(ba, ok)==false)    result.add(o);
        }
        return result;
    }

    public String discard() {
        client.discard();
        client.getAll(1); // Discard all but the last reply
        inTransaction = false;
        return client.getStatusCodeReply();
    }

    public ChainableTransaction  append(byte[] key, byte[] value) {
        client.append(key, value);
        return this;
    }

    public ChainableTransaction  blpop(byte[]... args) {
        client.blpop(args);
        return this;
    }

    public ChainableTransaction  brpop(byte[]... args) {
        client.brpop(args);
        return this;
    }

    public ChainableTransaction  decr(byte[] key) {
        client.decr(key);
        return this;
    }

    public ChainableTransaction  decrBy(byte[] key, long integer) {
        client.decrBy(key, integer);
        return this;
    }

    public ChainableTransaction  del(byte[]... keys) {
        client.del(keys);
        return this;
    }

    public ChainableTransaction  echo(byte[] string) {
        client.echo(string);
        return this;
    }

    public ChainableTransaction  exists(byte[] key) {
        client.exists(key);
        return this;
    }

    public ChainableTransaction  expire(byte[] key, int seconds) {
        client.expire(key, seconds);
        return this;
    }

    public ChainableTransaction  expireAt(byte[] key, long unixTime) {
        client.expireAt(key, unixTime);
        return this;
    }

    public ChainableTransaction  get(byte[] key) {
        client.get(key);
        return this;
    }

    public ChainableTransaction  getSet(byte[] key, byte[] value) {
        client.getSet(key, value);
        return this;
    }

    public ChainableTransaction  hdel(byte[] key, byte[] field) {
        client.hdel(key, field);
        return this;
    }

    public ChainableTransaction  hexists(byte[] key, byte[] field) {
        client.hexists(key, field);
        return this;
    }

    public ChainableTransaction  hget(byte[] key, byte[] field) {
        client.hget(key, field);
        return this;
    }

    public ChainableTransaction  hgetAll(byte[] key) {
        client.hgetAll(key);
        return this;
    }

    public ChainableTransaction  hincrBy(byte[] key, byte[] field, long value) {
        client.hincrBy(key, field, value);
        return this;
    }

    public ChainableTransaction  hkeys(byte[] key) {
        client.hkeys(key);
        return this;
    }

    public ChainableTransaction  hlen(byte[] key) {
        client.hlen(key);
        return this;
    }

    public ChainableTransaction  hmget(byte[] key, byte[]... fields) {
        client.hmget(key, fields);
        return this;
    }

    public ChainableTransaction  hmset(byte[] key, Map<byte[], byte[]> hash) {
        client.hmset(key, hash);
        return this;
    }

    public ChainableTransaction  hset(byte[] key, byte[] field, byte[] value) {
        client.hset(key, field, value);
        return this;
    }

    public ChainableTransaction  hsetnx(byte[] key, byte[] field, byte[] value) {
        client.hsetnx(key, field, value);
        return this;
    }

    public ChainableTransaction  hvals(byte[] key) {
        client.hvals(key);
        return this;
    }

    public ChainableTransaction  incr(byte[] key) {
        client.incr(key);
        return this;
    }

    public ChainableTransaction  incrBy(byte[] key, long integer) {
        client.incrBy(key, integer);
        return this;
    }

    public ChainableTransaction  keys(byte[] pattern) {
        client.keys(pattern);
        return this;
    }

    public ChainableTransaction  lindex(byte[] key, long index) {
        client.lindex(key, index);
        return this;
    }

    public ChainableTransaction  linsert(byte[] key, LIST_POSITION where,
            byte[] pivot, byte[] value) {
        client.linsert(key, where, pivot, value);
        return this;
    }

    public ChainableTransaction  llen(byte[] key) {
        client.llen(key);
        return this;
    }

    public ChainableTransaction  lpop(byte[] key) {
        client.lpop(key);
        return this;
    }

    public ChainableTransaction  lpush(byte[] key, byte[] string) {
        client.lpush(key, string);
        return this;
    }

    public ChainableTransaction  lpushx(byte[] key, byte[] bytes) {
        client.lpushx(key, bytes);
        return this;
    }

    public ChainableTransaction  lrange(byte[] key, long start, long end) {
        client.lrange(key, start, end);
        return this;
    }

    public ChainableTransaction  lrem(byte[] key, long count, byte[] value) {
        client.lrem(key, count, value);
        return this;
    }

    public ChainableTransaction  lset(byte[] key, long index, byte[] value) {
        client.lset(key, index, value);
        return this;
    }

    public ChainableTransaction  ltrim(byte[] key, long start, long end) {
        client.ltrim(key, start, end);
        return this;
    }

    public ChainableTransaction  mget(byte[]... keys) {
        client.mget(keys);
        return this;
    }

    public ChainableTransaction  move(byte[] key, int dbIndex) {
        client.move(key, dbIndex);
        return this;
    }

    public ChainableTransaction  mset(byte[]... keysvalues) {
        client.mset(keysvalues);
        return this;
    }

    public ChainableTransaction  msetnx(byte[]... keysvalues) {
        client.msetnx(keysvalues);
        return this;
    }

    public ChainableTransaction  persist(byte[] key) {
        client.persist(key);
        return this;
    }

    public ChainableTransaction  rename(byte[] oldkey, byte[] newkey) {
        client.rename(oldkey, newkey);
        return this;
    }

    public ChainableTransaction  renamenx(byte[] oldkey, byte[] newkey) {
        client.renamenx(oldkey, newkey);
        return this;
    }

    public ChainableTransaction  rpop(byte[] key) {
        client.rpop(key);
        return this;
    }

    public ChainableTransaction  rpoplpush(byte[] srckey, byte[] dstkey) {
        client.rpoplpush(srckey, dstkey);
        return this;
    }

    public ChainableTransaction  rpush(byte[] key, byte[] string) {
        client.rpush(key, string);
        return this;
    }

    public ChainableTransaction  rpushx(byte[] key, byte[] string) {
        client.rpushx(key, string);
        return this;
    }

    public ChainableTransaction  sadd(byte[] key, byte[] member) {
        client.sadd(key, member);
        return this;
    }

    public ChainableTransaction  scard(byte[] key) {
        client.scard(key);
        return this;
    }

    public ChainableTransaction  sdiff(byte[]... keys) {
        client.sdiff(keys);
        return this;
    }

    public ChainableTransaction  sdiffstore(byte[] dstkey, byte[]... keys) {
        client.sdiffstore(dstkey, keys);
        return this;
    }

    public ChainableTransaction  set(byte[] key, byte[] value) {
        client.set(key, value);
        return this;
    }

    public ChainableTransaction  setex(byte[] key, int seconds, byte[] value) {
        client.setex(key, seconds, value);
        return this;
    }

    public ChainableTransaction  setnx(byte[] key, byte[] value) {
        client.setnx(key, value);
        return this;
    }

    public ChainableTransaction  sinter(byte[]... keys) {
        client.sinter(keys);
        return this;
    }

    public ChainableTransaction  sinterstore(byte[] dstkey, byte[]... keys) {
        client.sinterstore(dstkey, keys);
        return this;
    }

    public ChainableTransaction  sismember(byte[] key, byte[] member) {
        client.sismember(key, member);
        return this;
    }

    public ChainableTransaction  smembers(byte[] key) {
        client.smembers(key);
        return this;
    }

    public ChainableTransaction  smove(byte[] srckey, byte[] dstkey, byte[] member) {
        client.smove(srckey, dstkey, member);
        return this;
    }

    public ChainableTransaction  sort(byte[] key) {
        client.sort(key);
        return this;
    }

    public ChainableTransaction  sort(byte[] key,
            SortingParams sortingParameters) {
        client.sort(key, sortingParameters);
        return this;
    }

    public ChainableTransaction  sort(byte[] key,
            SortingParams sortingParameters, byte[] dstkey) {
        client.sort(key, sortingParameters, dstkey);
        return this;
    }

    public ChainableTransaction  sort(byte[] key, byte[] dstkey) {
        client.sort(key, dstkey);
        return this;
    }

    public ChainableTransaction  spop(byte[] key) {
        client.spop(key);
        return this;
    }

    public ChainableTransaction  srandmember(byte[] key) {
        client.srandmember(key);
        return this;
    }

    public ChainableTransaction  srem(byte[] key, byte[] member) {
        client.srem(key, member);
        return this;
    }

    public ChainableTransaction  strlen(byte[] key) {
        client.strlen(key);
        return this;
    }

    public ChainableTransaction  substr(byte[] key, int start, int end) { // what's
        // that?
        client.substr(key, start, end);
        return this;
    }

    public ChainableTransaction  sunion(byte[]... keys) {
        client.sunion(keys);
        return this;
    }

    public ChainableTransaction  sunionstore(byte[] dstkey, byte[]... keys) {
        client.sunionstore(dstkey, keys);
        return this;
    }

    public ChainableTransaction  ttl(byte[] key) {
        client.ttl(key);
        return this;
    }

    public ChainableTransaction  type(byte[] key) {
        client.type(key);
        return this;
    }

    public ChainableTransaction  zadd(byte[] key, double score, byte[] member) {
        client.zadd(key, score, member);
        return this;
    }

    public ChainableTransaction  zcard(byte[] key) {
        client.zcard(key);
        return this;
    }

    public ChainableTransaction  zcount(byte[] key, double min, double max) {
        client.zcount(key, min, max);
        return this;
    }

    public ChainableTransaction  zincrby(byte[] key, double score, byte[] member) {
        client.zincrby(key, score, member);
        return this;
    }

    public ChainableTransaction  zinterstore(byte[] dstkey, byte[]... sets) {
        client.zinterstore(dstkey, sets);
        return this;
    }

    public ChainableTransaction  zinterstore(byte[] dstkey, ZParams params,
            byte[]... sets) {
        client.zinterstore(dstkey, params, sets);
        return this;
    }

    public ChainableTransaction  zrange(byte[] key, int start, int end) {
        client.zrange(key, start, end);
        return this;
    }

    public ChainableTransaction  zrangeByScore(byte[] key, double min,
            double max) {
        client.zrangeByScore(key, min, max);
        return this;
    }

    public ChainableTransaction  zrangeByScore(byte[] key, byte[] min,
            byte[] max) {
        client.zrangeByScore(key, min, max);
        return this;
    }

    public ChainableTransaction  zrangeByScore(byte[] key, double min,
            double max, int offset, int count) {
        client.zrangeByScore(key, min, max, offset, count);
        return this;
    }

    public ChainableTransaction  zrangeByScoreWithScores(byte[] key, double min,
            double max) {
        client.zrangeByScoreWithScores(key, min, max);
        return this;
    }

    public ChainableTransaction  zrangeByScoreWithScores(byte[] key, double min,
            double max, int offset, int count) {
        client.zrangeByScoreWithScores(key, min, max, offset, count);
        return this;
    }

    public ChainableTransaction  zrangeWithScores(byte[] key, int start, int end) {
        client.zrangeWithScores(key, start, end);
        return this;
    }

    public ChainableTransaction  zrank(byte[] key, byte[] member) {
        client.zrank(key, member);
        return this;
    }

    public ChainableTransaction  zrem(byte[] key, byte[] member) {
        client.zrem(key, member);
        return this;
    }

    public ChainableTransaction  zremrangeByRank(byte[] key, int start, int end) {
        client.zremrangeByRank(key, start, end);
        return this;
    }

    public ChainableTransaction  zremrangeByScore(byte[] key, double start, double end) {
        client.zremrangeByScore(key, start, end);
        return this;
    }

    public ChainableTransaction  zrevrange(byte[] key, int start, int end) {
        client.zrevrange(key, start, end);
        return this;
    }

    public ChainableTransaction  zrevrangeWithScores(byte[] key, int start,
            int end) {
        client.zrevrangeWithScores(key, start, end);
        return this;
    }

    public ChainableTransaction  zrevrank(byte[] key, byte[] member) {
        client.zrevrank(key, member);
        return this;
    }

    public ChainableTransaction  zscore(byte[] key, byte[] member) {
        client.zscore(key, member);
        return this;
    }

    public ChainableTransaction  zunionstore(byte[] dstkey, byte[]... sets) {
        client.zunionstore(dstkey, sets);
        return this;
    }

    public ChainableTransaction  zunionstore(byte[] dstkey, ZParams params,
            byte[]... sets) {
        client.zunionstore(dstkey, params, sets);
        return this;
    }

    public ChainableTransaction  brpoplpush(byte[] source, byte[] destination,
            int timeout) {
        client.brpoplpush(source, destination, timeout);
        return this;
    }
    

    public ChainableTransaction append(String key, String value) {
        client.append(key, value);
        return this;
    }

    public ChainableTransaction blpop(String... args) {
        client.blpop(args);
        return this;
    }

    public ChainableTransaction brpop(String... args) {
        client.brpop(args);
        return this;
    }

    public ChainableTransaction decr(String key) {
        client.decr(key);
        return this;
    }

    public ChainableTransaction decrBy(String key, long integer) {
        client.decrBy(key, integer);
        return this;
    }

    public ChainableTransaction del(String... keys) {
        client.del(keys);
        return this;
    }

    public ChainableTransaction echo(String string) {
        client.echo(string);
        return this;
    }

    public ChainableTransaction exists(String key) {
        client.exists(key);
        return this;
    }

    public ChainableTransaction expire(String key, int seconds) {
        client.expire(key, seconds);
        return this;
    }

    public ChainableTransaction expireAt(String key, long unixTime) {
        client.expireAt(key, unixTime);
        return this;
    }

    public ChainableTransaction get(String key) {
        client.get(key);
        return this;
    }

    public ChainableTransaction getbit(String key, long offset) {
        client.getbit(key, offset);
        return this;
    }

    public ChainableTransaction getrange(String key, long startOffset,
            long endOffset) {
        client.getrange(key, startOffset, endOffset);
        return this;
    }

    public ChainableTransaction getSet(String key, String value) {
        client.getSet(key, value);
        return this;
    }

    public ChainableTransaction hdel(String key, String field) {
        client.hdel(key, field);
        return this;
    }

    public ChainableTransaction hexists(String key, String field) {
        client.hexists(key, field);
        return this;
    }

    public ChainableTransaction hget(String key, String field) {
        client.hget(key, field);
        return this;
    }

    public ChainableTransaction hgetAll(String key) {
        client.hgetAll(key);
        return this;
    }

    public ChainableTransaction hincrBy(String key, String field, long value) {
        client.hincrBy(key, field, value);
        return this;
    }

    public ChainableTransaction hkeys(String key) {
        client.hkeys(key);
        return this;
    }

    public ChainableTransaction hlen(String key) {
        client.hlen(key);
        return this;
    }

    public ChainableTransaction hmget(String key, String... fields) {
        client.hmget(key, fields);
        return this;
    }

    public ChainableTransaction hmset(String key, Map<String, String> hash) {
        client.hmset(key, hash);
        return this;
    }

    public ChainableTransaction hset(String key, String field, String value) {
        client.hset(key, field, value);
        return this;
    }

    public ChainableTransaction hsetnx(String key, String field, String value) {
        client.hsetnx(key, field, value);
        return this;
    }

    public ChainableTransaction hvals(String key) {
        client.hvals(key);
        return this;
    }

    public ChainableTransaction incr(String key) {
        client.incr(key);
        return this;
    }

    public ChainableTransaction incrBy(String key, long integer) {
        client.incrBy(key, integer);
        return this;
    }

    public ChainableTransaction keys(String pattern) {
        client.keys(pattern);
        return this;
    }

    public ChainableTransaction lindex(String key, int index) {
        client.lindex(key, index);
        return this;
    }

    public ChainableTransaction linsert(String key, LIST_POSITION where,
            String pivot, String value) {
        client.linsert(key, where, pivot, value);
        return this;
    }

    public ChainableTransaction llen(String key) {
        client.llen(key);
        return this;
    }

    public ChainableTransaction lpop(String key) {
        client.lpop(key);
        return this;
    }

    public ChainableTransaction lpush(String key, String string) {
        client.lpush(key, string);
        return this;
    }

    public ChainableTransaction lpushx(String key, String string) {
        client.lpushx(key, string);
        return this;
    }

    public ChainableTransaction lrange(String key, long start, long end) {
        client.lrange(key, start, end);
        return this;
    }

    public ChainableTransaction lrem(String key, long count, String value) {
        client.lrem(key, count, value);
        return this;
    }

    public ChainableTransaction lset(String key, long index, String value) {
        client.lset(key, index, value);
        return this;
    }

    public ChainableTransaction ltrim(String key, long start, long end) {
        client.ltrim(key, start, end);
        return this;
    }

    public ChainableTransaction mget(String... keys) {
        client.mget(keys);
        return this;
    }

    public ChainableTransaction move(String key, int dbIndex) {
        client.move(key, dbIndex);
        return this;
    }

    public ChainableTransaction mset(String... keysvalues) {
        client.mset(keysvalues);
        return this;
    }

    public ChainableTransaction msetnx(String... keysvalues) {
        client.msetnx(keysvalues);
        return this;
    }

    public ChainableTransaction persist(String key) {
        client.persist(key);
        return this;
    }

    public ChainableTransaction rename(String oldkey, String newkey) {
        client.rename(oldkey, newkey);
        return this;
    }

    public ChainableTransaction renamenx(String oldkey, String newkey) {
        client.renamenx(oldkey, newkey);
        return this;
    }

    public ChainableTransaction rpop(String key) {
        client.rpop(key);
        return this;
    }

    public ChainableTransaction rpoplpush(String srckey, String dstkey) {
        client.rpoplpush(srckey, dstkey);
        return this;
    }

    public ChainableTransaction rpush(String key, String string) {
        client.rpush(key, string);
        return this;
    }

    public ChainableTransaction rpushx(String key, String string) {
        client.rpushx(key, string);
        return this;
    }

    public ChainableTransaction sadd(String key, String member) {
        client.sadd(key, member);
        return this;
    }

    public ChainableTransaction scard(String key) {
        client.scard(key);
        return this;
    }

    public ChainableTransaction sdiff(String... keys) {
        client.sdiff(keys);
        return this;
    }

    public ChainableTransaction sdiffstore(String dstkey, String... keys) {
        client.sdiffstore(dstkey, keys);
        return this;
    }

      public ChainableTransaction select(int index) {
        client.select(index);
          return this;
    }

    public ChainableTransaction set(String key, String value) {
        client.set(key, value);
        return this;
    }

    public ChainableTransaction setbit(String key, long offset, boolean value) {
        client.setbit(key, offset, value);
        return this;
    }

    public ChainableTransaction setex(String key, int seconds, String value) {
        client.setex(key, seconds, value);
        return this;
    }

    public ChainableTransaction setnx(String key, String value) {
        client.setnx(key, value);
        return this;
    }

    public ChainableTransaction setrange(String key, long offset, String value) {
        client.setrange(key, offset, value);
        return this;
    }

    public ChainableTransaction sinter(String... keys) {
        client.sinter(keys);
        return this;
    }

    public ChainableTransaction sinterstore(String dstkey, String... keys) {
        client.sinterstore(dstkey, keys);
        return this;
    }

    public ChainableTransaction sismember(String key, String member) {
        client.sismember(key, member);
        return this;
    }

    public ChainableTransaction smembers(String key) {
        client.smembers(key);
        return this;
    }

    public ChainableTransaction smove(String srckey, String dstkey, String member) {
        client.smove(srckey, dstkey, member);
        return this;
    }

    public ChainableTransaction sort(String key) {
        client.sort(key);
        return this;
    }

    public ChainableTransaction sort(String key,
            SortingParams sortingParameters) {
        client.sort(key, sortingParameters);
        return this;
    }

    public ChainableTransaction sort(String key,
            SortingParams sortingParameters, String dstkey) {
        client.sort(key, sortingParameters, dstkey);
        return this;
    }

    public ChainableTransaction sort(String key, String dstkey) {
        client.sort(key, dstkey);
        return this;
    }

    public ChainableTransaction spop(String key) {
        client.spop(key);
        return this;
    }

    public ChainableTransaction srandmember(String key) {
        client.srandmember(key);
        return this;
    }

    public ChainableTransaction srem(String key, String member) {
        client.srem(key, member);
        return this;
    }

    public ChainableTransaction strlen(String key) {
        client.strlen(key);
        return this;
    }

    public ChainableTransaction substr(String key, int start, int end) {
        client.substr(key, start, end);
        return this;
    }

    public ChainableTransaction sunion(String... keys) {
        client.sunion(keys);
        return this;
    }

    public ChainableTransaction sunionstore(String dstkey, String... keys) {
        client.sunionstore(dstkey, keys);
        return this;
    }

    public ChainableTransaction ttl(String key) {
        client.ttl(key);
        return this;
    }

    public ChainableTransaction type(String key) {
        client.type(key);
        return this;
    }

    public ChainableTransaction zadd(String key, double score, String member) {
        client.zadd(key, score, member);
        return this;
    }

    public ChainableTransaction zcard(String key) {
        client.zcard(key);
        return this;
    }

    public ChainableTransaction zcount(String key, double min, double max) {
        client.zcount(key, min, max);
        return this;
    }

    public ChainableTransaction zincrby(String key, double score, String member) {
        client.zincrby(key, score, member);
        return this;
    }

    public ChainableTransaction zinterstore(String dstkey, String... sets) {
        client.zinterstore(dstkey, sets);
        return this;
    }

    public ChainableTransaction zinterstore(String dstkey, ZParams params,
            String... sets) {
        client.zinterstore(dstkey, params, sets);
        return this;
    }

    public ChainableTransaction zrange(String key, int start, int end) {
        client.zrange(key, start, end);
        return this;
    }

    public ChainableTransaction zrangeByScore(String key, double min,
            double max) {
        client.zrangeByScore(key, min, max);
        return this;
    }

    public ChainableTransaction zrangeByScore(String key, String min,
            String max) {
        client.zrangeByScore(key, min, max);
        return this;
    }

    public ChainableTransaction zrangeByScore(String key, double min,
            double max, int offset, int count) {
        client.zrangeByScore(key, min, max, offset, count);
        return this;
    }

    public ChainableTransaction zrangeByScoreWithScores(String key, double min,
            double max) {
        client.zrangeByScoreWithScores(key, min, max);
        return this;
    }

    public ChainableTransaction zrangeByScoreWithScores(String key, double min,
            double max, int offset, int count) {
        client.zrangeByScoreWithScores(key, min, max, offset, count);
        return this;
    }

    public ChainableTransaction zrangeWithScores(String key, int start, int end) {
        client.zrangeWithScores(key, start, end);
        return this;
    }

    public ChainableTransaction zrank(String key, String member) {
        client.zrank(key, member);
        return this;
    }

    public ChainableTransaction zrem(String key, String member) {
        client.zrem(key, member);
        return this;
    }

    public ChainableTransaction zremrangeByRank(String key, int start, int end) {
        client.zremrangeByRank(key, start, end);
        return this;
    }

    public ChainableTransaction zremrangeByScore(String key, double start, double end) {
        client.zremrangeByScore(key, start, end);
        return this;
    }

    public ChainableTransaction zrevrange(String key, int start, int end) {
        client.zrevrange(key, start, end);
        return this;
    }

    public ChainableTransaction zrevrangeWithScores(String key, int start,
            int end) {
        client.zrevrangeWithScores(key, start, end);
        return this;
    }

    public ChainableTransaction zrevrank(String key, String member) {
        client.zrevrank(key, member);
        return this;
    }

    public ChainableTransaction zscore(String key, String member) {
        client.zscore(key, member);
        return this;
    }

    public ChainableTransaction zunionstore(String dstkey, String... sets) {
        client.zunionstore(dstkey, sets);
        return this;
    }

    public ChainableTransaction zunionstore(String dstkey, ZParams params,
            String... sets) {
        client.zunionstore(dstkey, params, sets);
        return this;
    }

    public ChainableTransaction bgrewriteaof() {
        client.bgrewriteaof();
        return this;
    }

    public ChainableTransaction bgsave() {
        client.bgsave();
        return this;
    }

    public ChainableTransaction configGet(String pattern) {
        client.configGet(pattern);
        return this;
    }

    public ChainableTransaction configSet(String parameter, String value) {
        client.configSet(parameter, value);
        return this;
    }

    public ChainableTransaction brpoplpush(String source, String destination,
            int timeout) {
        client.brpoplpush(source, destination, timeout);
        return this;
    }

    public ChainableTransaction configResetStat() {
        client.configResetStat();
        return this;
    }

    public ChainableTransaction save() {
        client.save();
        return this;
    }

    public ChainableTransaction lastsave() {
        client.lastsave();
        return this;
    }

    public ChainableTransaction publish(String channel, String message) {
        client.publish(channel, message);
        return this;
    }

    public ChainableTransaction publish(byte[] channel, byte[] message) {
        client.publish(channel, message);
        return this;
    }

}