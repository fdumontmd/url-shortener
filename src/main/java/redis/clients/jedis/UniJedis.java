/*
 * derived from http://groups.google.com/group/jedis_redis/msg/c8c76371cf543e36
 * Initial implementation by Ingvar Bogdahn
 * password for writePool, missing implementation for linsert(String...) and other small fixes by Frédéric Dumont
 */

package redis.clients.jedis;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.pool.impl.GenericObjectPool;

import redis.clients.jedis.BinaryClient.LIST_POSITION;
import redis.clients.util.Pool;
import redis.clients.util.SafeEncoder;


public class UniJedis implements JedisCommands, BinaryJedisCommands {


    private Pool<Jedis> writePool;
    private int standardDB = 0;

    private String masterIP;
    private int masterPort;
    private String password;
    private Pool<Jedis> readPool;
    private boolean readPoolSet = false;
    private int redundancyFactor = 2;       // this also works as a switch between single node / local use of jedis: redundancyFactor 0


// Todo -- currently for each command, a TTL is lost for calling select. If this was done in a pipeline, then we risk to get exceptions, in multithreading ?

// Todo -- Select semantics: setting standardDB. Is this wise, in multithreading?
// Todo -- slaves disburden master of persistence. How to integrate?
// Todo --failsave?

// Transactions / Pipeline commands that return a Transaction object, so it can be chained like this: Response<String> result1; uj.watch(keys).set(k, v).get(k2, result1).exec()
// for some reason, Response Object doesn't get reset --> NPE  --> falling back to old Mechanism, using the List<Object>

    public ChainableTransaction watch(String... keys) {
        Jedis j = writePool.getResource();
        Client c = j.getClient();
        c.watch(keys);
        Transaction jt = j.multi();
        return new ChainableTransaction(writePool, c, j);
    }

    public ChainableTransaction watch(DBKeys... keys) {
        Jedis j = writePool.getResource();
        Client c = j.getClient();
        for (DBKeys dbk : keys) {
            c.select(dbk.db);
            c.watch(dbk.keys);
        }
        Transaction jt = j.multi();
        return new ChainableTransaction(writePool, c, j);
    }

    public UniJedis(String masterIP, int masterPort, String password) {
        this.masterIP = masterIP;
        this.masterPort = masterPort;
        this.password = password;
        this.writePool = new JedisPool(new GenericObjectPool.Config(), masterIP, masterPort, Protocol.DEFAULT_TIMEOUT, password);
        this.readPool = writePool;
    }

    public UniJedis(String masterIP, int masterPort, String password, List<JedisShardInfo> shards) {
        this(masterIP, masterPort, password);
        this.readPoolSet = true;
        this.readPool = new RoundRobinPool(new JedisPoolConfig(), masterIP, masterPort, password, shards);
    }


    public void setShards(List<JedisShardInfo> shards) {
        readPool = new RoundRobinPool(new JedisPoolConfig(), masterIP, masterPort, password, shards);
        readPoolSet = true;
    }

    public void setMinIdle(boolean grow) {
        if (readPoolSet) ((RoundRobinPool) readPool).setWhenExhaustedGrow(grow);
    }

     public void setMinIdle(int minIdle) {
        if (readPoolSet) ((RoundRobinPool) readPool).setMinIdle(minIdle);
    }

    public void setMaxIdle(int maxIdle) {
       if (readPoolSet) ((RoundRobinPool) readPool).setMinIdle(maxIdle);
   }

    public void setTestOnBorrow(boolean validate) {
        if (readPoolSet) ((RoundRobinPool) readPool).setTestOnBorrow(validate);
    }

    public void setTestOnReturn(boolean validate) {
        if (readPoolSet) ((RoundRobinPool) readPool).setTestOnReturn(validate);
    }

    /*
    public void addSlaveToRoundRobinPool(JedisShardInfo jsi) {
        if (readPoolSet) {
            try {
                ((RoundRobinPool) readPool).addSlaveToRoundRobin(jsi);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
      */

    public String robinPing() {
    if(readPoolSet){
        Jedis j = readPool.getResource();
        String result = j.ping();
        readPool.returnResource(j); return result;}
        else
            return null;
    }

    public Long del(String... keys) {
        return del(standardDB, redundancyFactor, keys);
    }

    public Long del(int db, int toTryCount, String... keys) {
        Jedis j = writePool.getResource();
        j.select(db);
        Long result = j.del(keys);
        writePool.returnResource(j);
        return result;
    }

    public Long del(byte[]... keys) {
        return del(standardDB, redundancyFactor, keys);
    }

    public Long del(int db, int toTryCount, byte[]... keys) {
        Jedis j = writePool.getResource();
        j.select(db);
        Long result = j.del(keys);
        writePool.returnResource(j);
        return result;
    }


    public String ping() {
        return masterPing();
    }

    public String masterPing() {
        Jedis j = writePool.getResource();
        String result = j.ping();
        writePool.returnResource(j);
        return result;
    }


    public byte[] get(byte[] key) {
        return get(standardDB, redundancyFactor, key);
    }

    public byte[] get(int db, int toTryCount, byte[] key) {
        byte[] result = null;
        Jedis j = null;
        if (toTryCount > 0) j = readPool.getResource();
        else j = writePool.getResource();
        try {
            j.select(db);
            result = j.get(key);
        } catch (Exception ex) {
            if (toTryCount > 0) result = get(db, toTryCount - 1, key);
            else ex.printStackTrace(); /*throw new Exception(ex);         */
        } finally {
            if (toTryCount > 0) readPool.returnResource(j);
            else writePool.returnResource(j);
        }
        return result;
    }

    public Boolean exists(byte[] key) {
        return exists(standardDB, redundancyFactor, key);
    }

    public Boolean exists(int db, int toTryCount, byte[] key) {
        Boolean result = null;
        Jedis j = null;
        if (toTryCount > 0) j = readPool.getResource();
        else j = writePool.getResource();
        try {
            j.select(db);
            result = j.exists(key);
        } catch (Exception ex) {
            if (toTryCount > 0) result = exists(db, toTryCount - 1, key);
            else ex.printStackTrace(); /*throw new Exception(ex);         */
        } finally {
            if (toTryCount > 0) readPool.returnResource(j);
            else writePool.returnResource(j);
        }
        return result;
    }

    public String type(byte[] key) {
        return type(standardDB, redundancyFactor, key);
    }

    public String type(int db, int toTryCount, byte[] key) {
        String result =null;
        Jedis j = null;
        if (toTryCount > 0) j = readPool.getResource();
        else j = writePool.getResource();
        try {
            j.select(db);
            result = j.type(key);
        } catch (Exception ex) {
            if (toTryCount > 0) result = type(db, toTryCount - 1, key);
            else ex.printStackTrace(); /*throw new Exception(ex);         */
        } finally {
            if (toTryCount > 0) readPool.returnResource(j);
            else writePool.returnResource(j);
        }
        return result;
    }


    public Long expire(byte[] key, int seconds) {
        return expire(standardDB, redundancyFactor, key, seconds);
    }

    public Long expire(int db, int toTryCount, byte[] key, int seconds) {
        Jedis j = writePool.getResource();
        j.select(db);
        Long result = j.expire(key, seconds);
        writePool.returnResource(j);
        return result;
    }

    public Long expireAt(byte[] key, long unixTime) {
        return expireAt(standardDB, redundancyFactor, key, unixTime);
    }

    public Long expireAt(int db, int toTryCount, byte[] key, long unixTime) {
        Jedis j = writePool.getResource();
        j.select(db);
        Long result = j.expireAt(key, unixTime);
        writePool.returnResource(j);
        return result;
    }

    public Long ttl(byte[] key) {
        return ttl(standardDB, redundancyFactor, key);
    }

    public Long ttl(int db, int toTryCount, byte[] key) {
        Jedis j = writePool.getResource();
        j.select(db);
        Long result = j.ttl(key);
        writePool.returnResource(j);
        return result;
    }


    public byte[] getSet(byte[] key, byte[] value) {
        return getSet(standardDB, redundancyFactor, key, value);
    }

    public byte[] getSet(int db, int toTryCount, byte[] key, byte[] value) {
        Jedis j = writePool.getResource();
        j.select(db);
        byte[] result = j.getSet(key, value);
        writePool.returnResource(j);
        return result;
    }

    public Long setnx(byte[] key, byte[] value) {
        return setnx(standardDB, redundancyFactor, key, value);
    }

    public Long setnx(int db, int toTryCount, byte[] key, byte[] value) {
        Jedis j = writePool.getResource();
        j.select(db);
        Long result = j.setnx(key, value);
        writePool.returnResource(j);
        return result;
    }

    public String setex(byte[] key, int seconds, byte[] value) {
        return setex(standardDB, redundancyFactor, key, seconds, value);
    }

    public String setex(int db, int toTryCount, byte[] key, int seconds, byte[] value) {
        Jedis j = writePool.getResource();
        j.select(db);
        String result = j.setex(key, seconds, value);
        writePool.returnResource(j);
        return result;
    }

    public Long decrBy(byte[] key, long integer) {
        return decrBy(standardDB, redundancyFactor, key, integer);
    }

    public Long decrBy(int db, int toTryCount, byte[] key, long integer) {
        Jedis j = writePool.getResource();
        j.select(db);
        Long result = j.decrBy(key, integer);
        writePool.returnResource(j);
        return result;
    }

    public Long decr(byte[] key) {
        return decr(standardDB, redundancyFactor, key);
    }

    public Long decr(int db, int toTryCount, byte[] key) {
        Jedis j = writePool.getResource();
        j.select(db);
        Long result = j.decr(key);
        writePool.returnResource(j);
        return result;
    }

    public void select(int newDB) {
        standardDB = newDB;
    }

    public Long incrBy(byte[] key, long integer) {
        return incrBy(standardDB, redundancyFactor, key, integer);
    }

    public Long incrBy(int db, int toTryCount, byte[] key, long integer) {
        Jedis j = writePool.getResource();
        j.select(db);
        Long result = j.incrBy(key, integer);
        writePool.returnResource(j);
        return result;
    }

    public Long incr(byte[] key) {
        return incr(standardDB, redundancyFactor, key);
    }

    public Long incr(int db, int toTryCount, byte[] key) {
        Jedis j = writePool.getResource();
        j.select(db);
        Long result = j.incr(key);
        writePool.returnResource(j);
        return result;
    }

    public Long append(byte[] key, byte[] value) {
        return append(standardDB, redundancyFactor, key, value);
    }

    public Long append(int db, int toTryCount, byte[] key, byte[] value) {
        Jedis j = writePool.getResource();
        j.select(db);
        Long result = j.append(key, value);
        writePool.returnResource(j);
        return result;
    }

    public byte[] substr(byte[] key, int start, int end) {
        return substr(standardDB, redundancyFactor, key, start, end);
    }

    public byte[] substr(int db, int toTryCount, byte[] key, int start, int end) {
        Jedis j = writePool.getResource();
        j.select(db);
        byte[] result = j.substr(key, start, end);
        writePool.returnResource(j);
        return result;
    }

    public Long hset(byte[] key, byte[] field, byte[] value) {
        return hset(standardDB, redundancyFactor, key, field, value);
    }

    public Long hset(int db, int toTryCount, byte[] key, byte[] field, byte[] value) {
        Jedis j = writePool.getResource();
        j.select(db);
        Long result = j.hset(key, field, value);
        writePool.returnResource(j);
        return result;
    }

    public byte[] hget(byte[] key, byte[] field) {
        return hget(standardDB, redundancyFactor, key, field);
    }

    public byte[] hget(int db, int toTryCount, byte[] key, byte[] field) {
        Jedis j = null;
        byte[] result = null;
        if (toTryCount > 0) j = readPool.getResource();
        else j = writePool.getResource();
        try {
            j.select(db);
            result = j.hget(key, field);
        }
        catch (Exception ex) {
            if (toTryCount > 0) result = hget(db, toTryCount - 1, key, field);
            else ex.printStackTrace(); /*throw new Exception(ex);         */
        } finally {
            if (toTryCount > 0) readPool.returnResource(j);
            else writePool.returnResource(j);
        }
        return result;
    }

    public Long hsetnx(byte[] key, byte[] field, byte[] value) {
        return hsetnx(standardDB, redundancyFactor, key, field, value);
    }

    public Long hsetnx(int db, int toTryCount, byte[] key, byte[] field, byte[] value) {
        Jedis j = writePool.getResource();
        j.select(db);
        Long result = j.hsetnx(key, field, value);
        writePool.returnResource(j);
        return result;
    }

    public String hmset(byte[] key, Map<byte[], byte[]> hash) {
        return hmset(standardDB, redundancyFactor, key, hash);
    }

    public String hmset(int db, int toTryCount, byte[] key, Map<byte[], byte[]> hash) {
        Jedis j = writePool.getResource();
        j.select(db);
        String result = j.hmset(key, hash);
        writePool.returnResource(j);
        return result;
    }

    public List<byte[]> hmget(byte[] key, byte[]... fields) {
        return hmget(standardDB, redundancyFactor, key, fields);
    }

    public List<byte[]> hmget(int db, int toTryCount, byte[] key, byte[]... fields) {
        List<byte[]> result = null;
        Jedis j = null;
        if (toTryCount > 0) j = readPool.getResource();
        else j = writePool.getResource();
        try {
            j.select(db);
            result = j.hmget(key, fields);
        } catch (Exception ex) {
            if (toTryCount > 0) result = hmget(db, toTryCount - 1, key, fields);
            else ex.printStackTrace(); /*throw new Exception(ex);         */
        } finally {
            if (toTryCount > 0) readPool.returnResource(j);
            else writePool.returnResource(j);
        }
        return result;
    }

    public Long hincrBy(byte[] key, byte[] field, long value) {
        return hincrBy(standardDB, redundancyFactor, key, field, value);
    }

    public Long hincrBy(int db, int toTryCount, byte[] key, byte[] field, long value) {
        Jedis j = writePool.getResource();
        j.select(db);
        Long result = j.hincrBy(key, field, value);
        writePool.returnResource(j);
        return result;
    }

    public Boolean hexists(byte[] key, byte[] field) {
        return hexists(standardDB, redundancyFactor, key, field);
    }

    public Boolean hexists(int db, int toTryCount, byte[] key, byte[] field) {
        Boolean result = null;
        Jedis j = null;
        if (toTryCount > 0) j = readPool.getResource();
        else j = writePool.getResource();
        try {
            j.select(db);
            result = j.hexists(key, field);
        } catch (Exception ex) {
            if (toTryCount > 0) result = hexists(db, toTryCount - 1, key, field);
            else ex.printStackTrace(); /*throw new Exception(ex);         */
        } finally {
            if (toTryCount > 0) readPool.returnResource(j);
            else writePool.returnResource(j);
        }
        return result;
    }

    public Long hdel(byte[] key, byte[] field) {
        return hdel(standardDB, redundancyFactor, key, field);
    }

    public Long hdel(int db, int toTryCount, byte[] key, byte[] field) {
        Jedis j = writePool.getResource();
        j.select(db);
        Long result = j.hdel(key, field);
        writePool.returnResource(j);
        return result;
    }

    public Long hlen(byte[] key) {
        return hlen(standardDB, redundancyFactor,key);
    }

    public Long hlen(int db,int toTryCount, byte[] key) {
        Long result = null;
        Jedis j = null;
        if (toTryCount > 0) j = readPool.getResource();
        else j = writePool.getResource();
        try {
            j.select(db);
            result = j.hlen(key);
        } catch (Exception ex) {
            if (toTryCount > 0) result = hlen(db, toTryCount - 1, key);
            else ex.printStackTrace(); /*throw new Exception(ex);         */
        } finally {
            if (toTryCount > 0) readPool.returnResource(j);
            else writePool.returnResource(j);
        }
        return result;
    }

    public Set<byte[]> hkeys(byte[] key) {
        return hkeys(standardDB, redundancyFactor, key);
    }

    public Set<byte[]> hkeys(int db,int toTryCount, byte[] key) {
        Set<byte[]> result =null;
        Jedis j = null;
        if (toTryCount > 0) j = readPool.getResource();
        else j = writePool.getResource();
        try {
            j.select(db);
            result = j.hkeys(key);
        } catch (Exception ex) {
            if (toTryCount > 0) result = hkeys(db, toTryCount - 1, key);
            else ex.printStackTrace(); /*throw new Exception(ex);         */
        } finally {
            if (toTryCount > 0) readPool.returnResource(j);
            else writePool.returnResource(j);
        }
        return result;
    }

    public Collection<byte[]> hvals(byte[] key) {
        return hvals(standardDB, redundancyFactor, key);
    }

    public Collection<byte[]> hvals(int db, int toTryCount, byte[] key) {
        Collection<byte[]> result = null;
        Jedis j = null;
        if (toTryCount > 0) j = readPool.getResource();
        else j = writePool.getResource();
        try {
            j.select(db);
            result = j.hvals(key);
        } catch (Exception ex) {
            if (toTryCount > 0) result = hvals(db, toTryCount - 1, key);
            else ex.printStackTrace(); /*throw new Exception(ex);         */
        } finally {
            if (toTryCount > 0) readPool.returnResource(j);
            else writePool.returnResource(j);
        }
        return result;
    }

    public Map<byte[], byte[]> hgetAll(byte[] key) {
        return hgetAll(standardDB, redundancyFactor, key);
    }

    public Map<byte[], byte[]> hgetAll(int db, int toTryCount, byte[] key) {
        Map<byte[], byte[]> result = null;
        Jedis j = null;
        if (toTryCount > 0) j = readPool.getResource();
        else j = writePool.getResource();
        try {
            j.select(db);
           result = j.hgetAll(key);
        } catch (Exception ex) {
            if (toTryCount > 0) result = hgetAll(db, toTryCount - 1, key);
            else ex.printStackTrace(); /*throw new Exception(ex);         */
        } finally {
            if (toTryCount > 0) readPool.returnResource(j);
            else writePool.returnResource(j);
        }
        return result;
    }

    public Long rpush(byte[] key, byte[] string) {
        return rpush(standardDB, redundancyFactor, key, string);
    }

    public Long rpush(int db, int toTryCount, byte[] key, byte[] string) {
        Jedis j = writePool.getResource();
        j.select(db);
        Long result = j.rpush(key, string);
        writePool.returnResource(j);
        return result;
    }

    public Long lpush(byte[] key, byte[] string) {
        return lpush(standardDB, redundancyFactor, key, string);
    }

    public Long lpush(int db, int toTryCount, byte[] key, byte[] string) {
        Jedis j = writePool.getResource();
        j.select(db);
        Long result = j.lpush(key, string);
        writePool.returnResource(j);
        return result;
    }

    public Long llen(byte[] key) {return llen(standardDB, redundancyFactor, key);}

    public Long llen(int db,int toTryCount, byte[] itsakey) {
        Long result = null;
        Jedis j = null;
        if (toTryCount > 0) j = readPool.getResource();
        else j = writePool.getResource();
        try {
            j.select(db);
            result = j.llen(itsakey);
        } catch (Exception ex) {
            if (toTryCount > 0) result = llen(db, toTryCount - 1, itsakey);
            else ex.printStackTrace(); /*throw new Exception(ex);         */
        } finally {
            if (toTryCount > 0) readPool.returnResource(j);
            else writePool.returnResource(j);
        }
        return result;
    }

    public List<byte[]> lrange(byte[] key, int start, int end) {
        return lrange(standardDB, redundancyFactor, key, start, end);
    }

    public List<byte[]> lrange(int db, int toTryCount,byte[] key, int start, int end) {
        List<byte[]> result = null;
        Jedis j = null;
        if (toTryCount > 0) j = readPool.getResource();
        else j = writePool.getResource();
        try {
            j.select(db);
            result = j.lrange(key, start, end);
        } catch (Exception ex) {
            if (toTryCount > 0) result = lrange(db, toTryCount - 1, key, start, end);
            else ex.printStackTrace(); /*throw new Exception(ex);         */
        } finally {
            if (toTryCount > 0) readPool.returnResource(j);
            else writePool.returnResource(j);
        }
        return result;
    }

    public String ltrim(byte[] key, int start, int end) {
        return ltrim(standardDB, redundancyFactor, key, start, end);
    }

    public String ltrim(int db, int toTryCount, byte[] key, int start, int end) {
        Jedis j = writePool.getResource();
        j.select(db);
        String result = j.ltrim(key, start, end);
        writePool.returnResource(j);
        return result;
    }

    public byte[] lindex(byte[] key, int index) {
        return lindex(standardDB, redundancyFactor, key, index);
    }

    public byte[] lindex(int db, int toTryCount, byte[] key, int index) {
        byte[] result = null;
        Jedis j = null;
        if (toTryCount > 0) j = readPool.getResource();
        else j = writePool.getResource();
        try {
            j.select(db);
            result = j.lindex(key, index);
        } catch (Exception ex) {
            if (toTryCount > 0) result = lindex(db, toTryCount - 1, key, index);
            else ex.printStackTrace(); /*throw new Exception(ex);         */
        } finally {
            if (toTryCount > 0) readPool.returnResource(j);
            else writePool.returnResource(j);
        }
        return result;
    }

    public String lset(byte[] key, int index, byte[] value) {
        return lset(standardDB, redundancyFactor, key, index, value);
    }

    public String lset(int db, int toTryCount, byte[] key, int index, byte[] value) {
        Jedis j = writePool.getResource();
        j.select(db);
        String result = j.lset(key, index, value);
        writePool.returnResource(j);
        return result;
    }

    public Long lrem(byte[] key, int count, byte[] value) {
        return lrem(standardDB, redundancyFactor, key, count, value);
    }

    public Long lrem(int db, int toTryCount, byte[] key, int count, byte[] value) {
        Jedis j = writePool.getResource();
        j.select(db);
        Long result = j.lrem(key, count, value);
        writePool.returnResource(j);
        return result;
    }

    public byte[] lpop(byte[] key) {
        return lpop(standardDB, redundancyFactor, key);
    }

    public byte[] lpop(int db, int toTryCount, byte[] key) {
        Jedis j = writePool.getResource();
        j.select(db);
        byte[] result = j.lpop(key);
        writePool.returnResource(j);
        return result;
    }

    public byte[] rpop(byte[] key) {
        return rpop(standardDB, redundancyFactor, key);
    }

    public byte[] rpop(int db, int toTryCount, byte[] key) {
        Jedis j = writePool.getResource();
        j.select(db);
        byte[] result = j.rpop(key);
        writePool.returnResource(j);
        return result;
    }

    public Long sadd(byte[] key, byte[] member) {
        return sadd(standardDB, redundancyFactor, key, member);
    }

    public Long sadd(int db, int toTryCount, byte[] key, byte[] member) {
        Jedis j = writePool.getResource();
        j.select(db);
        Long result = j.sadd(key, member);
        writePool.returnResource(j);
        return result;
    }

    public Set<byte[]> smembers(byte[] key) {
        return smembers(standardDB, redundancyFactor, key);
    }

    public Set<byte[]> smembers(int db, int toTryCount, byte[] key) {
        Set<byte[]> result = null;
        Jedis j = null;
        if (toTryCount > 0) j = readPool.getResource();
        else j = writePool.getResource();
        try {
            j.select(db);
            result = j.smembers(key);
        } catch (Exception ex) {
            if (toTryCount > 0) result = smembers(db, toTryCount - 1, key);
            else ex.printStackTrace(); /*throw new Exception(ex);         */
        } finally {
            if (toTryCount > 0) readPool.returnResource(j);
            else writePool.returnResource(j);
        }
        return result;
    }

    public Long srem(byte[] key, byte[] member) {
        return srem(standardDB, redundancyFactor, key, member);
    }

    public Long srem(int db, int toTryCount, byte[] key, byte[] member) {
        Jedis j = writePool.getResource();
        j.select(db);
        Long result = j.srem(key, member);
        writePool.returnResource(j);
        return result;
    }

    public byte[] spop(byte[] key) {
        return spop(standardDB, redundancyFactor, key);
    }

    public byte[] spop(int db, int toTryCount, byte[] key) {
        Jedis j = writePool.getResource();
        j.select(db);
        byte[] result = j.spop(key);
        writePool.returnResource(j);
        return result;
    }

    public Long scard(byte[] key) {
        return scard(standardDB, redundancyFactor, key);
    }

    public Long scard(int db, int toTryCount, byte[] key) {
        Long result =  null;
        Jedis j = null;
        if (toTryCount > 0) j = readPool.getResource();
        else j = writePool.getResource();
        try {
            j.select(db);
            result = j.scard(key);
        } catch (Exception ex) {
            if (toTryCount > 0) result = scard(db, toTryCount - 1, key);
            else ex.printStackTrace(); /*throw new Exception(ex);         */
        } finally {
            if (toTryCount > 0) readPool.returnResource(j);
            else writePool.returnResource(j);
        }
        return result;
    }

    public Boolean sismember(byte[] key, byte[] member) {
        return sismember(standardDB, redundancyFactor, key, member);
    }

    public Boolean sismember(int db, int toTryCount, byte[] key, byte[] member) {

        Boolean result = null;
        Jedis j = null;
        if (toTryCount > 0) j = readPool.getResource();
        else j = writePool.getResource();
        try {
            j.select(db);
            result = j.sismember(key, member);
        } catch (Exception ex) {
            if (toTryCount > 0) result = sismember(db, toTryCount - 1, key, member);
            else ex.printStackTrace(); /*throw new Exception(ex);         */
        } finally {
            if (toTryCount > 0) readPool.returnResource(j);
            else writePool.returnResource(j);
        }
        return result;
    }

    public byte[] srandmember(byte[] key) {
        return srandmember(standardDB, redundancyFactor, key);
    }

    public byte[] srandmember(int db, int toTryCount, byte[] key) {
        byte[] result = null;
        Jedis j = null;
        if (toTryCount > 0) j = readPool.getResource();
        else j = writePool.getResource();
        try {
            j.select(db);
            result = j.srandmember(key);
        } catch (Exception ex) {
            if (toTryCount > 0) result = srandmember(db, toTryCount - 1, key);
            else ex.printStackTrace(); /*throw new Exception(ex);         */
        } finally {
            if (toTryCount > 0) readPool.returnResource(j);
            else writePool.returnResource(j);
        }
        return result;
    }

    public Long zadd(byte[] key, double score, byte[] member) {
        return zadd(standardDB, redundancyFactor, key, score, member);
    }

    public Long zadd(int db, int toTryCount, byte[] key, double score, byte[] member) {
        Jedis j = writePool.getResource();
        j.select(db);
        Long result = j.zadd(key, score, member);
        writePool.returnResource(j);
        return result;
    }

    public Set<byte[]> zrange(byte[] key, int start, int end) {
        return zrange(standardDB, redundancyFactor, key, start, end);
    }

    public Set<byte[]> zrange(int db, int toTryCount, byte[] key, int start, int end) {
        Set<byte[]> result = null;
        Jedis j = null;
        if (toTryCount > 0) j = readPool.getResource();
        else j = writePool.getResource();
        try {
            j.select(db);
            result = j.zrange(key, start, end);
        } catch (Exception ex) {
            if (toTryCount > 0) result = zrange(db, toTryCount - 1, key, start, end);
            else ex.printStackTrace(); /*throw new Exception(ex);         */
        } finally {
            if (toTryCount > 0) readPool.returnResource(j);
            else writePool.returnResource(j);
        }
        return result;
    }

    public Long zrem(byte[] key, byte[] member) {
        return zrem(standardDB, redundancyFactor, key, member);
    }

    public Long zrem(int db, int toTryCount, byte[] key, byte[] member) {
        Jedis j = writePool.getResource();
        j.select(db);
        Long result = j.zrem(key, member);
        writePool.returnResource(j);
        return result;
    }

    public Double zincrby(byte[] key, double score, byte[] member) {
        return zincrby(standardDB, redundancyFactor, key, score, member);
    }

    public Double zincrby(int db, int toTryCount, byte[] key, double score, byte[] member) {
        Jedis j = writePool.getResource();
        j.select(db);
        Double result = j.zincrby(key, score, member);
        writePool.returnResource(j);
        return result;
    }

    public Long zrank(byte[] key, byte[] member) {
        return zrank(standardDB, redundancyFactor, key, member);
    }

    public Long zrank(int db, int toTryCount, byte[] key, byte[] member) {
        Long result = null;
        Jedis j = null;
        if (toTryCount > 0) j = readPool.getResource();
        else j = writePool.getResource();
        try {
            j.select(db);
            result = j.zrank(key, member);
        } catch (Exception ex) {
            if (toTryCount > 0) result = zrank(db, toTryCount - 1, key, member);
            else ex.printStackTrace(); /*throw new Exception(ex);         */
        } finally {
            if (toTryCount > 0) readPool.returnResource(j);
            else writePool.returnResource(j);
        }
        return result;
    }

    public Long zrevrank(byte[] key, byte[] member) {
        return zrevrank(standardDB, redundancyFactor, key, member);
    }

    public Long zrevrank(int db, int toTryCount, byte[] key, byte[] member) {
        Long result = null;
        Jedis j = null;
        if (toTryCount > 0) j = readPool.getResource();
        else j = writePool.getResource();
        try {
            j.select(db);
            result = j.zrevrank(key, member);
        } catch (Exception ex) {
            if (toTryCount > 0) result = zrevrank(db, toTryCount - 1, key, member);
            else ex.printStackTrace(); /*throw new Exception(ex);         */
        } finally {
            if (toTryCount > 0) readPool.returnResource(j);
            else writePool.returnResource(j);
        }
        return result;
    }

    public Set<byte[]> zrevrange(byte[] key, int start, int end) {
        return zrevrange(standardDB, redundancyFactor, key, start, end);
    }

    public Set<byte[]> zrevrange(int db, int toTryCount, byte[] key, int start, int end) {
        Set<byte[]> result = null;
        Jedis j = null;
        if (toTryCount > 0) j = readPool.getResource();
        else j = writePool.getResource();
        try {
            j.select(db);
            result = j.zrevrange(key, start, end);
        } catch (Exception ex) {
            if (toTryCount > 0) result = zrevrange(db, toTryCount - 1, key, start, end);
            else ex.printStackTrace(); /*throw new Exception(ex);         */
        } finally {
            if (toTryCount > 0) readPool.returnResource(j);
            else writePool.returnResource(j);
        }
        return result;
    }

    public Set<Tuple> zrangeWithScores(byte[] key, int start, int end) {
        return zrangeWithScores(standardDB, redundancyFactor, key, start, end);
    }

    public Set<Tuple> zrangeWithScores(int db, int toTryCount, byte[] key, int start, int end) {
        Set<Tuple> result = null;
        Jedis j = null;
        if (toTryCount > 0) j = readPool.getResource();
        else j = writePool.getResource();
        try {
            j.select(db);
            result = j.zrangeWithScores(key, start, end);
        } catch (Exception ex) {
            if (toTryCount > 0) result = zrangeWithScores(db, toTryCount - 1, key, start, end);
            else ex.printStackTrace(); /*throw new Exception(ex);         */
        } finally {
            if (toTryCount > 0) readPool.returnResource(j);
            else writePool.returnResource(j);
        }
        return result;
    }

    public Set<Tuple> zrevrangeWithScores(byte[] key, int start, int end) {
        return zrevrangeWithScores(standardDB, redundancyFactor, key, start, end);
    }

    public Set<Tuple> zrevrangeWithScores(int db, int toTryCount, byte[] key, int start, int end) {
        Set<Tuple> result = null;
        Jedis j = null;
        if (toTryCount > 0) j = readPool.getResource();
        else j = writePool.getResource();
        try {
            j.select(db);
            result = j.zrevrangeWithScores(key, start, end);
        } catch (Exception ex) {
            if (toTryCount > 0) result = zrevrangeWithScores(db, toTryCount - 1, key, start, end);
            else ex.printStackTrace(); /*throw new Exception(ex);         */
        } finally {
            if (toTryCount > 0) readPool.returnResource(j);
            else writePool.returnResource(j);
        }
        return result;
    }

    public Long zcard(byte[] key) {
        return zcard(standardDB, redundancyFactor, key);
    }

    public Long zcard(int db, int toTryCount, byte[] key) {
        Long result = null;
        Jedis j = null;
        if (toTryCount > 0) j = readPool.getResource();
        else j = writePool.getResource();
        try {
            j.select(db);
            result = j.zcard(key);
        } catch (Exception ex) {
            if (toTryCount > 0) result = zcard(db, toTryCount - 1, key);
            else ex.printStackTrace(); /*throw new Exception(ex);         */
        } finally {
            if (toTryCount > 0) readPool.returnResource(j);
            else writePool.returnResource(j);
        }
        return result;
    }

    public Double zscore(byte[] key, byte[] member) {
        return zscore(standardDB, redundancyFactor, key, member);
    }

    public Double zscore(int db, int toTryCount, byte[] key, byte[] member) {
        Double result =null;
        Jedis j = null;
        if (toTryCount > 0) j = readPool.getResource();
        else j = writePool.getResource();
        try {
            j.select(db);
            result = j.zscore(key, member);
        } catch (Exception ex) {
            if (toTryCount > 0) result = zscore(db, toTryCount - 1, key, member);
            else ex.printStackTrace(); /*throw new Exception(ex);         */
        } finally {
            if (toTryCount > 0) readPool.returnResource(j);
            else writePool.returnResource(j);
        }
        return result;
    }

    public List<byte[]> sort(byte[] key) {
        return sort(standardDB, redundancyFactor, key);
    }

    public List<byte[]> sort(int db, int toTryCount, byte[] key) {
        List<byte[]> result =null;
        Jedis j = null;
        if (toTryCount > 0) j = readPool.getResource();
        else j = writePool.getResource();
        try {
            j.select(db);
            result = j.sort(key);
        } catch (Exception ex) {
            if (toTryCount > 0) result = sort(db, toTryCount - 1, key);
            else ex.printStackTrace(); /*throw new Exception(ex);         */
        } finally {
            if (toTryCount > 0) readPool.returnResource(j);
            else writePool.returnResource(j);
        }
        return result;
    }

    public List<byte[]> sort(byte[] key, SortingParams sortingParameters) {
        return sort(standardDB, redundancyFactor, key, sortingParameters);
    }

    public List<byte[]> sort(int db, int toTryCount, byte[] key, SortingParams sortingParameters) {
        Jedis j = writePool.getResource();                                       //using writePool here, since using Store option, user might store the sorted collection in Redis
        j.select(db);
        List<byte[]> result = j.sort(key, sortingParameters);
        writePool.returnResource(j);
        return result;
    }

    public Long zcount(byte[] key, double min, double max) {
        return zcount(standardDB, redundancyFactor, key, min, max);
    }

    public Long zcount(int db, int toTryCount, byte[] key, double min, double max) {
        Long result =null;
        Jedis j = null;
        if (toTryCount > 0) j = readPool.getResource();
        else j = writePool.getResource();
        try {
            j.select(db);
            result = j.zcount(key, min, max);
        } catch (Exception ex) {
            if (toTryCount > 0) result = zcount(db, toTryCount - 1, key, min, max);
            else ex.printStackTrace(); /*throw new Exception(ex);         */
        } finally {
            if (toTryCount > 0) readPool.returnResource(j);
            else writePool.returnResource(j);
        }
        return result;
    }

    public Set<byte[]> zrangeByScore(byte[] key, double min, double max) {
        return zrangeByScore(standardDB, redundancyFactor, key, min, max);
    }

    public Set<byte[]> zrangeByScore(int db, int toTryCount, byte[] key, double min, double max) {
        Set<byte[]> result =null;
        Jedis j = null;
        if (toTryCount > 0) j = readPool.getResource();
        else j = writePool.getResource();
        try {
            j.select(db);
            result = j.zrangeByScore(key, min, max);
        } catch (Exception ex) {
            if (toTryCount > 0) result = zrangeByScore(db, toTryCount - 1, key, min, max);
            else ex.printStackTrace(); /*throw new Exception(ex);         */
        } finally {
            if (toTryCount > 0) readPool.returnResource(j);
            else writePool.returnResource(j);
        }
        return result;
    }

    public Set<byte[]> zrangeByScore(byte[] key, double min, double max, int offset, int count) {
        return zrangeByScore(standardDB, redundancyFactor, key, min, max, offset, count);
    }

    public Set<byte[]> zrangeByScore(int db, int toTryCount, byte[] key, double min, double max, int offset, int count) {
        Set<byte[]> result =null;
        Jedis j = null;
        if (toTryCount > 0) j = readPool.getResource();
        else j = writePool.getResource();
        try {
            j.select(db);
            result = j.zrangeByScore(key, min, max, offset, count);
        } catch (Exception ex) {
            if (toTryCount > 0) result = zrangeByScore(db, toTryCount - 1, key, min, max, offset, count);
            else ex.printStackTrace(); /*throw new Exception(ex);         */
        } finally {
            if (toTryCount > 0) readPool.returnResource(j);
            else writePool.returnResource(j);
        }
        return result;
    }

    public Set<Tuple> zrangeByScoreWithScores(byte[] key, double min, double max) {
        return zrangeByScoreWithScores(standardDB, redundancyFactor, key, min, max);
    }

    public Set<Tuple> zrangeByScoreWithScores(int db, int toTryCount, byte[] key, double min, double max) {
        Set<Tuple> result =null;
        Jedis j = null;
        if (toTryCount > 0) j = readPool.getResource();
        else j = writePool.getResource();
        try {
            j.select(db);
            result = j.zrangeByScoreWithScores(key,min, max);
        } catch (Exception ex) {
            if (toTryCount > 0) result = zrangeByScoreWithScores(db, toTryCount - 1, key, min, max);
            else ex.printStackTrace(); /*throw new Exception(ex);         */
        } finally {
            if (toTryCount > 0) readPool.returnResource(j);
            else writePool.returnResource(j);
        }
        return result;
    }

    public Set<Tuple> zrangeByScoreWithScores(byte[] key, double min, double max, int offset, int count) {
        return zrangeByScoreWithScores(standardDB, redundancyFactor, key, min, max, offset, count);
    }

    public Set<Tuple> zrangeByScoreWithScores(int db, int toTryCount, byte[] key, double min, double max, int offset, int count) {
        Set<Tuple> result =null;
        Jedis j = null;
        if (toTryCount > 0) j = readPool.getResource();
        else j = writePool.getResource();
        try {
            j.select(db);
            result = j.zrangeByScoreWithScores(key, min, max,offset, count);
        } catch (Exception ex) {
            if (toTryCount > 0) result = zrangeByScoreWithScores(db, toTryCount - 1, key, min, max, offset, count);
            else ex.printStackTrace(); /*throw new Exception(ex);         */
        } finally {
            if (toTryCount > 0) readPool.returnResource(j);
            else writePool.returnResource(j);
        }
        return result;
    }

    public Set<byte[]> zrevrangeByScore(byte[] key, double max, double min) {
        return zrevrangeByScore(standardDB, redundancyFactor, key, max, min);
    }

    public Set<byte[]> zrevrangeByScore(int db, int toTryCount, byte[] key, double max, double min) {
        Set<byte[]> result =null;
        Jedis j = null;
        if (toTryCount > 0) j = readPool.getResource();
        else j = writePool.getResource();
        try {
            j.select(db);
            result = j.zrevrangeByScore(key, max, min);
        } catch (Exception ex) {
            if (toTryCount > 0) result = zrevrangeByScore(db, toTryCount - 1, key, max, min);
            else ex.printStackTrace(); /*throw new Exception(ex);         */
        } finally {
            if (toTryCount > 0) readPool.returnResource(j);
            else writePool.returnResource(j);
        }
        return result;
    }

    public Set<byte[]> zrevrangeByScore(byte[] key, double max, double min, int offset, int count) {
        return zrevrangeByScore(standardDB, redundancyFactor, key, max, min, offset, count);
    }

    public Set<byte[]> zrevrangeByScore(int db, int toTryCount, byte[] key, double max, double min, int offset, int count) {
        Set<byte[]> result =null;
        Jedis j = null;
        if (toTryCount > 0) j = readPool.getResource();
        else j = writePool.getResource();
        try {
            j.select(db);
            result = j.zrevrangeByScore(key, max, min, offset, count);
        } catch (Exception ex) {
            if (toTryCount > 0) result = zrevrangeByScore(db, toTryCount - 1, key, max, min, offset, count);
            else ex.printStackTrace(); /*throw new Exception(ex);         */
        } finally {
            if (toTryCount > 0) readPool.returnResource(j);
            else writePool.returnResource(j);
        }
        return result;
    }

    public Set<Tuple> zrevrangeByScoreWithScores(byte[] key, double max, double min) {
        return zrevrangeByScoreWithScores(standardDB, redundancyFactor, key, max, min);
    }

    public Set<Tuple> zrevrangeByScoreWithScores(int db, int toTryCount, byte[] key, double max, double min) {
        Set<Tuple> result =null;
        Jedis j = null;
        if (toTryCount > 0) j = readPool.getResource();
        else j = writePool.getResource();
        try {
            j.select(db);
            result = j.zrevrangeByScoreWithScores(key, max, min);
        } catch (Exception ex) {
            if (toTryCount > 0) result = zrevrangeByScoreWithScores(db, toTryCount - 1, key, max, min);
            else ex.printStackTrace(); /*throw new Exception(ex);         */
        } finally {
            if (toTryCount > 0) readPool.returnResource(j);
            else writePool.returnResource(j);
        }
        return result;
    }

    public Set<Tuple> zrevrangeByScoreWithScores(byte[] key, double max, double min, int offset, int count) {
        return zrevrangeByScoreWithScores(standardDB, redundancyFactor, key, max, min, offset, count);
    }

    public Set<Tuple> zrevrangeByScoreWithScores(int db, int toTryCount, byte[] key, double max, double min, int offset, int count) {
        Set<Tuple> result =null;
        Jedis j = null;
        if (toTryCount > 0) j = readPool.getResource();
        else j = writePool.getResource();
        try {
            j.select(db);
            result = j.zrevrangeByScoreWithScores(key, max, min, offset, count);
        } catch (Exception ex) {
            if (toTryCount > 0) result = zrevrangeByScoreWithScores(db, toTryCount - 1, key, max, min, offset, count);
            else ex.printStackTrace(); /*throw new Exception(ex);         */
        } finally {
            if (toTryCount > 0) readPool.returnResource(j);
            else writePool.returnResource(j);
        }
        return result;
    }

    public Long zremrangeByRank(byte[] key, int start, int end) {
        return zremrangeByRank(standardDB, key, start, end);
    }

    public Long zremrangeByRank(int db, byte[] key, int start, int end) {
        Jedis j = writePool.getResource();
        j.select(db);
        Long result = j.zremrangeByRank(key, start, end);
        writePool.returnResource(j);
        return result;
    }

    public Long zremrangeByScore(byte[] key, double start, double end) {
        return zremrangeByScore(standardDB, key, start, end);
    }

    public Long zremrangeByScore(int db, byte[] key, double start, double end) {
        Jedis j = writePool.getResource();
        j.select(db);
        Long result = j.zremrangeByScore(key, start, end);
        writePool.returnResource(j);
        return result;
    }

    public Long linsert(byte[] key, BinaryClient.LIST_POSITION where, byte[] pivot, byte[] value) {
        return linsert(standardDB, key, where, pivot, value);
    }

    public Long linsert(int db, byte[] key, BinaryClient.LIST_POSITION where, byte[] pivot, byte[] value) {
        Jedis j = writePool.getResource();
        j.select(db);
        Long result = j.linsert(key, where, pivot, value);
        writePool.returnResource(j);
        return result;
    }
    
	public Long linsert(int db, String key, LIST_POSITION where, String pivot,
			String value) {
        Jedis j = writePool.getResource();
        j.select(db);
        Long result = j.linsert(key, where, pivot, value);
        writePool.returnResource(j);
        return result;
	}

	public Long linsert(String key, LIST_POSITION where, String pivot,
			String value) {
		return linsert(standardDB, key, where, pivot, value);
	}
    public String get(String key) {
        return get(standardDB, redundancyFactor, key);
    }

    public String get(int db, int toTryCount, String key) {
        String result =null;
        Jedis j = null;
        if (toTryCount > 0) j = readPool.getResource();
        else j = writePool.getResource();
        try {
            j.select(db);
            result = j.get(key);
        } catch (Exception ex) {
            if (toTryCount > 0) result = get(db, toTryCount - 1, key);
            else ex.printStackTrace(); /*throw new Exception(ex);         */
        } finally {
            if (toTryCount > 0) readPool.returnResource(j);
            else writePool.returnResource(j);
        }
        return result;
    }

    public Boolean exists(String key) {
        return exists(standardDB, redundancyFactor, key);
    }

    public Boolean exists(int db, int toTryCount, String key) {
        Boolean result =null;
        Jedis j = null;
        if (toTryCount > 0) j = readPool.getResource();
        else j = writePool.getResource();
        try {
            j.select(db);
            result = j.exists(key);
        } catch (Exception ex) {
            if (toTryCount > 0) result = exists(db, toTryCount - 1, key);
            else ex.printStackTrace(); /*throw new Exception(ex);         */
        } finally {
            if (toTryCount > 0) readPool.returnResource(j);
            else writePool.returnResource(j);
        }
        return result;
    }

    public String type(String key) {
        return type(standardDB, redundancyFactor, key);
    }

    public String type(int db, int toTryCount, String key) {
        String result =null;
        Jedis j = null;
        if (toTryCount > 0) j = readPool.getResource();
        else j = writePool.getResource();
        try {
            j.select(db);
            result = j.type(key);
        } catch (Exception ex) {
            if (toTryCount > 0) result = type(db, toTryCount - 1, key);
            else ex.printStackTrace(); /*throw new Exception(ex);         */
        } finally {
            if (toTryCount > 0) readPool.returnResource(j);
            else writePool.returnResource(j);
        }
        return result;
    }

    public Long expire(String key, int seconds) {
        return expire(standardDB, redundancyFactor, key, seconds);
    }

    public Long expire(int db, int toTryCount, String key, int seconds) {
        Jedis j = writePool.getResource();
        j.select(db);
        Long result = j.expire(key, seconds);
        writePool.returnResource(j);
        return result;
    }

    public Long expireAt(String key, long unixTime) {
        return expireAt(standardDB, redundancyFactor, key, unixTime);
    }

    public Long expireAt(int db, int toTryCount, String key, long unixTime) {
        Jedis j = writePool.getResource();
        j.select(db);
        Long result = j.expireAt(key, unixTime);
        writePool.returnResource(j);
        return result;
    }

    public Long ttl(String key) {
        return ttl(standardDB, redundancyFactor, key);
    }

    public Long ttl(int db, int toTryCount, String key) {
        Jedis j = writePool.getResource();
        j.select(db);
        Long result = j.ttl(key);
        writePool.returnResource(j);
        return result;
    }

    public boolean setbit(String key, long offset, boolean value) {
        return setbit(standardDB, redundancyFactor, key, offset, value);
    }

    public boolean setbit(int db, int toTryCount, String key, long offset, boolean value) {
        Jedis j = writePool.getResource();
        j.select(db);
        boolean result = j.setbit(key, offset, value);
        writePool.returnResource(j);
        return result;
    }

    public boolean getbit(String key, long offset) {
        return getbit(standardDB, redundancyFactor, key, offset);
    }

    public boolean getbit(int db, int toTryCount, String key, long offset) {
        boolean result = false;
        Jedis j = null;
        if (toTryCount > 0) j = readPool.getResource();
        else j = writePool.getResource();
        try {
            j.select(db);
            result = j.getbit(key, offset);
        } catch (Exception ex) {
            if (toTryCount > 0) result = getbit(db, toTryCount - 1, key, offset);
            else ex.printStackTrace(); /*throw new Exception(ex);         */
        } finally {
            if (toTryCount > 0) readPool.returnResource(j);
            else writePool.returnResource(j);
        }
        return result;
    }

    public long setrange(String key, long offset, String value) {
        return setrange(standardDB, redundancyFactor, key, offset, value);
    }

    public long setrange(int db, int toTryCount, String key, long offset, String value) {
        Jedis j = writePool.getResource();
        j.select(db);
        long result = j.setrange(key, offset, value);
        writePool.returnResource(j);
        return result;
    }

    public String getrange(String key, long startOffset, long endOffset) {
        return getrange(standardDB, redundancyFactor, key, startOffset, endOffset);
    }

    public String getrange(int db, int toTryCount, String key, long startOffset, long endOffset) {
        String result =null;
        Jedis j = null;
        if (toTryCount > 0) j = readPool.getResource();
        else j = writePool.getResource();
        try {
            j.select(db);
            result = j.getrange(key, startOffset, endOffset);
        } catch (Exception ex) {
            if (toTryCount > 0) result = getrange(db, toTryCount - 1, key, startOffset, endOffset);
            else ex.printStackTrace(); /*throw new Exception(ex);         */
        } finally {
            if (toTryCount > 0) readPool.returnResource(j);
            else writePool.returnResource(j);
        }
        return result;
    }

    public String getSet(String key, String value) {
        return getSet(standardDB, key, value);
    }

    public String getSet(int db, String key, String value) {
        Jedis j = writePool.getResource();
        j.select(db);
        String result = j.getSet(key, value);
        writePool.returnResource(j);
        return result;
    }

    public Long setnx(String key, String value) {
        return setnx(standardDB, key, value);
    }

    public Long setnx(int db, String key, String value) {
        Jedis j = writePool.getResource();
        j.select(db);
        Long result = j.setnx(key, value);
        writePool.returnResource(j);
        return result;
    }

    public String setex(String key, int seconds, String value) {
        return setex(standardDB, key, seconds, value);
    }

    public String setex(int db, String key, int seconds, String value) {
        Jedis j = writePool.getResource();
        j.select(db);
        String result = j.setex(key, seconds, value);
        writePool.returnResource(j);
        return result;
    }

    public Long decrBy(String key, long integer) {
        return decrBy(standardDB, key, integer);
    }

    public Long decrBy(int db, String key, long integer) {
        Jedis j = writePool.getResource();
        j.select(db);
        Long result = j.decrBy(key, integer);
        writePool.returnResource(j);
        return result;
    }

    public Long decr(String key) {
        return decr(standardDB, key);
    }

    public Long decr(int db, String key) {
        Jedis j = writePool.getResource();
        j.select(db);
        Long result = j.decr(key);
        writePool.returnResource(j);
        return result;
    }

    public Long incrBy(String key, long integer) {
        return incrBy(standardDB, key, integer);
    }

    public Long incrBy(int db, String key, long integer) {
        Jedis j = writePool.getResource();
        j.select(db);
        Long result = j.incrBy(key, integer);
        writePool.returnResource(j);
        return result;
    }

    public Long incr(String key) {
        return incr(standardDB, redundancyFactor, key);
    }

    public Long incr(int db, int toTryCount, String key) {
        Jedis j = writePool.getResource();
        j.select(db);
        Long result = j.incr(key);
        writePool.returnResource(j);
        return result;
    }

    public Long append(String key, String value) {
        return append(standardDB, redundancyFactor, key, value);
    }

    public Long append(int db, int toTryCount, String key, String value) {
        Jedis j = writePool.getResource();
        j.select(db);
        Long result = j.append(key, value);
        writePool.returnResource(j);
        return result;
    }

    public String substr(String key, int start, int end) {
        return substr(standardDB, redundancyFactor, key, start, end);
    }

    public String substr(int db, int toTryCount, String key, int start, int end) {
        Jedis j = writePool.getResource();
        j.select(db);
        String result = j.substr(key, start, end);
        writePool.returnResource(j);
        return result;
    }

    public Long hset(String key, String field, String value) {
        return hset(standardDB, redundancyFactor, key, field, value);
    }

    public Long hset(int db, int toTryCount, String key, String field, String value) {
        Jedis j = writePool.getResource();
        j.select(db);
        Long result = j.hset(key, field, value);
        writePool.returnResource(j);
        return result;
    }

    public String hget(String key, String field) {
        return hget(standardDB, redundancyFactor, key, field);
    }

    public String hget(int db, int toTryCount, String key, String field) {
        String result =null;
        Jedis j = null;
        if (toTryCount > 0) j = readPool.getResource();
        else j = writePool.getResource();
        try {
            j.select(db);
            result = j.hget(key, field);
        } catch (Exception ex) {
            if (toTryCount > 0) result = hget(db, toTryCount - 1, key, field);
            else ex.printStackTrace(); /*throw new Exception(ex);         */
        } finally {
            if (toTryCount > 0) readPool.returnResource(j);
            else writePool.returnResource(j);
        }
        return result;
    }

    public Long hsetnx(String key, String field, String value) {
        return hsetnx(standardDB, redundancyFactor, key, field, value);
    }

    public Long hsetnx(int db, int toTryCount, String key, String field, String value) {
        Jedis j = writePool.getResource();
        j.select(db);
        Long result = j.hsetnx(key, field, value);
        writePool.returnResource(j);
        return result;
    }

    public String hmset(String key, Map<String, String> hash) {
        return hmset(standardDB, redundancyFactor, key, hash);
    }

    public String hmset(int db, int toTryCount, String key, Map<String, String> hash) {
        Jedis j = writePool.getResource();
        j.select(db);
        String result = j.hmset(key, hash);
        writePool.returnResource(j);
        return result;
    }

    public List<String> hmget(String key, String... fields) {
        return hmget(standardDB, redundancyFactor, key, fields);
    }

    public List<String> hmget(int db, int toTryCount, String key, String... fields) {
        List<String> result =null;
        Jedis j = null;
        if (toTryCount > 0) j = readPool.getResource();
        else j = writePool.getResource();
        try {
            j.select(db);
            result = j.hmget(key, fields);
        } catch (Exception ex) {
            if (toTryCount > 0) result = hmget(db, toTryCount - 1, key, fields);
            else ex.printStackTrace(); /*throw new Exception(ex);         */
        } finally {
            if (toTryCount > 0) readPool.returnResource(j);
            else writePool.returnResource(j);
        }
        return result;
    }

    public Long hincrBy(String key, String field, long value) {
        return hincrBy(standardDB, redundancyFactor, key, field, value);
    }

    public Long hincrBy(int db, int toTryCount, String key, String field, long value) {
        Jedis j = writePool.getResource();
        j.select(db);
        Long result = j.hincrBy(key, field, value);
        writePool.returnResource(j);
        return result;
    }

    public Boolean hexists(String key, String field) {
        return hexists(standardDB, redundancyFactor, key, field);
    }

    public Boolean hexists(int db, int toTryCount, String key, String field) {
        Boolean result =null;
        Jedis j = null;
        if (toTryCount > 0) j = readPool.getResource();
        else j = writePool.getResource();
        try {
            j.select(db);
            result = j.hexists(key, field);
        } catch (Exception ex) {
            if (toTryCount > 0) result = hexists(db, toTryCount - 1, key, field);
            else ex.printStackTrace(); /*throw new Exception(ex);         */
        } finally {
            if (toTryCount > 0) readPool.returnResource(j);
            else writePool.returnResource(j);
        }
        return result;
    }

    public Long hdel(String key, String field) {
        return hdel(standardDB, redundancyFactor, key, field);
    }

    public Long hdel(int db, int toTryCount, String key, String field) {
        Jedis j = writePool.getResource();
        j.select(db);
        Long result = j.hdel(key, field);
        writePool.returnResource(j);
        return result;
    }

    public Long hlen(String key) {
        return hlen(standardDB, redundancyFactor, key);
    }

    public Long hlen(int db, int toTryCount, String key) {
        Long result =null;
        Jedis j = null;
        if (toTryCount > 0) j = readPool.getResource();
        else j = writePool.getResource();
        try {
            j.select(db);
            result = j.hlen(key);
        } catch (Exception ex) {
            if (toTryCount > 0) result = hlen(db, toTryCount - 1, key);
            else ex.printStackTrace(); /*throw new Exception(ex);         */
        } finally {
            if (toTryCount > 0) readPool.returnResource(j);
            else writePool.returnResource(j);
        }
        return result;
    }

    public Set<String> hkeys(String key) {
        return hkeys(standardDB, redundancyFactor, key);
    }

    public Set<String> hkeys(int db, int toTryCount, String key) {
        Set<String> result =null;
        Jedis j = null;
        if (toTryCount > 0) j = readPool.getResource();
        else j = writePool.getResource();
        try {
            j.select(db);
            result = j.hkeys(key);
        } catch (Exception ex) {
            if (toTryCount > 0) result = hkeys(db, toTryCount - 1, key);
            else ex.printStackTrace(); /*throw new Exception(ex);         */
        } finally {
            if (toTryCount > 0) readPool.returnResource(j);
            else writePool.returnResource(j);
        }
        return result;
    }

    public List<String> hvals(String key) {
        return hvals(standardDB, redundancyFactor, key);
    }

    public List<String> hvals(int db, int toTryCount, String key) {
        List<String> result =null;
        Jedis j = null;
        if (toTryCount > 0) j = readPool.getResource();
        else j = writePool.getResource();
        try {
            j.select(db);
            result = j.hvals(key);
        } catch (Exception ex) {
            if (toTryCount > 0) result = hvals(db, toTryCount - 1, key);
            else ex.printStackTrace(); /*throw new Exception(ex);         */
        } finally {
            if (toTryCount > 0) readPool.returnResource(j);
            else writePool.returnResource(j);
        }
        return result;
    }

    public Map<String, String> hgetAll(String key) {
        return hgetAll(standardDB, redundancyFactor, key);
    }

    public Map<String, String> hgetAll(int db, int toTryCount, String key) {
        Map<String, String> result =null;Jedis j = null;
        if (toTryCount > 0) j = readPool.getResource();
        else j = writePool.getResource();
        try {
            j.select(db);
            result = j.hgetAll(key);
        } catch (Exception ex) {
            if (toTryCount > 0) result = hgetAll(db, toTryCount - 1, key);
            else ex.printStackTrace(); /*throw new Exception(ex);         */
        } finally {
            if (toTryCount > 0) readPool.returnResource(j);
            else writePool.returnResource(j);
        }
        return result;
    }

    public Long rpush(String key, String string) {
        return rpush(standardDB, redundancyFactor, key, string);
    }

    public Long rpush(int db, int toTryCount, String key, String string) {
        Jedis j = writePool.getResource();
        j.select(db);
        Long result = j.rpush(key, string);
        writePool.returnResource(j);
        return result;
    }

    public Long lpush(String key, String string) {
        return lpush(standardDB, redundancyFactor, key, string);
    }

    public Long lpush(int db, int toTryCount, String key, String string) {
        Jedis j = writePool.getResource();
        j.select(db);
        Long result = j.lpush(key, string);
        writePool.returnResource(j);
        return result;
    }

    public Long llen(String key) {
        return llen(standardDB, redundancyFactor, key);
    }

    public Long llen(int db, int toTryCount, String key) {
        Long result =null;
        Jedis j = null;
        if (toTryCount > 0) j = readPool.getResource();
        else j = writePool.getResource();
        try {
            j.select(db);
            result = j.llen(key);
        } catch (Exception ex) {
            if (toTryCount > 0) result = llen(db, toTryCount - 1, key);
            else ex.printStackTrace(); /*throw new Exception(ex);         */
        } finally {
            if (toTryCount > 0) readPool.returnResource(j);
            else writePool.returnResource(j);
        }
        return result;
    }

    public List<String> lrange(String key, long start, long end) {
        return lrange(standardDB, redundancyFactor, key, start, end);
    }

    public List<String> lrange(int db, int toTryCount, String key, long start, long end) {
        List<String> result =null;
        Jedis j = null;
        if (toTryCount > 0) j = readPool.getResource();
        else j = writePool.getResource();
        try {
            j.select(db);
            result = j.lrange(key, start, end);
        } catch (Exception ex) {
            if (toTryCount > 0) result = lrange(db, toTryCount - 1, key, start, end);
            else ex.printStackTrace(); /*throw new Exception(ex);         */
        } finally {
            if (toTryCount > 0) readPool.returnResource(j);
            else writePool.returnResource(j);
        }
        return result;
    }


    public String ltrim(String key, long start, long end) {
        return ltrim(standardDB, redundancyFactor, key, start, end);
    }

    public String ltrim(int db, int toTryCount, String key, long start, long end) {
        Jedis j = writePool.getResource();
        j.select(db);
        String result = j.ltrim(key, start, end);
        writePool.returnResource(j);
        return result;
    }

    public String lindex(String key, long index) {
        return lindex(standardDB, redundancyFactor, key, index);
    }

    public String lindex(int db, int toTryCount, String key, long index) {
        String result =null;
        Jedis j = null;
        if (toTryCount > 0) j = readPool.getResource();
        else j = writePool.getResource();
        try {
            j.select(db);
            result = j.lindex(key, index);
        } catch (Exception ex) {
            if (toTryCount > 0) result = lindex(db, toTryCount - 1, key, index);
            else ex.printStackTrace(); /*throw new Exception(ex);         */
        } finally {
            if (toTryCount > 0) readPool.returnResource(j);
            else writePool.returnResource(j);
        }
        return result;
    }

    public String lset(String key, long index, String value) {
        return lset(standardDB, redundancyFactor, key, index, value);
    }

    public String lset(int db, int toTryCount, String key, long index, String value) {
        Jedis j = writePool.getResource();
        j.select(db);
        String result = j.lset(key, index, value);
        writePool.returnResource(j);
        return result;
    }

    public Long lrem(String key, long count, String value) {
        return lrem(standardDB, redundancyFactor, key, count, value);
    }

    public Long lrem(int db, int toTryCount, String key, long count, String value) {
        Jedis j = writePool.getResource();
        j.select(db);
        Long result = j.lrem(key, count, value);
        writePool.returnResource(j);
        return result;
    }

    public String lpop(String key) {
        return lpop(standardDB, redundancyFactor, key);
    }

    public String lpop(int db, int toTryCount, String key) {
        Jedis j = writePool.getResource();
        j.select(db);
        String result = j.lpop(key);
        writePool.returnResource(j);
        return result;
    }

    public String rpop(String key) {
        return rpop(standardDB, redundancyFactor, key);
    }

    public String rpop(int db, int toTryCount, String key) {
        Jedis j = writePool.getResource();
        j.select(db);
        String result = j.rpop(key);
        writePool.returnResource(j);
        return result;
    }

    public Long sadd(String key, String member) {
        return sadd(standardDB, redundancyFactor, key, member);
    }

    public Long sadd(int db, int toTryCount, String key, String member) {
        Jedis j = writePool.getResource();
        j.select(db);
        Long result = j.sadd(key, member);
        writePool.returnResource(j);
        return result;
    }

    public Set<String> smembers(String key) {
        return smembers(standardDB, redundancyFactor, key);
    }

    public Set<String> smembers(int db, int toTryCount, String key) {
        Set<String> result =null;
        Jedis j = null;
        if (toTryCount > 0) j = readPool.getResource();
        else j = writePool.getResource();
        try {
            j.select(db);
            result = j.smembers(key);
        } catch (Exception ex) {
            if (toTryCount > 0) result = smembers(db, toTryCount - 1, key);
            else ex.printStackTrace(); /*throw new Exception(ex);         */
        } finally {
            if (toTryCount > 0) readPool.returnResource(j);
            else writePool.returnResource(j);
        }
        return result;
    }

    public Long srem(String key, String member) {
        return srem(standardDB, redundancyFactor, key, member);
    }

    public Long srem(int db, int toTryCount, String key, String member) {
        Jedis j = writePool.getResource();
        j.select(db);
        Long result = j.srem(key, member);
        writePool.returnResource(j);
        return result;
    }

    public String spop(String key) {
        return spop(standardDB, redundancyFactor, key);
    }

    public String spop(int db, int toTryCount, String key) {
        Jedis j = writePool.getResource();
        j.select(db);
        String result = j.spop(key);
        writePool.returnResource(j);
        return result;
    }

    public Long scard(String key) {
        return scard(standardDB, redundancyFactor, key);
    }

    public Long scard(int db, int toTryCount, String key) {
        Long result =null;
        Jedis j = null;
        if (toTryCount > 0) j = readPool.getResource();
        else j = writePool.getResource();
        try {
            j.select(db);
            result = j.scard(key);
        } catch (Exception ex) {
            if (toTryCount > 0) result = scard(db, toTryCount - 1, key);
            else ex.printStackTrace(); /*throw new Exception(ex);         */
        } finally {
            if (toTryCount > 0) readPool.returnResource(j);
            else writePool.returnResource(j);
        }
        return result;
    }

    public Boolean sismember(String key, String member) {
        return sismember(standardDB, redundancyFactor, key, member);
    }

    public Boolean sismember(int db, int toTryCount, String key, String member) {
        Boolean result =null;
        Jedis j = null;
        if (toTryCount > 0) j = readPool.getResource();
        else j = writePool.getResource();
        try {
            j.select(db);
            result = j.sismember(key, member);
        } catch (Exception ex) {
            if (toTryCount > 0) result = sismember(db, toTryCount - 1, key, member);
            else ex.printStackTrace(); /*throw new Exception(ex);         */
        } finally {
            if (toTryCount > 0) readPool.returnResource(j);
            else writePool.returnResource(j);
        }
        return result;
    }

    public String srandmember(String key) {
        return srandmember(standardDB, redundancyFactor, key);
    }

    public String srandmember(int db, int toTryCount, String key) {
        String result =null;
        Jedis j = null;
        if (toTryCount > 0) j = readPool.getResource();
        else j = writePool.getResource();
        try {
            j.select(db);
            result = j.srandmember(key);
        } catch (Exception ex) {
            if (toTryCount > 0) result = srandmember(db, toTryCount - 1, key);
            else ex.printStackTrace(); /*throw new Exception(ex);         */
        } finally {
            if (toTryCount > 0) readPool.returnResource(j);
            else writePool.returnResource(j);
        }
        return result;
    }

    public Long zadd(String key, double score, String member) {
        return zadd(standardDB, redundancyFactor, key, score, member);
    }

    public Long zadd(int db, int toTryCount, String key, double score, String member) {
        Jedis j = writePool.getResource();
        j.select(db);
        Long result = j.zadd(key, score, member);
        writePool.returnResource(j);
        return result;
    }

    public Set<String> zrange(String key, int start, int end) {
        return zrange(standardDB, redundancyFactor, key, start, end);
    }

    public Set<String> zrange(int db, int toTryCount, String key, int start, int end) {
        Set<String> result =null;
        Jedis j = null;
        if (toTryCount > 0) j = readPool.getResource();
        else j = writePool.getResource();
        try {
            j.select(db);
            result = j.zrange(key, start, end);
        } catch (Exception ex) {
            if (toTryCount > 0) result = zrange(db, toTryCount - 1, key, start, end);
            else ex.printStackTrace(); /*throw new Exception(ex);         */
        } finally {
            if (toTryCount > 0) readPool.returnResource(j);
            else writePool.returnResource(j);
        }
        return result;
    }

    public Long zrem(String key, String member) {
        return zrem(standardDB, redundancyFactor, key, member);
    }

    public Long zrem(int db, int toTryCount, String key, String member) {
        Jedis j = writePool.getResource();
        j.select(db);
        Long result = j.zrem(key, member);
        writePool.returnResource(j);
        return result;
    }

    public Double zincrby(String key, double score, String member) {
        return zincrby(standardDB, redundancyFactor, key, score, member);
    }

    public Double zincrby(int db, int toTryCount, String key, double score, String member) {
        Jedis j = writePool.getResource();
        j.select(db);
        Double result = j.zincrby(key, score, member);
        writePool.returnResource(j);
        return result;
    }

    public Long zrank(String key, String member) {
        return zrank(standardDB, redundancyFactor, key, member);
    }

    public Long zrank(int db, int toTryCount, String key, String member) {
        Long result =null;
        Jedis j = null;
        if (toTryCount > 0) j = readPool.getResource();
        else j = writePool.getResource();
        try {
            j.select(db);
            result = j.zrank(key, member);
        } catch (Exception ex) {
            if (toTryCount > 0) result = zrank(db, toTryCount - 1, key, member);
            else ex.printStackTrace(); /*throw new Exception(ex);         */
        } finally {
            if (toTryCount > 0) readPool.returnResource(j);
            else writePool.returnResource(j);
        }
        return result;
    }

    public Long zrevrank(String key, String member) {
        return zrevrank(standardDB, redundancyFactor, key, member);
    }

    public Long zrevrank(int db, int toTryCount, String key, String member) {
        Long result =null;
        Jedis j = null;
        if (toTryCount > 0) j = readPool.getResource();
        else j = writePool.getResource();
        try {
            j.select(db);
            result = j.zrevrank(key, member);
        } catch (Exception ex) {
            if (toTryCount > 0) result = zrevrank(db, toTryCount - 1, key, member);
            else ex.printStackTrace(); /*throw new Exception(ex);         */
        } finally {
            if (toTryCount > 0) readPool.returnResource(j);
            else writePool.returnResource(j);
        }
        return result;
    }

    public Set<String> zrevrange(String key, int start, int end) {
        return zrevrange(standardDB, redundancyFactor, key, start, end);
    }

    public Set<String> zrevrange(int db, int toTryCount, String key, int start, int end) {
        Set<String> result =null;
        Jedis j = null;
        if (toTryCount > 0) j = readPool.getResource();
        else j = writePool.getResource();
        try {
            j.select(db);
            result = j.zrevrange(key, start, end);
        } catch (Exception ex) {
            if (toTryCount > 0) result = zrevrange(db, toTryCount - 1, key, start, end);
            else ex.printStackTrace(); /*throw new Exception(ex);         */
        } finally {
            if (toTryCount > 0) readPool.returnResource(j);
            else writePool.returnResource(j);
        }
        return result;
    }

    public Set<Tuple> zrangeWithScores(String key, int start, int end) {
        return zrangeWithScores(standardDB, redundancyFactor, key, start, end);
    }

    public Set<Tuple> zrangeWithScores(int db, int toTryCount, String key, int start, int end) {
        Set<Tuple> result =null;
        Jedis j = null;
        if (toTryCount > 0) j = readPool.getResource();
        else j = writePool.getResource();
        try {
            j.select(db);
            result = j.zrangeWithScores(key, start, end);
        } catch (Exception ex) {
            if (toTryCount > 0) result = zrangeWithScores(db, toTryCount - 1, key, start, end);
            else ex.printStackTrace(); /*throw new Exception(ex);         */
        } finally {
            if (toTryCount > 0) readPool.returnResource(j);
            else writePool.returnResource(j);
        }
        return result;
    }

    public Set<Tuple> zrevrangeWithScores(String key, int start, int end) {
        return zrevrangeWithScores(standardDB, redundancyFactor, key, start, end);
    }

    public Set<Tuple> zrevrangeWithScores(int db, int toTryCount, String key, int start, int end) {
        Set<Tuple> result =null;
        Jedis j = null;
        if (toTryCount > 0) j = readPool.getResource();
        else j = writePool.getResource();
        try {
            j.select(db);
            result = j.zrevrangeWithScores(key, start, end);
        } catch (Exception ex) {
            if (toTryCount > 0) result = zrevrangeWithScores(db, toTryCount - 1, key, start, end);
            else ex.printStackTrace(); /*throw new Exception(ex);         */
        } finally {
            if (toTryCount > 0) readPool.returnResource(j);
            else writePool.returnResource(j);
        }
        return result;
    }

    public Long zcard(String key) {
        return zcard(standardDB, redundancyFactor, key);
    }

    public Long zcard(int db, int toTryCount, String key) {
        Long result =null;
        Jedis j = null;
        if (toTryCount > 0) j = readPool.getResource();
        else j = writePool.getResource();
        try {
            j.select(db);
            result = j.zcard(key);
        } catch (Exception ex) {
            if (toTryCount > 0) result = zcard(db, toTryCount - 1, key);
            else ex.printStackTrace(); /*throw new Exception(ex);         */
        } finally {
            if (toTryCount > 0) readPool.returnResource(j);
            else writePool.returnResource(j);
        }
        return result;
    }

    public Double zscore(String key, String member) {
        return zscore(standardDB, redundancyFactor, key, member);
    }

    public Double zscore(int db, int toTryCount, String key, String member) {
        Double result =null;
        Jedis j = null;
        if (toTryCount > 0) j = readPool.getResource();
        else j = writePool.getResource();
        try {
            j.select(db);
            result = j.zscore(key, member);
        } catch (Exception ex) {
            if (toTryCount > 0) result = zscore(db, toTryCount - 1, key, member);
            else ex.printStackTrace(); /*throw new Exception(ex);         */
        } finally {
            if (toTryCount > 0) readPool.returnResource(j);
            else writePool.returnResource(j);
        }
        return result;
    }

    public List<String> sort(String key) {
        return sort(standardDB, redundancyFactor, key);
    }

    public List<String> sort(int db, int toTryCount, String key) {
        List<String> result =null;
        Jedis j = null;
        if (toTryCount > 0) j = readPool.getResource();
        else j = writePool.getResource();
        try {
            j.select(db);
            result = j.sort(key);
        } catch (Exception ex) {
            if (toTryCount > 0) result = sort(db, toTryCount - 1, key);
            else ex.printStackTrace(); /*throw new Exception(ex);         */
        } finally {
            if (toTryCount > 0) readPool.returnResource(j);
            else writePool.returnResource(j);
        }
        return result;
    }

    public List<String> sort(String key, SortingParams sortingParameters) {
        return sort(standardDB, key, sortingParameters);
    }

    public List<String> sort(int db, String key, SortingParams sortingParameters) {
        Jedis j = writePool.getResource();
        j.select(db);
        List<String> result = j.sort(key, sortingParameters);
        writePool.returnResource(j);
        return result;
    }

    public Long zcount(String key, double min, double max) {
        return zcount(standardDB, redundancyFactor, key, min, max);
    }

    public Long zcount(int db, int toTryCount, String key, double min, double max) {
        Long result =null;
        Jedis j = null;
        if (toTryCount > 0) j = readPool.getResource();
        else j = writePool.getResource();
        try {
            j.select(db);
            result = j.zcount(key, min, max);
        } catch (Exception ex) {
            if (toTryCount > 0) result = zcount(db, toTryCount - 1, key, min, max);
            else ex.printStackTrace(); /*throw new Exception(ex);         */
        } finally {
            if (toTryCount > 0) readPool.returnResource(j);
            else writePool.returnResource(j);
        }
        return result;
    }

    public Set<String> zrangeByScore(String key, double min, double max) {
        return zrangeByScore(standardDB, redundancyFactor, key, min, max);
    }

    public Set<String> zrangeByScore(int db, int toTryCount, String key, double min, double max) {
        Set<String> result =null;
        Jedis j = null;
        if (toTryCount > 0) j = readPool.getResource();
        else j = writePool.getResource();
        try {
            j.select(db);
            result = j.zrangeByScore(key, max, min);
        } catch (Exception ex) {
            if (toTryCount > 0) result = zrangeByScore(db, toTryCount - 1, key, min, max);
            else ex.printStackTrace(); /*throw new Exception(ex);         */
        } finally {
            if (toTryCount > 0) readPool.returnResource(j);
            else writePool.returnResource(j);
        }
        return result;
    }

    public Set<String> zrevrangeByScore(String key, double max, double min) {
        return zrevrangeByScore(standardDB, redundancyFactor, key, max, min);
    }

    public Set<String> zrevrangeByScore(int db, int toTryCount, String key, double max, double min) {
        Set<String> result =null;
        Jedis j = null;
        if (toTryCount > 0) j = readPool.getResource();
        else j = writePool.getResource();
        try {
            j.select(db);
            result = j.zrevrangeByScore(key, max, min);
        } catch (Exception ex) {
            if (toTryCount > 0) result = zrevrangeByScore(db, toTryCount - 1, key, max, min);
            else ex.printStackTrace(); /*throw new Exception(ex);         */
        } finally {
            if (toTryCount > 0) readPool.returnResource(j);
            else writePool.returnResource(j);
        }
        return result;
    }

    public Set<String> zrangeByScore(String key, double min, double max, int offset, int count) {
        return zrangeByScore(standardDB, redundancyFactor, key, min, max, offset, count);
    }

    public Set<String> zrangeByScore(int db, int toTryCount, String key, double min, double max, int offset, int count) {
        Set<String> result =null;
        Jedis j = null;
        if (toTryCount > 0) j = readPool.getResource();
        else j = writePool.getResource();
        try {
            j.select(db);
            result = j.zrangeByScore(key, max, min, offset, count);
        } catch (Exception ex) {
            if (toTryCount > 0) result = zrangeByScore(db, toTryCount - 1, key, min, max);
            else ex.printStackTrace(); /*throw new Exception(ex);         */
        } finally {
            if (toTryCount > 0) readPool.returnResource(j);
            else writePool.returnResource(j);
        }
        return result;
    }

    public Set<String> zrevrangeByScore(String key, double max, double min, int offset, int count) {
        return zrevrangeByScore(standardDB, redundancyFactor, key, max, min, offset, count);
    }

    public Set<String> zrevrangeByScore(int db, int toTryCount, String key, double max, double min, int offset, int count) {
        Set<String> result =null;
        Jedis j = null;
        if (toTryCount > 0) j = readPool.getResource();
        else j = writePool.getResource();
        try {
            j.select(db);
            result = j.zrevrangeByScore(key, max, min, offset, count);
        } catch (Exception ex) {
            if (toTryCount > 0) result = zrevrangeByScore(db, toTryCount - 1, key, max, min, offset, count);
            else ex.printStackTrace(); /*throw new Exception(ex);         */
        } finally {
            if (toTryCount > 0) readPool.returnResource(j);
            else writePool.returnResource(j);
        }
        return result;
    }

    public Set<Tuple> zrangeByScoreWithScores(String key, double min, double max) {
        return zrangeByScoreWithScores(standardDB, redundancyFactor, key, min, max);
    }

    public Set<Tuple> zrangeByScoreWithScores(int db, int toTryCount, String key, double min, double max) {
        Set<Tuple> result =null;
        Jedis j = null;
        if (toTryCount > 0) j = readPool.getResource();
        else j = writePool.getResource();
        try {
            j.select(db);
            result = j.zrangeByScoreWithScores(key, min, max);
        } catch (Exception ex) {
            if (toTryCount > 0) result = zrangeByScoreWithScores(db, toTryCount - 1, key, min, max);
            else ex.printStackTrace(); /*throw new Exception(ex);         */
        } finally {
            if (toTryCount > 0) readPool.returnResource(j);
            else writePool.returnResource(j);
        }
        return result;
    }

    public Set<Tuple> zrevrangeByScoreWithScores(String key, double max, double min) {
        return zrevrangeByScoreWithScores(standardDB, redundancyFactor, key, max, min);
    }

    public Set<Tuple> zrevrangeByScoreWithScores(int db, int toTryCount, String key, double max, double min) {
        Set<Tuple> result =null;
        Jedis j = null;
        if (toTryCount > 0) j = readPool.getResource();
        else j = writePool.getResource();
        try {
            j.select(db);
            result = j.zrevrangeByScoreWithScores(key, max, min);
        } catch (Exception ex) {
            if (toTryCount > 0) result = zrevrangeByScoreWithScores(db, toTryCount - 1, key, max,min);
            else ex.printStackTrace(); /*throw new Exception(ex);         */
        } finally {
            if (toTryCount > 0) readPool.returnResource(j);
            else writePool.returnResource(j);
        }
        return result;
    }

    public Set<Tuple> zrangeByScoreWithScores(String key, double min, double max, int offset, int count) {
        return zrangeByScoreWithScores(standardDB, redundancyFactor, key, min, max, offset, count);
    }

    public Set<Tuple> zrangeByScoreWithScores(int db, int toTryCount, String key, double min, double max, int offset, int count) {
        Set<Tuple> result =null;
        Jedis j = null;
        if (toTryCount > 0) j = readPool.getResource();
        else j = writePool.getResource();
        try {
            j.select(db);
            result = j.zrangeByScoreWithScores(key, min, max, offset, count);
        } catch (Exception ex) {
            if (toTryCount > 0) result = zrangeByScoreWithScores(db, toTryCount - 1, key, min, max, offset, count);
            else ex.printStackTrace(); /*throw new Exception(ex);         */
        } finally {
            if (toTryCount > 0) readPool.returnResource(j);
            else writePool.returnResource(j);
        }
        return result;

    }

    public Set<Tuple> zrevrangeByScoreWithScores(String key, double max, double min, int offset, int count) {
        return zrevrangeByScoreWithScores(standardDB, redundancyFactor, key, max, min, offset, count);
    }

    public Set<Tuple> zrevrangeByScoreWithScores(int db, int toTryCount, String key, double max, double min, int offset, int count) {
        Set<Tuple> result =null;
        Jedis j = null;
        if (toTryCount > 0) j = readPool.getResource();
        else j = writePool.getResource();
        try {
            j.select(db);
            result = j.zrevrangeByScoreWithScores(key, max, min, offset, count);
        } catch (Exception ex) {
            if (toTryCount > 0) result = zrevrangeByScoreWithScores(db, toTryCount - 1, key, max, min, offset, count);
            else ex.printStackTrace(); /*throw new Exception(ex);         */
        } finally {
            if (toTryCount > 0) readPool.returnResource(j);
            else writePool.returnResource(j);
        }
        return result;
    }

    public Long zremrangeByRank(String key, int start, int end) {
        return zremrangeByRank(standardDB, redundancyFactor, key, start, end);
    }

    public Long zremrangeByRank(int db, int toTryCount, String key, int start, int end) {
        Jedis j = writePool.getResource();
        j.select(db);
        Long result = j.zremrangeByRank(key, start, end);
        writePool.returnResource(j);
        return result;
    }

    public Long zremrangeByScore(String key, double start, double end) {
        return zremrangeByScore(standardDB, redundancyFactor, key, start, end);
    }

    public Long zremrangeByScore(int db, int toTryCount, String key, double start, double end) {
        Jedis j = writePool.getResource();
        j.select(db);
        Long result = j.zremrangeByScore(key, start, end);
        writePool.returnResource(j);
        return result;
    }

    public String set(byte[] key, byte[] value) {
        return set(standardDB, redundancyFactor, key, value);
    }

    public String set(int db, int toTryCount, byte[] key, byte[] value) {
        Jedis j = writePool.getResource();
        j.select(db);
        String result = j.set(key, value);
        writePool.returnResource(j);
        return result;
    }

    public String set(String key, String value) {
        return set(standardDB, redundancyFactor, key, value);
    }

    public String set(int db, int toTryCount, String key, String value) {
        Jedis j = writePool.getResource();
        j.select(db);
        j.set(key, value);
        writePool.returnResource(j);
        return key;
    }

    public void zadd(byte[] key, int score, byte[] value) {
        zadd(standardDB, redundancyFactor, key, score, value);
    }

    public void zadd(int db, int toTryCount, byte[] key, int score, byte[] value) {
        Jedis j = writePool.getResource();
        j.select(db);
        j.zadd(key, score, value);
        writePool.returnResource(j);
    }

    public void zaddSet(byte[] key, Set<byte[]> values) {
        zaddSet(standardDB, redundancyFactor, key, values);
    }

    public void zaddSet(int db, int toTryCount, byte[] key, Set<byte[]> values) {
        Jedis j = writePool.getResource();
        j.select(db);
        j.watch(key);
        Transaction t = j.multi();
        for (byte[] ba : values)
            t.zadd(key, 0, ba);
        t.exec();
        writePool.returnResource(j);
    }

    public void zaddSet(String key, Set<String> values) {
        zaddSet(standardDB, redundancyFactor, key, values);
    }

    public void zaddSet(int db, int toTryCount, String key, Set<String> values) {
        byte[] keyBA = SafeEncoder.encode(key);
        Jedis j = writePool.getResource();
        j.select(db);
        j.watch(keyBA);
        Transaction t = j.multi();
        for (String s : values)
            t.zadd(keyBA, 0, SafeEncoder.encode(s));
        t.exec();
        writePool.returnResource(j);
    }

    public String zelementAtScore(String key, int score) {
        return zelementAtScore(standardDB, key, score);
    }

    public String zelementAtScore(int db,  String key, int score) {
        byte[] temp = zelementAtScore(db, redundancyFactor, SafeEncoder.encode(key), score);
        if (temp == null)
            return null;
        else
            return SafeEncoder.encode(temp);
    }

    public byte[] zelementAtScore(byte[] keyBA, int redundancyFactor, int score) {
        return zelementAtScore(standardDB, redundancyFactor, keyBA, score);
    }

    public byte[] zelementAtScore(int db, int toTryCount, byte[] keyBA, int score) {
        byte[] result = null;
        Jedis j = null;
        if (toTryCount > 0) j = readPool.getResource();
        else j = writePool.getResource();
        try {
            j.select(db);
            Set<byte[]> temp = j.zrange(keyBA, score, score);
            if (!(temp.isEmpty()) && !(temp.size() > 1))
                result = temp.iterator().next();
        } catch (Exception ex) {
            if (toTryCount > 0) result = zelementAtScore(db, toTryCount - 1, keyBA, score);
            else ex.printStackTrace(); /*throw new Exception(ex);         */
        } finally {
            if (toTryCount > 0) readPool.returnResource(j);
            else writePool.returnResource(j);
        }
        return result;
    }

    public void destroy() {
        writePool.destroy();
        readPool.destroy();
    }

    public void returnReadJedis(Jedis jedis) {
        readPool.returnResource(jedis);
    }

    public void returnWriteJedis(Jedis jedis) {
        writePool.returnResource(jedis);
    }


    public Set<byte[]> keys(byte[] bbarstar) {
        return keys(standardDB, redundancyFactor, bbarstar);
    }

    public Set<byte[]> keys(int db, int toTryCount, byte[] bbarstar) {
        Set<byte[]> result =null;
        Jedis j = null;
        if (toTryCount > 0) j = readPool.getResource();
        else j = writePool.getResource();
        try {
            j.select(db);
            result = j.keys(bbarstar);
        } catch (Exception ex) {
            if (toTryCount > 0) result = keys(db, toTryCount - 1, bbarstar);
            else ex.printStackTrace(); /*throw new Exception(ex);         */
        } finally {
            if (toTryCount > 0) readPool.returnResource(j);
            else writePool.returnResource(j);
        }
        return result;
    }

    public Set<String> keys(String bbarstar) {
        return keys(standardDB, redundancyFactor, bbarstar);
    }

    public Set<String> keys(int db, int toTryCount, String bbarstar) {
        Set<String> result =null;
        Jedis j = null;
        if (toTryCount > 0) j = readPool.getResource();
        else j = writePool.getResource();
        try {
            j.select(db);
            result = j.keys(bbarstar);
        } catch (Exception ex) {
            if (toTryCount > 0) result = keys(db, toTryCount - 1, bbarstar);
            else ex.printStackTrace(); /*throw new Exception(ex);         */
        } finally {
            if (toTryCount > 0) readPool.returnResource(j);
            else writePool.returnResource(j);
        }
        return result;
    }

    public String randomKey() {
        return randomKey(redundancyFactor);
    }

    public String randomKey(int toTryCount) {
        String result =null;
        Jedis j = null;
        if (toTryCount > 0) j = readPool.getResource();
        else j = writePool.getResource();
        try {
            result = j.randomKey();
        } catch (Exception ex) {
            if (toTryCount > 0) result = randomKey(toTryCount - 1);
            else ex.printStackTrace(); /*throw new Exception(ex);         */
        } finally {
            if (toTryCount > 0) readPool.returnResource(j);
            else writePool.returnResource(j);
        }
        return result;
    }

    public byte[] randomBinaryKey() {
        return randomBinaryKey(redundancyFactor);
    }

    public byte[] randomBinaryKey(int toTryCount) {
        byte[] result =null;
        Jedis j = null;
        if (toTryCount > 0) j = readPool.getResource();
        else j = writePool.getResource();
        try {
            result = j.randomBinaryKey();
        } catch (Exception ex) {
            if (toTryCount > 0) result = randomBinaryKey(toTryCount - 1);
            else ex.printStackTrace(); /*throw new Exception(ex);         */
        } finally {
            if (toTryCount > 0) readPool.returnResource(j);
            else writePool.returnResource(j);
        }
        return result;
    }

    public String rename(byte[] bfoo, byte[] bbar) {
        return rename(standardDB, bfoo, bbar);
    }

    public String rename(int db, byte[] bfoo, byte[] bbar) {
        Jedis j = writePool.getResource();
        j.select(db);
        String result = j.rename(bfoo, bbar);
        writePool.returnResource(j);
        return result;
    }

    public String rename(String oldkey, String newkey) {
        return rename(standardDB, oldkey, newkey);
    }

    public String rename(int db, String oldkey, String newkey) {
        Jedis j = writePool.getResource();
        j.select(db);
        String result = j.rename(oldkey, newkey);
        writePool.returnResource(j);
        return result;
    }

    public Long renamenx(byte[] bfoo, byte[] bbar) {
        return renamenx(standardDB, bfoo, bbar);
    }

    public Long renamenx(int db, byte[] bfoo, byte[] bbar) {
        Jedis j = writePool.getResource();
        j.select(db);
        Long result = j.renamenx(bfoo, bbar);
        writePool.returnResource(j);
        return result;
    }

    public Long renamenx(String oldkey, String newkey) {
        return renamenx(standardDB, oldkey, newkey);
    }

    public Long renamenx(int db, String oldkey, String newkey) {
        Jedis j = writePool.getResource();
        j.select(db);
        Long result = j.renamenx(oldkey, newkey);
        writePool.returnResource(j);
        return result;
    }

    public Long dbSize() {
        return dbSize(redundancyFactor);
    }

    public Long dbSize(int toTryCount) {
        Long result =null;
        Jedis j = null;
        if (toTryCount > 0) j = readPool.getResource();
        else j = writePool.getResource();
        try {
            result = j.dbSize();
        } catch (Exception ex) {
            if (toTryCount > 0) result = dbSize(toTryCount - 1);
            else ex.printStackTrace(); /*throw new Exception(ex);         */
        } finally {
            if (toTryCount > 0) readPool.returnResource(j);
            else writePool.returnResource(j);
        }
        return result;
    }

    public Long move(String foo, int fromDB, int toDB) {
        Jedis j = writePool.getResource();
        j.select(fromDB);
        Long result = j.move(foo, toDB);
        writePool.returnResource(j);
        return result;
    }

    public Long move(byte[] foo, int fromDB, int toDB) {
        Jedis j = writePool.getResource();
        j.select(fromDB);
        Long result = j.move(foo, toDB);
        writePool.returnResource(j);
        return result;
    }

    public String flushDB() {
        return flushDB(standardDB);
    }

    public String flushDB(int db) {
        Jedis j = writePool.getResource();
        j.select(db);
        String result = j.flushDB();
        writePool.returnResource(j);
        return result;
    }

    public String flushAll() {
        Jedis j = writePool.getResource();
        String result = j.flushAll();
        writePool.returnResource(j);
        return result;
    }

    public long persist(byte[] key) {
        return persist(standardDB, key);
    }

    public long persist(int db,byte[] key) {
        Jedis j = writePool.getResource();
        j.select(db);
        Long result = j.persist(key);
        writePool.returnResource(j);
        return result;
    }

    public long persist(String key) {
        return persist(standardDB, key);
    }

    public long persist(int db, String key) {
        Jedis j = writePool.getResource();
        j.select(db);
        Long result = j.persist(key);
        writePool.returnResource(j);
        return result;
    }

    public byte[] echo(byte[] echo) {
        Jedis j = writePool.getResource();
        byte[] result = j.echo(echo);
        writePool.returnResource(j);
        return result;
    }

    public String echo(String echo) {
        Jedis j = writePool.getResource();
        String result = j.echo(echo);
        writePool.returnResource(j);
        return result;
    }

    public void disconnect() {
        readPool.destroy();
        writePool.destroy();
}

    // Getters and Setters

    public Pool<Jedis> getWritePool() {
        return writePool;
    }


    public int getStandardDB() {
        return standardDB;
    }

    public Pool<Jedis> getReadPool() {
        return readPool;
    }



}
