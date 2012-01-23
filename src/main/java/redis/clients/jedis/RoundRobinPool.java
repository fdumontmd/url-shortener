/*
 * derived from http://groups.google.com/group/jedis_redis/msg/c8c76371cf543e36
 * Initial implementation by Ingvar Bogdahn
 */

package redis.clients.jedis;

import org.apache.commons.pool.BasePoolableObjectFactory;
import org.apache.commons.pool.PoolableObjectFactory;
import org.apache.commons.pool.impl.GenericObjectPool;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisException;
import redis.clients.util.Pool;

import java.util.Iterator;
import java.util.List;

public class RoundRobinPool extends Pool<Jedis>{
        private GenericObjectPool internalPool;

    public RoundRobinPool(final GenericObjectPool.Config poolConfig, String masterIP, int masterPort, String masterPassword, List<JedisShardInfo> shards) {
        super( poolConfig, null);
        this.internalPool = new GenericObjectPool(new RoundRobinFactory(shards, masterIP, masterPort, masterPassword), poolConfig);
        internalPool.setLifo(false);
        internalPool.setMaxWait(100);
        initializePool();
    }


    public RoundRobinPool(final GenericObjectPool.Config poolConfig, PoolableObjectFactory factory) {
        super(poolConfig, factory);
    }

    private void initializePool() {
         for (int i = RoundRobinFactory.shards.size(); i > 0; i--)
            try {
                internalPool.addObject();
            } catch (Exception e) {
                e.printStackTrace();
            }
    }

     public void addSlaveToRoundRobin(JedisShardInfo... jsi) throws Exception {
        for(JedisShardInfo i: jsi) {
            RoundRobinFactory.makeSlaveOfMaster(i,RoundRobinFactory.masterIP, RoundRobinFactory.masterPort, RoundRobinFactory.masterPassword);
            RoundRobinFactory.shards.add(i);
        }
        RoundRobinFactory.shardIterator = RoundRobinFactory.shards.iterator();
//        internalPool.addObject();
        initializePool();
    }

    public void setWhenExhaustedGrow(boolean whenExhaustedGrow) {this.internalPool.setWhenExhaustedAction(GenericObjectPool.WHEN_EXHAUSTED_GROW);}

    public void setMinIdle(int minIdle) {internalPool.setMinIdle(minIdle);}
    public void setMaxIdle(int maxIdle) {internalPool.setMaxIdle(maxIdle);}

    @SuppressWarnings("unchecked")
    public Jedis getResource() {
        try {
            return (Jedis) internalPool.borrowObject();
        } catch (Exception e) {
            throw new JedisConnectionException(
                    "Could not get a resource from the pool", e);
        }
    }

    public RoundRobinPool chainGetResource(Jedis result) {
          try {
              result = (Jedis) internalPool.borrowObject();
          } catch (Exception e) {
              throw new JedisConnectionException(
                      "Could not get a resource from the pool", e);
          }
        return this;
      }

    public void returnResource(final Jedis resource) {
        try {
            internalPool.returnObject(resource);
        } catch (Exception e) {
            throw new JedisException(
                    "Could not return the resource to the pool", e);
        }
    }

      public void chainReturnResource(final Jedis resource, Pool pool) {
        try {
            internalPool.returnObject(resource);
        } catch (Exception e) {
            throw new JedisException(
                    "Could not return the resource to the pool", e);
        }
    }

    public void returnBrokenResource(final Jedis resource) {
        try {
            internalPool.invalidateObject(resource);
        } catch (Exception e) {
            throw new JedisException(
                    "Could not return the resource to the pool", e);
        }
    }

    public void destroy() {
        try {
            internalPool.close();
        } catch (Exception e) {
            throw new JedisException("Could not destroy the pool", e);
        }
    }

    public void setTestOnBorrow(boolean stob) {
        internalPool.setTestOnBorrow(stob);
    }

    public void setTestOnReturn(boolean stor) {
        internalPool.setTestOnReturn(stor);
    }

    private static class RoundRobinFactory extends BasePoolableObjectFactory {
        private static List<JedisShardInfo> shards;             // TODO - have checked if setting these 2 fields static is ok
        private static Iterator<JedisShardInfo> shardIterator;
        private static String masterIP;
        private static int masterPort;
        private static String masterPassword;

        public void addSlave(JedisShardInfo jsi) {
            shards.add(jsi);
            shardIterator = shards.iterator();
        }


        public RoundRobinFactory(List<JedisShardInfo> shards, String masterIP, int masterPort, String masterPassword) {
            this.shards = shards;
            this.masterIP = masterIP;
            this.masterPort = masterPort;
            this.masterPassword = masterPassword;

            for (JedisShardInfo jsi : shards)
                makeSlaveOfMaster(jsi, masterIP, masterPort, masterPassword);

            this.shardIterator = this.shards.iterator();
        }

        public static void makeSlaveOfMaster(JedisShardInfo s, String masterIP, int masterPort, String masterPasswort) {
            Jedis temp = s.createResource();
            temp.connect();
            temp.auth(masterPasswort);
            temp.slaveof(masterIP, masterPort);
            temp.disconnect();
        }

        public Object makeObject() throws Exception {
            JedisShardInfo jsi = null;
            if (shardIterator.hasNext())
                jsi = (JedisShardInfo) shardIterator.next();
            else {
                shardIterator = this.shards.iterator();
                if (shardIterator.hasNext()) {
                    jsi = (JedisShardInfo) shardIterator.next();
                }
            }
            return new Jedis(jsi.getHost(), jsi.getPort());
        }

        public void destroyObject(final Object obj) throws Exception {
            if ((obj != null) && (obj instanceof Jedis)) {
                Jedis jedis = (Jedis) obj;
                try {
                    try {
                        jedis.quit();
                    } catch (Exception e) {
                    }
                    jedis.disconnect();
                } catch (Exception e) {
                }
            }
        }

        public boolean validateObject(final Object obj) {
            try {
                Jedis jedis = ((Jedis) obj);
                if (!jedis.ping().equals("PONG")) {
                    return false;
                }
                return true;
            } catch (Exception ex) {
                return false;
            }
        }
    }
}
