package redis.clients.jedis;

import redis.clients.util.ShardInfo;

public class UniJedisShardInfo extends ShardInfo<UniJedis>  {
	private final UniJedis uniJedis;
	
	public UniJedisShardInfo(final UniJedis uniJedis) {
		super(1);
		this.uniJedis = uniJedis;
	}
		
	@Override
	protected UniJedis createResource() {
		return uniJedis;
	}

	@Override
	public String getName() {
		return null;
	}

}
