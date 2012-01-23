/*
 * derived from http://groups.google.com/group/jedis_redis/msg/c8c76371cf543e36
 * Initial implementation by Ingvar Bogdahn
 */

package redis.clients.jedis;

public class DBKeys {
	public final int db;
	public final String[] keys;
	
	public DBKeys(final int db, final String...keys) {
		this.db = db;
		this.keys = keys;
	}
}
