package com.github.ddth.lucext.qnd.cassandra;

import org.slf4j.LoggerFactory;

import com.github.ddth.cacheadapter.ICacheFactory;
import com.github.ddth.cacheadapter.cacheimpl.redis.RedisCacheFactory;
import com.github.ddth.commons.redis.JedisConnector;
import com.github.ddth.cql.SessionManager;
import com.github.ddth.lucext.directory.LucextDirectory;
import com.github.ddth.lucext.directory.cassandra.CassandraDirectory;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import redis.clients.jedis.Jedis;

public class BaseQndCassandra {

    static {
        System.setProperty("org.slf4j.simpleLogger.logFile", "System.out");
        System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "TRACE");
        System.setProperty("org.slf4j.simpleLogger.showThreadName", "false");
        System.setProperty("org.slf4j.simpleLogger.showLogName", "false");
        System.setProperty("org.slf4j.simpleLogger.showShortLogName", "false");
    }

    public static void initLoggers(Level level) {
        {
            Logger logger = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
            logger.setLevel(Level.ERROR);
        }
        {
            Logger logger = (Logger) LoggerFactory.getLogger(LucextDirectory.class);
            logger.setLevel(level);
        }
        {
            Logger logger = (Logger) LoggerFactory.getLogger(CassandraDirectory.class);
            logger.setLevel(level);
        }
        {
            Logger logger = (Logger) LoggerFactory
                    .getLogger(LucextDirectory.class.getName() + ".LucextIndexInput");
            logger.setLevel(level);
        }
        {
            Logger logger = (Logger) LoggerFactory
                    .getLogger(LucextDirectory.class.getName() + ".LucextIndexInput");
            logger.setLevel(level);
        }
        {
            Logger logger = (Logger) LoggerFactory
                    .getLogger(LucextDirectory.class.getName() + ".LucextLock");
            logger.setLevel(level);
        }
    }

    protected static SessionManager getSessionManager() {
        SessionManager sm = new SessionManager();
        sm.setDefaultHostsAndPorts("localhost").setDefaultKeyspace(null).init();
        return sm;
    }

    protected static JedisConnector getJedisConnector() {
        JedisConnector jc = new JedisConnector();
        jc.setRedisHostsAndPorts("localhost:6379").init();
        return jc;
    }

    private static final boolean cacheEnabled = false;

    protected static ICacheFactory getCacheFactory(boolean flushCache) {
        if (!cacheEnabled) {
            return null;
        }
        JedisConnector jc = getJedisConnector();
        if (flushCache) {
            try (Jedis jedis = jc.getJedis()) {
                jedis.flushAll();
            }
        }
        RedisCacheFactory cf = new RedisCacheFactory();
        cf.setJedisConnector(jc);
        cf.init();
        return cf;
    }
}
