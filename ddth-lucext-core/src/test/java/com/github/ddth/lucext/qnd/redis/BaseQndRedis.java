package com.github.ddth.lucext.qnd.redis;

import org.slf4j.LoggerFactory;

import com.github.ddth.commons.redis.JedisConnector;
import com.github.ddth.lucext.directory.LucextDirectory;
import com.github.ddth.lucext.directory.cassandra.CassandraDirectory;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;

public class BaseQndRedis {

    static {
        System.setProperty("org.slf4j.simpleLogger.logFile", "System.out");
        System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "INFO");
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

    protected static JedisConnector getJedisConnector() {
        JedisConnector jc = new JedisConnector();
        jc.setRedisHostsAndPorts("localhost:6379").init();
        return jc;
    }
}
