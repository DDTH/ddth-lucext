package com.github.ddth.lucext.directory.redis;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.Lock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.ddth.commons.redis.JedisConnector;
import com.github.ddth.lucext.directory.FileInfo;
import com.github.ddth.lucext.directory.LucextDirectory;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

/**
 * Redis implementation of {@link Directory}.
 * 
 * <p>
 * Design:
 * <ul>
 * <li>A Redis hash (named {@link #hashDirectoryMetadata}) to store directory metadata (file info:
 * {@code id, name, size}), each file info is stored in a field keyed by file name.</li>
 * <li>Data of each file is stored in a Redis hash, where:
 * <ul>
 * <li>File's id is hash name.</li>
 * <li>File's data is divided into {@link #getBlockSize()}-byte chunks. Data of each chunk is stored
 * in one hash's field, keyed by chunk's index (0, 1, 2 and so on).</li>
 * </ul>
 * </li>
 * </ul>
 * </p>
 * 
 * @author Thanh Nguyen <btnguyen2k@gmail.com>
 * @since 0.1.0
 */
public class RedisDirectory extends LucextDirectory {

    private Logger LOGGER = LoggerFactory.getLogger(RedisDirectory.class);

    public final static String DEFAULT_HASH_DIRECTORY_METADATA = "_directory_metadata_";

    private String strHashDirectoryMetadata = DEFAULT_HASH_DIRECTORY_METADATA;
    private byte[] hashDirectoryMetadata = strHashDirectoryMetadata
            .getBytes(StandardCharsets.UTF_8);

    private JedisConnector jedisConnector;

    public RedisDirectory(JedisConnector jedisConnector) {
        this.jedisConnector = jedisConnector;
    }

    protected JedisConnector getJedisConnector() {
        return jedisConnector;
    }

    /**
     * Name of Redis hash to store directory metadata, default value
     * {@link #DEFAULT_HASH_DIRECTORY_METADATA}.
     * 
     * @return
     */
    public String getHashDirectoryMetadata() {
        return strHashDirectoryMetadata;
    }

    /**
     * Name of Redis hash to store directory metadata, default value
     * {@link #DEFAULT_HASH_DIRECTORY_METADATA}.
     * 
     * @param strHashDirectoryMetadata
     * @return
     */
    public RedisDirectory setHashDirectoryMetadata(String strHashDirectoryMetadata) {
        this.strHashDirectoryMetadata = StringUtils.isBlank(strHashDirectoryMetadata)
                ? DEFAULT_HASH_DIRECTORY_METADATA
                : strHashDirectoryMetadata;
        this.hashDirectoryMetadata = strHashDirectoryMetadata.getBytes(StandardCharsets.UTF_8);
        return this;
    }

    /*----------------------------------------------------------------------*/

    private byte[] dataKeyFor(FileInfo fileInfo) {
        return fileInfo.getId().getBytes(StandardCharsets.UTF_8);
    }

    private byte[] metadataKeyFor(FileInfo fileInfo) {
        return fileInfo.getName().getBytes(StandardCharsets.UTF_8);
    }

    private byte[] metadataKeyFor(String fileName) {
        return fileName.getBytes(StandardCharsets.UTF_8);
    }

    private Jedis getJedis() {
        return jedisConnector.getJedis();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected FileInfo getFileInfo(String name) {
        byte[] FIELD = metadataKeyFor(name);
        try (Jedis jedis = getJedis()) {
            byte[] dataArr = jedis.hget(hashDirectoryMetadata, FIELD);
            return FileInfo.newInstance(dataArr);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void removeFileInfo(FileInfo fileInfo) {
        byte[] FIELD = metadataKeyFor(fileInfo);
        try (Jedis jedis = getJedis()) {
            jedis.hdel(hashDirectoryMetadata, FIELD);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected FileInfo updateFileInfo(FileInfo fileInfo) {
        if (LOGGER.isTraceEnabled()) {
            String logMsg = "updateFile(" + fileInfo.getName() + "/" + fileInfo.getId() + "/"
                    + fileInfo.getSize() + ") is called";
            LOGGER.trace(logMsg);
        }
        byte[] FIELD = metadataKeyFor(fileInfo);
        try (Jedis jedis = getJedis()) {
            jedis.hset(hashDirectoryMetadata, FIELD, fileInfo.asBytes());
            return fileInfo;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected List<FileInfo> getAllFileInfo() {
        try (Jedis jedis = getJedis()) {
            List<FileInfo> result = new ArrayList<>();
            Map<byte[], byte[]> allFilesMap = jedis.hgetAll(hashDirectoryMetadata);
            if (allFilesMap != null) {
                allFilesMap.forEach((k, data) -> {
                    FileInfo fileInfo = FileInfo.newInstance(data);
                    if (fileInfo != null) {
                        result.add(fileInfo);
                    }
                });
            }
            return result;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected byte[] readFileBlock(FileInfo fileInfo, int blockNum) {
        byte[] KEY = dataKeyFor(fileInfo);
        byte[] FIELD = String.valueOf(blockNum).getBytes(StandardCharsets.UTF_8);
        try (Jedis jedis = getJedis()) {
            byte[] dataArr = jedis.hget(KEY, FIELD);
            return dataArr != null
                    ? (dataArr.length >= getBlockSize() ? dataArr
                            : Arrays.copyOf(dataArr, getBlockSize()))
                    : null;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void writeFileBlock(FileInfo fileInfo, int blockNum, byte[] data) {
        byte[] KEY = dataKeyFor(fileInfo);
        byte[] FIELD = String.valueOf(blockNum).getBytes(StandardCharsets.UTF_8);
        try (Jedis jedis = getJedis()) {
            jedis.hset(KEY, FIELD, data);
        }
    }

    /*----------------------------------------------------------------------*/

    /**
     * {@inheritDoc}
     */
    @Override
    public void deleteFile(String name) {
        FileInfo fileInfo = getFileInfo(name);
        if (fileInfo != null) {
            if (LOGGER.isTraceEnabled()) {
                String logMsg = "deleteFile(" + name + "/" + fileInfo.getId() + ") is called";
                LOGGER.trace(logMsg);
            }
            try (Jedis jedis = getJedis()) {
                try (Pipeline p = jedis.pipelined()) {
                    // delete file entry in directory
                    byte[] KEY_METADATA = metadataKeyFor(fileInfo);
                    p.hdel(hashDirectoryMetadata, KEY_METADATA);

                    // delete file data
                    byte[] KEY_DATA = dataKeyFor(fileInfo);
                    p.del(KEY_DATA);

                    /*
                     * No need to call p.sync() because we do not wish to receive any response.
                     */
                }
            }
        } else {
            if (LOGGER.isTraceEnabled()) {
                String logMsg = "deleteFile(" + name + ") is called, but file is not found";
                LOGGER.trace(logMsg);
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public IndexInput openInput(String name, IOContext context) throws IOException {
        FileInfo fileInfo = getFileInfo(name);
        if (fileInfo == null) {
            throw new FileNotFoundException("File [" + name + "] not found!");
        }
        return new LucextIndexInput(this, fileInfo);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public IndexOutput createOutput(String name, IOContext context) {
        FileInfo fileInfo = getFileInfo(name);
        if (fileInfo == null) {
            fileInfo = FileInfo.newInstance(name);
            updateFileInfo(fileInfo);
        }
        return new LucextIndexOutput(this, fileInfo);
    }

    /*----------------------------------------------------------------------*/
    /**
     * Create a new lock instance.
     */
    public Lock createLock(String lockName) {
        return new RedisLock(this, lockName);
    }

    /**
     * Redis implementation of {@link Lock}.
     * 
     * @author Thanh Nguyen <btnguyen2k@gmail.com>
     * @since 0.1.0
     */
    private class RedisLock extends LucextLock {
        public RedisLock(LucextDirectory directory, String fileName) {
            super(directory, fileName);
        }

        protected boolean obtainLock() {
            FileInfo fileInfo = getFileInfo();
            byte[] FIELD = metadataKeyFor(fileInfo);
            try (Jedis jedis = getJedis()) {
                /*
                 * Try to create an entry in the directory for the log file
                 * Note: lock file has 0 size, so we just need to create file's metadata.
                 */
                Long result = jedis.hsetnx(hashDirectoryMetadata, FIELD, fileInfo.asBytes());
                return result != null && result.longValue() == 1;
            }
        }
    }
}
