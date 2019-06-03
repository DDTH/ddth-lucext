package com.github.ddth.lucext.directory.cassandra;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.api.core.cql.DefaultBatchType;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.github.ddth.cql.SessionManager;
import com.github.ddth.lucext.directory.FileInfo;
import com.github.ddth.lucext.directory.LucextDirectory;
import org.apache.commons.lang3.StringUtils;
import org.apache.lucene.store.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Cassandra implementation of {@link Directory}.
 *
 * <p>
 * Data schema:
 * </p>
 *
 * <pre>
 * DROP TABLE IF EXISTS directory_metadata;
 * DROP TABLE IF EXISTS filedata;
 *
 * -- table to store directory's metadata (file information: name, size, id
 * CREATE TABLE directory_metadata (
 *     name                VARCHAR,
 *     size                BIGINT,
 *     id                  VARCHAR,
 *     PRIMARY KEY (name)
 * );
 *
 * -- table to store actual file's data.
 * CREATE TABLE filedata (
 *     id                  VARCHAR,
 *     blocknum            INT,
 *     blockdata           BLOB,
 *     PRIMARY KEY (id, blocknum)
 * );
 * </pre>
 *
 * <p>
 * Design:
 * <ul>
 * <li>A table named {@link #tableMetadata} to store directory metadata (file info:
 * {@code id, name, size}), each file info is stored in a field keyed by file name.</li>
 * <li>A table named {@link #tableFiledata} to store file data. File data is divided into
 * {@link #getBlockSize()}-byte chunks.
 * <li>
 * </ul>
 * </p>
 *
 * @author Thanh Nguyen <btnguyen2k@gmail.com>
 * @since 0.1.0
 */
public class CassandraDirectory extends LucextDirectory {

    private Logger LOGGER = LoggerFactory.getLogger(CassandraDirectory.class);

    public final static String DEFAULT_TBL_METADATA = "directory_metadata";
    public final static String DEFAULT_TBL_FILEDATA = "filedata";
    public final static ConsistencyLevel DEFAULT_CONSISTENCY_LEVEL = DefaultConsistencyLevel.LOCAL_QUORUM;

    private ConsistencyLevel consistencyLevelReadFileData = DEFAULT_CONSISTENCY_LEVEL;
    private ConsistencyLevel consistencyLevelWriteFileData = DEFAULT_CONSISTENCY_LEVEL;
    private ConsistencyLevel consistencyLevelReadFileInfo = DEFAULT_CONSISTENCY_LEVEL;
    private ConsistencyLevel consistencyLevelWriteFileInfo = DEFAULT_CONSISTENCY_LEVEL;
    private ConsistencyLevel consistencyLevelRemoveFileInfo = DEFAULT_CONSISTENCY_LEVEL;
    private ConsistencyLevel consistencyLevelRemoveFileData = DEFAULT_CONSISTENCY_LEVEL;
    private ConsistencyLevel consistencyLevelLock = DefaultConsistencyLevel.LOCAL_SERIAL;

    private String keyspace;
    private String tableFiledata = DEFAULT_TBL_FILEDATA;
    private String tableMetadata = DEFAULT_TBL_METADATA;
    public final static String COL_FILE_NAME = "name";
    public final static String COL_FILE_SIZE = "size";
    public final static String COL_FILE_ID = "id";
    public final static String COL_BLOCK_NUM = "blocknum";
    public final static String COL_BLOCK_DATA = "blockdata";

    private String CQL_REMOVE_FILEINFO = "DELETE FROM {0} WHERE " + COL_FILE_NAME + "=?";
    private String CQL_REMOVE_FILEDATA = "DELETE FROM {0} WHERE " + COL_FILE_ID + "=?";

    private String CQL_LOAD_FILEDATA =
            "SELECT " + StringUtils.join(new String[] { COL_FILE_ID, COL_BLOCK_NUM, COL_BLOCK_DATA }, ",")
                    + " FROM {0} WHERE " + COL_FILE_ID + "=? AND " + COL_BLOCK_NUM + "=?";
    private String CQL_WRITE_FILEDATA =
            "UPDATE {0} SET " + COL_BLOCK_DATA + "=? WHERE " + COL_FILE_ID + "=? AND " + COL_BLOCK_NUM + "=?";

    private String CQL_GET_FILEINFO =
            "SELECT " + StringUtils.join(new String[] { COL_FILE_NAME, COL_FILE_SIZE, COL_FILE_ID }, ",")
                    + " FROM {0} WHERE " + COL_FILE_NAME + "=?";
    private String CQL_GET_ALL_FILES = "SELECT " + StringUtils.join(new String[] { COL_FILE_NAME }, ",") + " FROM {0}";

    private String CQL_ENSURE_FILE = "UPDATE {0} SET " + COL_FILE_ID + "=? WHERE " + COL_FILE_NAME + "=?";
    private String CQL_UPDATE_FILEINFO =
            "UPDATE {0} SET " + COL_FILE_SIZE + "=?," + COL_FILE_ID + "=? WHERE " + COL_FILE_NAME + "=?";

    private String CQL_LOCK = "INSERT INTO {0} (" + StringUtils.join(new String[] { COL_FILE_NAME, COL_FILE_ID }, ",")
            + ") VALUES (?, ?) IF NOT EXISTS";

    private SessionManager sessionManager;

    public CassandraDirectory(SessionManager sessionManager) {
        this.sessionManager = sessionManager;
    }

    protected SessionManager getSessionManager() {
        return sessionManager;
    }

    /**
     * Table to store file data.
     *
     * @return
     */
    public String getTableFiledata() {
        return tableFiledata;
    }

    /**
     * Table to store file data.
     *
     * @param tableFiledata
     * @return
     */
    public CassandraDirectory setTableFiledata(String tableFiledata) {
        this.tableFiledata = tableFiledata;
        return this;
    }

    /**
     * Table to store directory metadata.
     *
     * @return
     */
    public String getTableMetadata() {
        return tableMetadata;
    }

    /**
     * Table to store directory metadata.
     *
     * @param tableMetadata
     * @return
     */
    public CassandraDirectory setTableMetadata(String tableMetadata) {
        this.tableMetadata = tableMetadata;
        return this;
    }

    /**
     * Keyspace to store data.
     *
     * @return
     */
    public String getKeyspace() {
        return keyspace;
    }

    /**
     * Keyspace to store data.
     *
     * @param keyspace
     * @return
     */
    public CassandraDirectory setKeyspace(String keyspace) {
        this.keyspace = keyspace;
        return this;
    }

    /**
     * ConsistencyLevel for reading file data.
     *
     * @return
     */
    public ConsistencyLevel getConsistencyLevelReadFileData() {
        return consistencyLevelReadFileData;
    }

    /**
     * ConsistencyLevel for reading file data.
     *
     * @param consistencyLevelReadFileData
     * @return
     */
    public CassandraDirectory setConsistencyLevelReadFileData(ConsistencyLevel consistencyLevelReadFileData) {
        this.consistencyLevelReadFileData = consistencyLevelReadFileData;
        return this;
    }

    /**
     * ConsistencyLevel for writing file data.
     *
     * @return
     */
    public ConsistencyLevel getConsistencyLevelWriteFileData() {
        return consistencyLevelWriteFileData;
    }

    /**
     * ConsistencyLevel for writing file data.
     *
     * @param consistencyLevelWriteFileData
     * @return
     */
    public CassandraDirectory setConsistencyLevelWriteFileData(ConsistencyLevel consistencyLevelWriteFileData) {
        this.consistencyLevelWriteFileData = consistencyLevelWriteFileData;
        return this;
    }

    /**
     * ConsistencyLevel for reading file metadata.
     *
     * @return
     */
    public ConsistencyLevel getConsistencyLevelReadFileInfo() {
        return consistencyLevelReadFileInfo;
    }

    /**
     * ConsistencyLevel for reading file metadata.
     *
     * @param consistencyLevelReadFileInfo
     * @return
     */
    public CassandraDirectory setConsistencyLevelReadFileInfo(ConsistencyLevel consistencyLevelReadFileInfo) {
        this.consistencyLevelReadFileInfo = consistencyLevelReadFileInfo;
        return this;
    }

    /**
     * ConsistencyLevel for writing file metadata.
     *
     * @return
     */
    public ConsistencyLevel getConsistencyLevelWriteFileInfo() {
        return consistencyLevelWriteFileInfo;
    }

    /**
     * ConsistencyLevel for writing file metadata.
     *
     * @param consistencyLevelWriteFileInfo
     * @return
     */
    public CassandraDirectory setConsistencyLevelWriteFileInfo(ConsistencyLevel consistencyLevelWriteFileInfo) {
        this.consistencyLevelWriteFileInfo = consistencyLevelWriteFileInfo;
        return this;
    }

    /**
     * ConsistencyLevel for removing file data.
     *
     * @return
     */
    public ConsistencyLevel getConsistencyLevelRemoveFileData() {
        return consistencyLevelRemoveFileData;
    }

    /**
     * ConsistencyLevel for removing file data.
     *
     * @param consistencyLevelRemoveFileData
     * @return
     */
    public CassandraDirectory setConsistencyLevelRemoveFileData(ConsistencyLevel consistencyLevelRemoveFileData) {
        this.consistencyLevelRemoveFileData = consistencyLevelRemoveFileData;
        return this;
    }

    /**
     * ConsistencyLevel for removing file metadata.
     *
     * @return
     */
    public ConsistencyLevel getConsistencyLevelRemoveFileInfo() {
        return consistencyLevelRemoveFileInfo;
    }

    /**
     * ConsistencyLevel for removing file metadata.
     *
     * @param consistencyLevelRemoveFileInfo
     * @return
     */
    public CassandraDirectory setConsistencyLevelRemoveFileInfo(ConsistencyLevel consistencyLevelRemoveFileInfo) {
        this.consistencyLevelRemoveFileInfo = consistencyLevelRemoveFileInfo;
        return this;
    }

    /**
     * ConsistencyLevel for file locking.
     *
     * @return
     */
    public ConsistencyLevel getConsistencyLevelLock() {
        return consistencyLevelLock;
    }

    /**
     * ConsistencyLevel for file locking.
     *
     * @param consistencyLevelLock
     * @return
     */
    public CassandraDirectory setConsistencyLevelLock(ConsistencyLevel consistencyLevelLock) {
        this.consistencyLevelLock = consistencyLevelLock;
        return this;
    }

    /*----------------------------------------------------------------------*/

    /**
     * {@inheritDoc}
     */
    @Override
    public CassandraDirectory init() {
        super.init();

        boolean hasKeyspace = !StringUtils.isBlank(keyspace);

        String tableNameFiledata = hasKeyspace ? keyspace + "." + tableFiledata : tableFiledata;
        CQL_LOAD_FILEDATA = MessageFormat.format(CQL_LOAD_FILEDATA, tableNameFiledata);
        CQL_REMOVE_FILEDATA = MessageFormat.format(CQL_REMOVE_FILEDATA, tableNameFiledata);
        CQL_WRITE_FILEDATA = MessageFormat.format(CQL_WRITE_FILEDATA, tableNameFiledata);

        String tableNameMetadata = hasKeyspace ? keyspace + "." + tableMetadata : tableMetadata;
        CQL_ENSURE_FILE = MessageFormat.format(CQL_ENSURE_FILE, tableNameMetadata);
        CQL_GET_FILEINFO = MessageFormat.format(CQL_GET_FILEINFO, tableNameMetadata);
        CQL_GET_ALL_FILES = MessageFormat.format(CQL_GET_ALL_FILES, tableNameMetadata);
        CQL_REMOVE_FILEINFO = MessageFormat.format(CQL_REMOVE_FILEINFO, tableNameMetadata);
        CQL_UPDATE_FILEINFO = MessageFormat.format(CQL_UPDATE_FILEINFO, tableNameMetadata);

        CQL_LOCK = MessageFormat.format(CQL_LOCK, tableNameMetadata);

        return this;
    }

    protected CqlSession getCassandraSession() {
        return sessionManager.getSession();
    }

    /*----------------------------------------------------------------------*/

    private FileInfo createFileInfo(Row row) {
        FileInfo fileInfo = FileInfo.newInstance();
        fileInfo.setId(row.getString(COL_FILE_ID));
        fileInfo.setName(row.getString(COL_FILE_NAME));
        fileInfo.setSize(row.getLong(COL_FILE_SIZE));
        return fileInfo;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected FileInfo getFileInfo(String name) {
        String CACHE_KEY = cacheKeyFileInfo(name);
        FileInfo fileInfo = getFromCache(CACHE_KEY, FileInfo.class);
        if (fileInfo == null) {
            Row row = sessionManager.executeOne(CQL_GET_FILEINFO, consistencyLevelReadFileInfo, name);
            if (row != null) {
                fileInfo = createFileInfo(row);
                putToCache(CACHE_KEY, fileInfo);
            }
        }
        return fileInfo;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void removeFileInfo(FileInfo fileInfo) {
        try {
            sessionManager.execute(CQL_REMOVE_FILEINFO, consistencyLevelRemoveFileInfo, fileInfo.getName());
        } finally {
            removeFromCache(cacheKeyFileInfo(fileInfo));
            removeFromCache(getCacheKeyAllFiles());
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected FileInfo updateFileInfo(FileInfo fileInfo) {
        if (LOGGER.isTraceEnabled()) {
            String logMsg = "updateFile(" + fileInfo.getId() + ":" + fileInfo.getName() + "/" + fileInfo.getSize()
                    + ") is called";
            LOGGER.trace(logMsg);
        }
        try {
            sessionManager
                    .execute(CQL_UPDATE_FILEINFO, consistencyLevelWriteFileInfo, fileInfo.getSize(), fileInfo.getId(),
                            fileInfo.getName());
            putToCache(cacheKeyFileInfo(fileInfo), fileInfo);
        } finally {
            removeFromCache(getCacheKeyAllFiles());
        }
        return fileInfo;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected byte[] readFileBlock(FileInfo fileInfo, int blockNum) {
        String CACHE_KEY = cacheKeyDataBlock(fileInfo, blockNum);
        byte[] dataArr = getFromCache(CACHE_KEY, byte[].class);
        if (LOGGER.isTraceEnabled()) {
            if (dataArr != null) {
                LOGGER.trace("readFileBlock(" + fileInfo.getId() + ":" + fileInfo.getName() + "/" + blockNum
                        + ") --> cache hit");
            } else {
                LOGGER.trace("readFileBlock(" + fileInfo.getId() + ":" + fileInfo.getName() + "/" + blockNum
                        + ") --> cache missed");
            }
        }
        if (dataArr == null) {
            Row row = sessionManager
                    .executeOne(CQL_LOAD_FILEDATA, consistencyLevelReadFileData, fileInfo.getId(), blockNum);
            ByteBuffer data = row != null ? row.getByteBuffer(COL_BLOCK_DATA) : null;
            dataArr = data != null ? data.array() : null;
            putToCache(CACHE_KEY, dataArr);
        }
        return dataArr != null ?
                (dataArr.length >= getBlockSize() ? dataArr : Arrays.copyOf(dataArr, getBlockSize())) :
                null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void writeFileBlock(FileInfo fileInfo, int blockNum, byte[] data) {
        sessionManager
                .execute(CQL_WRITE_FILEDATA, consistencyLevelWriteFileData, ByteBuffer.wrap(data), fileInfo.getId(),
                        blockNum);
        String CACHE_KEY = cacheKeyDataBlock(fileInfo, blockNum);
        putToCache(CACHE_KEY, data);
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("writeFileBlock(" + fileInfo.getId() + ":" + fileInfo.getName() + "/" + blockNum
                    + ") --> update cache");
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @SuppressWarnings("unchecked")
    protected List<FileInfo> getAllFileInfo() {
        if (LOGGER.isTraceEnabled()) {
            String logMsg = "getAllFileInfo() is called";
            LOGGER.trace(logMsg);
        }
        String CACHE_KEY = getCacheKeyAllFiles();
        List<FileInfo> result = getFromCache(CACHE_KEY, List.class);
        if (result == null) {
            ResultSet rs = sessionManager.execute(CQL_GET_ALL_FILES, consistencyLevelReadFileInfo);
            List<Row> allRows = rs != null ? rs.all() : new ArrayList<>();
            result = new ArrayList<>();
            for (Row row : allRows) {
                FileInfo fileInfo = getFileInfo(row.getString(COL_FILE_NAME));
                if (fileInfo != null) {
                    result.add(fileInfo);
                }
            }
            putToCache(CACHE_KEY, result);
        }
        return result;
    }

    /**
     * Ensures a file's existence.
     *
     * @param filename
     * @return
     */
    private FileInfo ensureFile(String filename) {
        if (LOGGER.isTraceEnabled()) {
            String logMsg = "ensureFile(" + filename + ") is called";
            LOGGER.trace(logMsg);
        }
        FileInfo fileInfo = FileInfo.newInstance(filename);
        try {
            sessionManager
                    .execute(CQL_ENSURE_FILE, consistencyLevelWriteFileInfo, fileInfo.getId(), fileInfo.getName());
            putToCache(cacheKeyFileInfo(fileInfo), fileInfo);
        } finally {
            removeFromCache(getCacheKeyAllFiles());
        }
        // return getFileInfo(filename);
        return fileInfo;
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
                String logMsg = "deleteFile(" + fileInfo.getId() + ":" + name + ") is called";
                LOGGER.trace(logMsg);
            }
            try {
                Statement<?> stmRemoveFileInfo = sessionManager
                        .bindValues(sessionManager.prepareStatement(CQL_REMOVE_FILEINFO), fileInfo.getName())
                        .setConsistencyLevel(consistencyLevelRemoveFileInfo);
                Statement<?> stmRemoveFileData = sessionManager
                        .bindValues(sessionManager.prepareStatement(CQL_REMOVE_FILEDATA), fileInfo.getId())
                        .setConsistencyLevel(consistencyLevelRemoveFileData);
                sessionManager.executeBatch(DefaultBatchType.LOGGED, stmRemoveFileInfo, stmRemoveFileData);
            } finally {
                if (getCache() != null) {
                    removeFromCache(cacheKeyFileInfo(fileInfo));
                    removeFromCache(cacheKeyFileInfo(getCacheKeyAllFiles()));
                    long size = fileInfo.getSize();
                    long numBlocks = (size / getBlockSize()) + (size % getBlockSize() != 0 ? 1 : 0);
                    for (int i = 0; i < numBlocks; i++) {
                        removeFromCache(cacheKeyFileInfo(cacheKeyDataBlock(fileInfo, i)));
                    }
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
            throw new FileNotFoundException("File [" + name + "] not found");
        }
        return new LucextIndexInput(this, fileInfo);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public IndexOutput createOutput(String name, IOContext context) throws IOException {
        FileInfo fileInfo = ensureFile(name);
        if (fileInfo == null) {
            throw new IOException("File [" + name + "] cannot be created");
        }
        return new LucextIndexOutput(this, fileInfo);
    }

    /*----------------------------------------------------------------------*/

    /**
     * Create a new lock instance.
     */
    public Lock createLock(String lockName) {
        return new CassandraLock(this, lockName);
    }

    /**
     * Cassandra implementation of {@link Lock}.
     *
     * @author Thanh Nguyen <btnguyen2k@gmail.com>
     * @since 0.1.0
     */
    private class CassandraLock extends LucextLock {
        public CassandraLock(LucextDirectory directory, String fileName) {
            super(directory, fileName);
        }

        protected boolean obtainLock() {
            FileInfo fileInfo = getFileInfo();
            sessionManager.execute(CQL_LOCK, consistencyLevelLock, fileInfo.getName(), fileInfo.getId());
            FileInfo lockFile = CassandraDirectory.this.getFileInfo(fileInfo.getName());
            return lockFile != null && StringUtils.equals(lockFile.getId(), fileInfo.getId());
        }
    }
}
