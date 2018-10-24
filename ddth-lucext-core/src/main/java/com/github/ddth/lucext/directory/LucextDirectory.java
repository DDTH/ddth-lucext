package com.github.ddth.lucext.directory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.zip.CRC32;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.BaseDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.Lock;
import org.apache.lucene.store.LockFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.ddth.cacheadapter.ICache;
import com.github.ddth.cacheadapter.ICacheFactory;

/**
 * Base class for Lucext's implementations of Lucene's {@link Directory}.
 * 
 * <p>
 * Design:
 * <ul>
 * <li>A storage to store directory metadata: each entry is a {@link FileInfo}, keyed by file's
 * name.</li>
 * <li>A storage to store file data, keyed by file's id. File's data can be divided into
 * {@link #blockSize}-byte chunks.</li>
 * </ul>
 * </p>
 * 
 * @author Thanh Nguyen <btnguyen2k@gmail.com>
 * @since 0.1.0
 */
public abstract class LucextDirectory extends BaseDirectory {

    public static class LucextLockFactory extends LockFactory {
        public final static LucextLockFactory INSTANCE = new LucextLockFactory();

        private LucextLockFactory() {
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public Lock obtainLock(Directory dir, String lockName) throws IOException {
            if (!(dir instanceof LucextDirectory)) {
                throw new IllegalArgumentException(
                        "Expect argument of type [" + LucextDirectory.class.getName() + "]!");
            }
            return ((LucextDirectory) dir).createLock(lockName);
        }
    }

    private Logger LOGGER = LoggerFactory.getLogger(LucextDirectory.class);

    public final static int DEFAULT_BLOCK_SIZE = 64 * 1024; // 64kb
    private int blockSize = DEFAULT_BLOCK_SIZE;

    private ICacheFactory cacheFactory;
    private String cacheName;
    private String cacheKeyAllFiles = "ALL_FILES";

    public LucextDirectory() {
        super(LucextLockFactory.INSTANCE);
    }

    /**
     * File's data can be divided into {@link #blockSize}-byte chunks.
     * 
     * @return
     */
    public int getBlockSize() {
        return blockSize;
    }

    /**
     * File's data can be divided into {@link #blockSize}-byte chunks.
     * 
     * @param blockSize
     * @return
     */
    public LucextDirectory setBlockSize(int blockSize) {
        this.blockSize = blockSize <= 0 ? DEFAULT_BLOCK_SIZE : blockSize;
        return this;
    }

    public LucextDirectory init() {
        // EMPTY
        return this;
    }

    public void destroy() {
        // EMPTY
    }

    /*----------------------------------------------------------------------*/
    /**
     * Cache factory to cache data.
     * 
     * @return
     */
    public ICacheFactory getCacheFactory() {
        return cacheFactory;
    }

    /**
     * Cache factory to cache data.
     * 
     * @param cacheFactory
     * @return
     */
    public LucextDirectory setCacheFactory(ICacheFactory cacheFactory) {
        this.cacheFactory = cacheFactory;
        return this;
    }

    /**
     * Name of cache to store cached data.
     * 
     * @return
     */
    public String getCacheName() {
        return cacheName;
    }

    /**
     * Name of cache to store cached data.
     * 
     * @param cacheName
     * @return
     */
    public LucextDirectory setCacheName(String cacheName) {
        this.cacheName = cacheName;
        return this;
    }

    /**
     * Cache key to store all file's metadata.
     * 
     * @return
     */
    public String getCacheKeyAllFiles() {
        return cacheKeyAllFiles;
    }

    /**
     * Cache key to store all file's metadata.
     * 
     * @param cacheKeyAllFiles
     * @return
     */
    public LucextDirectory setCacheKeyAllFiles(String cacheKeyAllFiles) {
        this.cacheKeyAllFiles = cacheKeyAllFiles;
        return this;
    }

    /**
     * Get cache instance.
     * 
     * @return
     */
    protected ICache getCache() {
        return cacheFactory != null && cacheName != null ? cacheFactory.createCache(cacheName)
                : null;
    }

    /**
     * Calculate cache key for a file's chunk of data.
     * 
     * @param fileInfo
     * @param blockNum
     * @return
     */
    protected String cacheKeyDataBlock(FileInfo fileInfo, int blockNum) {
        return fileInfo.getId() + ":" + blockNum;
    }

    /**
     * Calculate cache key for a file info.
     * 
     * @param fileInfo
     * @return
     */
    protected String cacheKeyFileInfo(FileInfo fileInfo) {
        return fileInfo.getName();
    }

    /**
     * Calculate cache key for a file info.
     * 
     * @param fileName
     * @return
     */
    protected String cacheKeyFileInfo(String fileName) {
        return fileName;
    }

    /**
     * Get an item from cache.
     * 
     * @param cacheKey
     * @param clazz
     * @return
     */
    @SuppressWarnings("unchecked")
    protected <T> T getFromCache(String cacheKey, Class<T> clazz) {
        ICache cache = getCache();
        Object obj = cache != null ? cache.get(cacheKey) : null;
        if (obj != null && clazz.isAssignableFrom(obj.getClass())) {
            return (T) obj;
        }
        return null;
    }

    /**
     * Put an item to cache.
     * 
     * @param cacheKey
     * @param obj
     * @return
     */
    protected boolean putToCache(String cacheKey, Object obj) {
        ICache cache = getCache();
        if (cache != null) {
            cache.set(cacheKey, obj);
            return true;
        }
        return false;
    }

    /**
     * Delete an item from cache.
     * 
     * @param cacheKey
     */
    protected void removeFromCache(String cacheKey) {
        ICache cache = getCache();
        if (cache != null) {
            cache.delete(cacheKey);
        }
    }

    /*----------------------------------------------------------------------*/

    /**
     * Get all directory metdata.
     * 
     * @return
     * @throws IOException
     */
    protected abstract List<FileInfo> getAllFileInfo() throws IOException;

    /**
     * Get a file's metadata.
     * 
     * @param name
     * @return
     * @throws IOException
     */
    protected abstract FileInfo getFileInfo(String name) throws IOException;

    /**
     * Remove a file's meetadata.
     * 
     * @param fileInfo
     * @throws IOException
     */
    protected abstract void removeFileInfo(FileInfo fileInfo) throws IOException;

    /**
     * Update a file's metadata.
     *
     * @param fileInfo
     * @return
     * @throws IOException
     */
    protected abstract FileInfo updateFileInfo(FileInfo fileInfo) throws IOException;

    /**
     * Loads a file's data chunk from storage.
     * 
     * @param fileInfo
     * @param blockNum
     * @return {@code null} if file and/or block does not exist, otherwise a
     *         {@code byte[]} with minimum {@link #BLOCK_SIZE} length is
     *         returned
     * @throws IOException
     */
    protected abstract byte[] readFileBlock(FileInfo fileInfo, int blockNum) throws IOException;

    /**
     * Write a file's data chunk to storage.
     * 
     * @param fileInfo
     * @param blockNum
     * @param data
     * @throws IOException
     */
    protected abstract void writeFileBlock(FileInfo fileInfo, int blockNum, byte[] data)
            throws IOException;

    /**
     * {@inheritDoc}
     */
    @Override
    public String[] listAll() throws IOException {
        List<String> result = new ArrayList<>();
        getAllFileInfo().forEach(fi -> result.add(fi.getName()));
        return result.toArray(ArrayUtils.EMPTY_STRING_ARRAY);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long fileLength(String name) throws IOException {
        FileInfo fileInfo = getFileInfo(name);
        if (fileInfo == null) {
            throw new FileNotFoundException("File [" + name + "] not found!");
        }
        return fileInfo.getSize();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void rename(String source, String dest) throws IOException {
        if (LOGGER.isTraceEnabled()) {
            String logMsg = "rename(" + source + "," + dest + ") is called";
            LOGGER.trace(logMsg);
        }

        FileInfo fiSource = getFileInfo(source);
        if (fiSource == null) {
            throw new FileNotFoundException("File [" + source + "] not found!");
        }
        updateFileInfo(fiSource.clone().setName(dest));
        removeFileInfo(fiSource);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public IndexOutput createTempOutput(String prefix, String suffix, IOContext context)
            throws IOException {
        String fileName = prefix + context.context + suffix + ".tmp";
        return createOutput(fileName, context);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void sync(Collection<String> names) {
        if (LOGGER.isTraceEnabled()) {
            final String logMsg = "sync(" + names + ") is called";
            LOGGER.trace(logMsg);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void syncMetaData() {
        if (LOGGER.isTraceEnabled()) {
            final String logMsg = "syncMetaData() is called";
            LOGGER.trace(logMsg);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() {
        destroy();
    }

    /*----------------------------------------------------------------------*/

    /**
     * Create a lock instance.
     * 
     * @param lockName
     * @return
     */
    protected abstract Lock createLock(String lockName);

    protected static abstract class LucextLock extends Lock {
        private FileInfo fileInfo;
        private boolean locked = false;
        private LucextDirectory directory;

        public LucextLock(LucextDirectory directory, String fileName) {
            this.directory = directory;
            this.fileInfo = FileInfo.newInstance(fileName);
            this.locked = obtainLock();
        }

        protected FileInfo getFileInfo() {
            return fileInfo;
        }

        protected boolean isLocked() {
            return locked;
        }

        protected LucextLock markLock(boolean isLock) {
            this.locked = isLock;
            return this;
        }

        public LucextDirectory getDirectory() {
            return directory;
        }

        protected abstract boolean obtainLock();

        /**
         * {@inheritDoc}
         */
        @Override
        public void close() throws IOException {
            if (locked) {
                directory.deleteFile(fileInfo.getName());
                locked = false;
            }
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void ensureValid() throws IOException {
            if (!locked) {
                throw new AlreadyClosedException(
                        "Lock instance is not held or already released: " + this);
            }
            FileInfo fileInfo = directory.getFileInfo(this.fileInfo.getName());
            if (fileInfo == null || !StringUtils.equals(fileInfo.getId(), this.fileInfo.getId())) {
                throw new AlreadyClosedException(
                        "Lock invalidated or is held by another source: " + this);
            }
        }
    }

    /*----------------------------------------------------------------------*/
    protected static class LucextIndexOutput extends IndexOutput {
        private final Logger LOGGER = LoggerFactory.getLogger(LucextIndexOutput.class);

        private LucextDirectory directory;

        private CRC32 crc = new CRC32();
        private long bytesWritten = 0L;
        private FileInfo fileInfo;

        private int bufferOffset = 0;
        private int blockNum = 0;
        private byte[] buffer;

        public LucextIndexOutput(LucextDirectory directory, FileInfo fileInfo) {
            super(fileInfo.getId() + "_" + fileInfo.getName(), fileInfo.getName());
            this.directory = directory;
            this.fileInfo = fileInfo;
            this.buffer = new byte[directory.getBlockSize()];
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void close() throws IOException {
            flushBlock();
        }

        synchronized private void flushBlock() throws IOException {
            if (bufferOffset > 0) {
                long t1 = System.currentTimeMillis();
                directory.writeFileBlock(fileInfo, blockNum, buffer);
                blockNum++;
                bufferOffset = 0;
                buffer = new byte[directory.getBlockSize()];
                fileInfo.setSize(bytesWritten);
                directory.updateFileInfo(fileInfo);
                long t2 = System.currentTimeMillis();
                if (LOGGER.isTraceEnabled()) {
                    LOGGER.trace("flushBlock(" + fileInfo.getId() + ":" + fileInfo.getName() + ","
                            + (blockNum - 1) + ") in " + (t2 - t1) + " ms");
                }
            }
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void writeByte(byte b) throws IOException {
            crc.update(b);
            buffer[bufferOffset++] = b;
            bytesWritten++;
            fileInfo.setSize(bytesWritten);
            if (bufferOffset >= directory.getBlockSize()) {
                flushBlock();
            }
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void writeBytes(byte[] b, int offset, int length) throws IOException {
            long t1 = System.currentTimeMillis();
            for (int i = 0; i < length; i++) {
                writeByte(b[offset + i]);
            }
            long t2 = System.currentTimeMillis();
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("writeBytes(" + fileInfo.getId() + ":" + fileInfo.getName() + "/"
                        + offset + "/" + length + ") in " + (t2 - t1) + " ms");
            }
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public long getChecksum() {
            return crc.getValue();
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public long getFilePointer() {
            return bytesWritten;
        }
    }

    /*----------------------------------------------------------------------*/
    protected static class LucextIndexInput extends IndexInput {

        private final Logger LOGGER = LoggerFactory.getLogger(LucextIndexInput.class);

        private LucextDirectory directory;
        private FileInfo fileInfo;

        private boolean isSlice = false;
        private byte[] block;
        private int blockOffset = 0;
        private int blockNum = 0;

        private long offset, end, pos;

        public LucextIndexInput(LucextDirectory directory, FileInfo fileInfo) {
            super(fileInfo.getId() + "_" + fileInfo.getName());
            this.directory = directory;
            this.fileInfo = fileInfo;
            this.offset = 0L;
            this.pos = 0L;
            this.end = fileInfo.getSize();
        }

        public LucextIndexInput(String resourceDesc, LucextIndexInput another, long offset,
                long length) {
            super(resourceDesc);
            this.directory = another.directory;
            this.fileInfo = another.fileInfo;
            this.offset = another.offset + offset;
            this.end = this.offset + length;
            this.blockNum = another.blockNum;
            this.blockOffset = another.blockOffset;
            // if (another.block != null) {
            // this.block = Arrays.copyOf(another.block, another.block.length);
            // }
            try {
                seek(0);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        private void loadBlock(int blockNum) throws IOException {
            if (LOGGER.isTraceEnabled()) {
                final String logMsg = "loadBlock(" + fileInfo.getId() + ":" + fileInfo.getName()
                        + "/" + blockNum + ")";
                LOGGER.trace(logMsg);
            }
            block = directory.readFileBlock(fileInfo, blockNum);
            this.blockNum = blockNum;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public LucextIndexInput clone() {
            LucextIndexInput clone = (LucextIndexInput) super.clone();
            clone.directory = directory;
            clone.fileInfo = fileInfo;
            clone.offset = offset;
            clone.pos = pos;
            clone.end = end;
            clone.blockNum = blockNum;
            clone.blockOffset = blockOffset;
            if (block != null) {
                clone.block = Arrays.copyOf(block, block.length);
            }
            clone.isSlice = this.isSlice;
            return clone;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void close() {
            // EMPTY
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public long getFilePointer() {
            return pos;
            // return pos + offset;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public long length() {
            return end - offset;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void seek(long pos) throws IOException {
            if (pos < 0 || pos + offset > end) {
                throw new IllegalArgumentException(
                        "Seek position is out of range [0," + length() + "]!");
            }

            if (LOGGER.isTraceEnabled()) {
                String logMsg = "seek(" + fileInfo.getId() + ":" + fileInfo.getName() + ","
                        + isSlice + "," + offset + "/" + end + "," + pos + ") is called";
                LOGGER.trace(logMsg);
            }

            this.pos = pos;
            long newBlockNum = (pos + offset) / directory.getBlockSize();
            if (newBlockNum != blockNum) {
                loadBlock((int) newBlockNum);
            }
            blockOffset = (int) ((pos + offset) % directory.getBlockSize());
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public IndexInput slice(String sliceDescription, long offset, long length)
                throws IOException {
            if (LOGGER.isTraceEnabled()) {
                String logMsg = "slice(" + sliceDescription + "," + offset + "," + length + ") -> "
                        + fileInfo.getId() + ":" + fileInfo.getName();
                LOGGER.trace(logMsg);
            }
            if (offset < 0 || length < 0 || offset + length > this.length()) {
                throw new IllegalArgumentException(
                        "slice(" + sliceDescription + ") " + " out of bounds: " + this);
            }
            LucextIndexInput clone = new LucextIndexInput(sliceDescription, this, offset, length);
            clone.isSlice = true;
            return clone;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public byte readByte() throws IOException {
            if (pos + offset >= end) {
                return -1;
            }

            if (block == null) {
                loadBlock(blockNum);
            }

            byte data = block[blockOffset++];
            pos++;
            if (blockOffset >= directory.getBlockSize()) {
                loadBlock(blockNum + 1);
            }
            blockOffset = (int) ((pos + offset) % directory.getBlockSize());
            return data;
        }

        @Override
        public void readBytes(byte[] buffer, int offset, int length) throws IOException {
            long t1 = System.currentTimeMillis();
            for (int i = 0; i < length; i++) {
                buffer[offset + i] = readByte();
            }
            long t2 = System.currentTimeMillis();
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("readBytes(" + fileInfo.getId() + ":" + fileInfo.getName() + "/"
                        + offset + "/" + length + "] in " + (t2 - t1) + " ms");
            }
        }
    }
}
