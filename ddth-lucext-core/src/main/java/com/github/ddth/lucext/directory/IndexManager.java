package com.github.ddth.lucext.directory;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.Directory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javassist.util.proxy.ProxyFactory;

/**
 * Manage index-objects (IndexWriter, IndexSearcher, etc).
 * 
 * <ul>
 * <li>Automatically refresh {@link IndexSearcher} in the background. See
 * {@link #getBackgroundRefreshIndexSearcherPeriodMs()}.</li>
 * <li>Automatically call {@link IndexWriter#commit()} in the background. See
 * {@link #getBackgroundCommitIndexPeriodMs()}.</li>
 * <li>Near-real-time {@link DirectoryReader} and {@link IndexSearcher}. See
 * {@link #isNrtIndexSearcher()}</li>
 * </ul>
 *
 * @author Thanh Nguyen <btnguyen2k@gmail.com>
 * @since 0.2.0
 */
public class IndexManager implements AutoCloseable {

    private final Logger LOGGER = LoggerFactory.getLogger(IndexManager.class);

    private Directory directory;
    private IndexWriterConfig indexWriterConfig;

    private IndexWriter indexWriter;
    private DirectoryReader directoryReader;
    private IndexSearcher indexSearcher;

    private ScheduledExecutorService ses;
    private boolean myOwnScheduledExecutorService = false;

    public final static long DEFAULT_BACKGROUND_REFRESH_INDEXSEARCHER_PERIOD_MS = 10000;
    private long backgroundRefreshIndexSearcherPeriodMs = DEFAULT_BACKGROUND_REFRESH_INDEXSEARCHER_PERIOD_MS;

    public final static long DEFAULT_BACKGROUND_COMMIT_INDEX_PERIOD_MS = 1000;
    private long backgroundCommitIndexPeriodMs = DEFAULT_BACKGROUND_COMMIT_INDEX_PERIOD_MS;

    private boolean nrtIndexSearcher = true;

    public IndexManager(Directory directory) {
        this.directory = directory;
    }

    /**
     * Get the associated {@link Directory}.
     * 
     * @return
     */
    protected Directory getDirectory() {
        return directory;
    }

    /**
     * Getter for {@link #indexWriterConfig}.
     * 
     * @return
     */
    public IndexWriterConfig getIndexWriterConfig() {
        return indexWriterConfig;
    }

    /**
     * Setter for {@link #indexWriterConfig}.
     * 
     * @param iwc
     * @return
     */
    public IndexManager setIndexWriterConfig(IndexWriterConfig iwc) {
        if (indexWriter == null) {
            this.indexWriterConfig = iwc;
        } else {
            LOGGER.warn("IndexManager has been initalized, cannot change this configuration!");
        }
        return this;
    }

    /**
     * The {@link ScheduledExecutorService} for internal scheduled jobs.
     * 
     * @return
     */
    protected ScheduledExecutorService getScheduledExecutorService() {
        return ses;
    }

    /**
     * The {@link ScheduledExecutorService} for internal scheduled jobs.
     * 
     * @param ses
     * @return
     */
    public IndexManager setScheduledExecutorService(ScheduledExecutorService ses) {
        if (indexWriter == null) {
            if (this.ses != null && myOwnScheduledExecutorService) {
                this.ses.shutdown();
                myOwnScheduledExecutorService = false;
            }
            this.ses = ses;
        } else {
            LOGGER.warn("IndexManager has been initalized, cannot change this configuration!");
        }
        return this;
    }

    /**
     * If set to a positive value, the {@link IndexSearcher} will be periodically refreshed in a
     * background thread. Default value {@link #DEFAULT_BACKGROUND_REFRESH_INDEXSEARCHER_PERIOD_MS}.
     * 
     * @return
     */
    public long getBackgroundRefreshIndexSearcherPeriodMs() {
        return backgroundRefreshIndexSearcherPeriodMs;
    }

    /**
     * If set to a positive value, the {@link IndexSearcher} will be periodically refreshed in a
     * background thread. Default value {@link #DEFAULT_BACKGROUND_REFRESH_INDEXSEARCHER_PERIOD_MS}.
     * 
     * @param indexSeacherRefreshPeriodMs
     * @return
     */
    public IndexManager setBackgroundRefreshIndexSearcherPeriodMs(
            long backgroundRefreshIndexSearcherPeriodMs) {
        long oldValue = this.backgroundRefreshIndexSearcherPeriodMs;
        this.backgroundRefreshIndexSearcherPeriodMs = backgroundRefreshIndexSearcherPeriodMs;
        if (oldValue != backgroundRefreshIndexSearcherPeriodMs && indexWriter != null
                && ses != null) {
            if (backgroundRefreshIndexSearcher != null) {
                backgroundRefreshIndexSearcher.cancel(true);
            }
            if (backgroundRefreshIndexSearcherPeriodMs > 0) {
                backgroundRefreshIndexSearcher = ses.scheduleWithFixedDelay(() -> {
                    try {
                        indexSearcher = upToDateIndexSeacher();
                    } catch (IOException e) {
                        LOGGER.error(e.getMessage(), e);
                    }
                }, 1000, backgroundRefreshIndexSearcherPeriodMs, TimeUnit.MILLISECONDS);
            } else if (!isNrtIndexSearcher()) {
                LOGGER.warn(
                        "Background IndexSearcher is not configured and NRT mode is off, IndexSearcher will not be able to read new data!");
            }
        }
        return this;
    }

    /**
     * If set to a positive value, the {@link IndexWriter} will be periodically committed in a
     * background thread. Default value {@link #DEFAULT_BACKGROUND_COMMIT_INDEX_PERIOD_MS}.
     * 
     * @return
     */
    public long getBackgroundCommitIndexPeriodMs() {
        return backgroundCommitIndexPeriodMs;
    }

    /**
     * If set to a positive value, the {@link IndexWriter} will be periodically committed in a
     * background thread. Default value {@link #DEFAULT_BACKGROUND_COMMIT_INDEX_PERIOD_MS}.
     * 
     * @param indexSeacherRefreshPeriodMs
     * @return
     */
    public IndexManager setBackgroundCommitIndexPeriodMs(long backgroundCommitIndexPeriodMs) {
        long oldValue = this.backgroundCommitIndexPeriodMs;
        this.backgroundCommitIndexPeriodMs = backgroundCommitIndexPeriodMs;
        if (oldValue != backgroundCommitIndexPeriodMs && indexWriter != null && ses != null) {
            if (backgroundCommitIndex != null) {
                backgroundCommitIndex.cancel(true);
            }
            if (backgroundCommitIndexPeriodMs > 0) {
                backgroundCommitIndex = ses.scheduleWithFixedDelay(() -> {
                    try {
                        indexWriter.commit();
                    } catch (IOException e) {
                        LOGGER.error(e.getMessage(), e);
                    }
                }, 1000, backgroundCommitIndexPeriodMs, TimeUnit.MILLISECONDS);
            }
        }
        return this;
    }

    /**
     * Should the {@link IndexSearcher} be near-real-time? In near-real-time mode, whenever the
     * index is changed (document add, delete, update), the {@link DirectoryReader} and
     * {@link IndexSearcher} are refreshed so that directory-reader and
     * index-searcher (almost) always reads the latest data.
     * 
     * @return
     */
    public boolean isNrtIndexSearcher() {
        return nrtIndexSearcher;
    }

    /**
     * Should the {@link IndexSearcher} be near-real-time? In near-real-time mode, whenever the
     * index is changed (document add, delete, update), the {@link DirectoryReader} and
     * {@link IndexSearcher} are refreshed so that directory-reader and
     * index-searcher (almost) always reads the latest data.
     * 
     * @param nrtIndexSearcher
     * @return
     */
    public IndexManager setNrtIndexSearcher(boolean nrtIndexSearcher) {
        this.nrtIndexSearcher = nrtIndexSearcher;
        if (indexWriter != null && backgroundRefreshIndexSearcher == null && !nrtIndexSearcher) {
            LOGGER.warn(
                    "Background IndexSearcher is not configured and NRT mode is off, IndexSearcher will not be able to read new data!");
        }
        return this;
    }

    private ScheduledFuture<?> backgroundRefreshIndexSearcher, backgroundCommitIndex;

    /**
     * Initializing method.
     *
     * @return
     * @throws IOException
     */
    synchronized public IndexManager init() throws IOException {
        if (indexWriter == null) {
            if (indexWriterConfig == null) {
                indexWriterConfig = new IndexWriterConfig();
                indexWriterConfig.setCommitOnClose(true);
            }
            indexWriter = createIndexWriter(directory, indexWriterConfig);
            directoryReader = createDirectoryReader(null, indexWriter);
            indexSearcher = createIndexSearcher(directoryReader);

            if (ses == null) {
                ses = Executors.newScheduledThreadPool(Runtime.getRuntime().availableProcessors());
                myOwnScheduledExecutorService = true;
            }

            if (backgroundRefreshIndexSearcherPeriodMs > 0) {
                backgroundRefreshIndexSearcher = ses.scheduleWithFixedDelay(() -> {
                    try {
                        indexSearcher = upToDateIndexSeacher();
                    } catch (IOException e) {
                        LOGGER.error(e.getMessage(), e);
                    }
                }, 1000, backgroundRefreshIndexSearcherPeriodMs, TimeUnit.MILLISECONDS);
            } else if (!isNrtIndexSearcher()) {
                LOGGER.warn(
                        "Background IndexSearcher is not configured and NRT mode is off, IndexSearcher will not be able to read new data!");
            }

            if (backgroundCommitIndexPeriodMs > 0) {
                backgroundCommitIndex = ses.scheduleWithFixedDelay(() -> {
                    try {
                        indexWriter.commit();
                    } catch (IOException e) {
                        LOGGER.error(e.getMessage(), e);
                    }
                }, 1000, backgroundCommitIndexPeriodMs, TimeUnit.MILLISECONDS);
            }
        }
        return this;
    }

    /**
     * Cleanup method.
     */
    public void destroy() {
        if (ses != null && myOwnScheduledExecutorService) {
            try {
                ses.shutdown();
            } catch (Exception e) {
                LOGGER.warn(e.getMessage(), e);
            } finally {
                ses = null;
            }
        }

        if (directoryReader != null) {
            try {
                directoryReader.close();
            } catch (Exception e) {
                LOGGER.warn(e.getMessage(), e);
            } finally {
                directoryReader = null;
            }
        }

        if (indexWriter != null) {
            try {
                indexWriter.close();
            } catch (Exception e) {
                LOGGER.warn(e.getMessage(), e);
            } finally {
                indexWriter = null;
            }
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
    private IndexWriter createIndexWriter(Directory directory, IndexWriterConfig iwc)
            throws IOException {
        ProxyFactory pf = new ProxyFactory();
        pf.setSuperclass(IndexWriter.class);
        try {
            IndexWriter indexWriter = (IndexWriter) pf.create(
                    new Class<?>[] { Directory.class, IndexWriterConfig.class },
                    new Object[] { directory, iwc }, (self, thisMethod, proceed, args) -> {
                        try {
                            return proceed.invoke(self, args);
                        } finally {
                            String name = thisMethod.getName();
                            if (name.startsWith("addDocument") || name.startsWith("addIndex")
                                    || name.startsWith("deleteDocument")
                                    || name.startsWith("deleteAll") || name.startsWith("updateDoc")
                                    || (name.startsWith("update") && name.endsWith("DocValue"))) {
                                markIndexChanged();
                            }
                        }
                    });
            return indexWriter;
        } catch (NoSuchMethodException | IllegalArgumentException | InstantiationException
                | IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * This method creates new {@link DirectoryReader} if the index has changed, otherwise the old
     * one is returned.
     * 
     * @param oldDirectoryReader
     * @param indexWriter
     * @return
     * @throws IOException
     */
    @SuppressWarnings({ "resource" })
    private DirectoryReader createDirectoryReader(DirectoryReader oldDirectoryReader,
            IndexWriter indexWriter) throws IOException {
        DirectoryReader dirReader = oldDirectoryReader != null
                ? DirectoryReader.openIfChanged(oldDirectoryReader, indexWriter)
                : DirectoryReader.open(indexWriter);
        if (dirReader == null) {
            dirReader = oldDirectoryReader;
        }
        return dirReader;
    }

    private IndexSearcher createIndexSearcher(DirectoryReader directoryReader) {
        return new IndexSearcher(directoryReader);
    }

    /**
     * Get the associated {@link DirectoryReader}.
     * 
     * @return
     * @throws IOException
     */
    public DirectoryReader getDirectoryReader() throws IOException {
        return nrtIndexSearcher ? upToDateDirectoryReader() : directoryReader;
    }

    /**
     * Get the associated {@link IndexSearcher}.
     * 
     * @return
     * @throws IOException
     */
    public IndexSearcher getIndexSearcher() throws IOException {
        return nrtIndexSearcher ? upToDateIndexSeacher() : indexSearcher;
    }

    /**
     * Get the associated {@link IndexWriter}.
     * 
     * @return
     */
    public IndexWriter getIndexWriter() {
        return indexWriter;
    }

    /*----------------------------------------------------------------------*/
    private AtomicLong changeCounter = new AtomicLong(0);

    /**
     * Mark that index has changed.
     * 
     * @return
     */
    public IndexManager markIndexChanged() {
        changeCounter.incrementAndGet();
        return this;
    }

    /**
     * Check if index has changed and {@link DirectoryReader} and {@link IndexSearcher} has not been
     * refreshed.
     * 
     * @return
     */
    public boolean isIndexChanged() {
        return changeCounter.get() > 0;
    }

    private IndexSearcher upToDateIndexSeacher() throws IOException {
        DirectoryReader oldDirReader = directoryReader;
        DirectoryReader newDirReader = upToDateDirectoryReader();
        if (newDirReader != oldDirReader) {
            indexSearcher = createIndexSearcher(newDirReader);
        }
        return indexSearcher;
    }

    synchronized private DirectoryReader upToDateDirectoryReader() throws IOException {
        long changeToken = changeCounter.get();
        DirectoryReader oldDirReader = directoryReader;
        directoryReader = createDirectoryReader(oldDirReader, indexWriter);
        if (oldDirReader != null && oldDirReader != directoryReader) {
            oldDirReader.close();
        }
        changeCounter.compareAndSet(changeToken, 0);
        return directoryReader;
    }

}
