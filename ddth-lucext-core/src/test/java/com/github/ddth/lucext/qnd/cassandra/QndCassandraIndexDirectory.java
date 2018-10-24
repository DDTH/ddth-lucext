package com.github.ddth.lucext.qnd.cassandra;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.index.Term;

import com.github.ddth.cacheadapter.ICacheFactory;
import com.github.ddth.cql.SessionManager;
import com.github.ddth.lucext.directory.cassandra.CassandraDirectory;

import ch.qos.logback.classic.Level;

public class QndCassandraIndexDirectory extends BaseQndCassandra {

    private static final long MAX_ITEMS = 15;
    private static final AtomicLong COUNTER = new AtomicLong(0);
    private static final AtomicLong JOBS_DONE = new AtomicLong(0);

    public static void main(String[] args) throws Exception {
        initLoggers(Level.INFO);

        try (SessionManager sm = getSessionManager()) {
            sm.executeNonSelect(
                    "CREATE KEYSPACE IF NOT EXISTS test WITH REPLICATION={'class' : 'SimpleStrategy', 'replication_factor' : 1}");
            sm.executeNonSelect(
                    "DROP TABLE IF EXISTS test." + CassandraDirectory.DEFAULT_TBL_FILEDATA);
            sm.executeNonSelect(
                    "DROP TABLE IF EXISTS test." + CassandraDirectory.DEFAULT_TBL_METADATA);

            sm.executeNonSelect("CREATE TABLE test." + CassandraDirectory.DEFAULT_TBL_METADATA
                    + " (name VARCHAR, size BIGINT, id VARCHAR, PRIMARY KEY (name))");
            sm.executeNonSelect("CREATE TABLE test." + CassandraDirectory.DEFAULT_TBL_FILEDATA
                    + " (id VARCHAR, blocknum INT, blockdata BLOB, PRIMARY KEY (id, blocknum))");
            Thread.sleep(1000);

            ICacheFactory cf = getCacheFactory(true);

            try (CassandraDirectory DIR = new CassandraDirectory(sm)) {
                DIR.setKeyspace("test").setCacheFactory(cf).setCacheName("lucext");
                DIR.init();

                long t1 = System.currentTimeMillis();

                Analyzer analyzer = new StandardAnalyzer();
                IndexWriterConfig iwc = new IndexWriterConfig(analyzer);
                iwc.setOpenMode(OpenMode.CREATE_OR_APPEND);
                IndexWriter iw = new IndexWriter(DIR, iwc);

                Path docDir = Paths.get("./");
                indexDocs(iw, docDir);

                iw.commit();
                iw.close();

                long t2 = System.currentTimeMillis();
                System.out.println("Finished indexing in " + (t2 - t1) / 1000.0 + " sec");
            }
        }
    }

    static void indexDocs(final IndexWriter writer, Path path) throws IOException {
        if (Files.isDirectory(path)) {
            Files.walkFileTree(path, new SimpleFileVisitor<Path>() {
                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
                        throws IOException {
                    try {
                        indexDoc(writer, file, attrs.lastModifiedTime().toMillis());
                    } catch (IOException ignore) {
                        // don't index files that can't be read.
                    }
                    return FileVisitResult.CONTINUE;
                }
            });
        } else {
            indexDoc(writer, path, Files.getLastModifiedTime(path).toMillis());
        }
    }

    static void indexDoc(IndexWriter writer, Path file, long lastModified) throws IOException {
        String filename = file.getFileName().toString().toLowerCase();
        if (!filename.endsWith(".java") && !filename.endsWith(".properties")
                && !filename.endsWith(".xml") && !filename.endsWith(".html")
                && !filename.endsWith(".txt") && !filename.endsWith(".md")) {
            return;
        }

        long counter = COUNTER.incrementAndGet();
        if (counter > MAX_ITEMS) {
            return;
        }
        System.out.println("Counter: " + counter);

        try (InputStream stream = Files.newInputStream(file)) {
            Document doc = new Document();

            Field pathField = new StringField("path", file.toString(), Field.Store.YES);
            doc.add(pathField);

            doc.add(new LongPoint("modified", lastModified));

            doc.add(new TextField("contents",
                    new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))));

            if (writer.getConfig().getOpenMode() == OpenMode.CREATE) {
                System.out.println("adding " + file);
                writer.addDocument(doc);
            } else {
                System.out.println("updating " + file);
                writer.updateDocument(new Term("path", file.toString()), doc);
            }
        } finally {
            long jobsDone = JOBS_DONE.incrementAndGet();
            System.out.println("Jobs done: " + jobsDone);
        }
    }
}
