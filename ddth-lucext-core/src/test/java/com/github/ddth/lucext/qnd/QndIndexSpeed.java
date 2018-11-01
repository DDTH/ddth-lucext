package com.github.ddth.lucext.qnd;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.io.FileUtils;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import com.github.ddth.lucext.directory.IndexManager;

public class QndIndexSpeed {

    public static void main(String[] args) throws Exception {
        File temp = new File("./temp");
        FileUtils.deleteQuietly(temp);
        temp.mkdirs();

        try (Directory dir = FSDirectory.open(temp.toPath())) {
            try (IndexManager indexManage = new IndexManager(dir)) {
                indexManage.setBackgroundCommitIndexPeriodMs(0)
                        .setBackgroundRefreshIndexSearcherPeriodMs(0).setNrtIndexSearcher(true);
                indexManage.init();

                long start = System.currentTimeMillis();

                AtomicLong counter = new AtomicLong(0);
                int numThreeads = 16;
                int itemsPerThread = 1000000;
                Thread[] threads = new Thread[numThreeads];
                for (int i = 0; i < numThreeads; i++) {
                    threads[i] = new Thread(() -> {
                        for (int j = 0; j < itemsPerThread; j++) {
                            long value = counter.incrementAndGet();
                            Document doc = new Document();
                            doc.add(new StringField("class", "test", Store.YES));
                            doc.add(new StringField("id", String.valueOf(value), Store.YES));
                            try {
                                indexManage.getIndexWriter().addDocument(doc);
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                        }
                    });
                }
                for (Thread t : threads) {
                    t.start();
                }
                for (Thread t : threads) {
                    t.join();
                }

                long d = System.currentTimeMillis() - start;
                System.out.println("Write " + counter.get() + " items in " + d + " ms ("
                        + Math.round(counter.get() * 1000.0 / d) + " items/s)");

                IndexSearcher is = indexManage.getIndexSearcher();
                TermQuery query = new TermQuery(new Term("class", "test"));
                TopDocs result = is.search(query, 1);
                System.out.println("Written items: " + result.totalHits);
            }
        }
    }

}
