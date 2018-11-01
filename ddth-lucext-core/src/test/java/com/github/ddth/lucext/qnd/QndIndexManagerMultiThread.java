package com.github.ddth.lucext.qnd;

import java.io.File;
import java.util.Random;

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

public class QndIndexManagerMultiThread {

    private static Random random = new Random(System.currentTimeMillis());

    public static void main(String[] args) throws Exception {
        File temp = new File("./temp");
        FileUtils.deleteQuietly(temp);
        temp.mkdirs();

        try (Directory dir = FSDirectory.open(temp.toPath())) {
            try (IndexManager indexManage = new IndexManager(dir)) {
                indexManage.init();

                Thread producer = new Thread(() -> {
                    while (true) {
                        try {
                            int id = random.nextInt(1024);
                            Document doc = new Document();
                            doc.add(new StringField("class", "test", Store.YES));
                            doc.add(new StringField("id", String.valueOf(id), Store.YES));
                            indexManage.getIndexWriter().addDocument(doc);
                            System.err.println("Added document: " + id);
                            Thread.sleep(random.nextInt(1024));
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                });
                producer.setDaemon(true);
                producer.start();

                Thread searcher = new Thread(() -> {
                    while (true) {
                        try {
                            IndexSearcher is = indexManage.getIndexSearcher();
                            TermQuery query = new TermQuery(new Term("class", "test"));
                            TopDocs result = is.search(query, 1);
                            System.out.println(System.identityHashCode(is) + " - Total hits: "
                                    + result.totalHits);
                            Thread.sleep(random.nextInt(102));
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                });
                searcher.setDaemon(true);
                searcher.start();

                Thread.sleep(30000);
            }
        }
    }

}
