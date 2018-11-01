package com.github.ddth.lucext.qnd;

import java.io.File;

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

public class QndIndexManager {

    public static void main(String[] args) throws Exception {
        File temp = new File("./temp");
        FileUtils.deleteQuietly(temp);
        temp.mkdirs();

        try (Directory dir = FSDirectory.open(temp.toPath())) {
            try (IndexManager indexManage = new IndexManager(dir)) {
                indexManage.setBackgroundCommitIndexPeriodMs(0)
                        .setBackgroundRefreshIndexSearcherPeriodMs(1000).setNrtIndexSearcher(false);
                indexManage.init();

                for (int i = 0; i < 10; i++) {
                    Document doc = new Document();
                    doc.add(new StringField("class", "test", Store.YES));
                    doc.add(new StringField("id", String.valueOf(i), Store.YES));
                    indexManage.getIndexWriter().addDocument(doc);
                }

                Thread.sleep(10000);

                IndexSearcher is = indexManage.getIndexSearcher();
                TermQuery query = new TermQuery(new Term("class", "test"));
                TopDocs result = is.search(query, 100);
                System.out.println("Is changed: " + indexManage.isIndexChanged());
                System.out.println("Num docs  : " + result.totalHits);
            }
        }
    }

}
