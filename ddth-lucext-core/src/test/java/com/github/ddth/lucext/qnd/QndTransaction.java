package com.github.ddth.lucext.qnd;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import com.github.ddth.lucext.directory.IndexManager;

public class QndTransaction {

    private static void countDocs(IndexManager indexManager) throws IOException {
        IndexSearcher is = indexManager.getIndexSearcher();
        TermQuery query = new TermQuery(new Term("class", "test"));
        TopDocs result = is.search(query, 100);
        System.out.println("Num docs  : " + result.totalHits);
    }

    public static void main(String[] args) throws Exception {
        File temp = new File("./temp");
        FileUtils.deleteQuietly(temp);
        temp.mkdirs();

        try (Directory dir = FSDirectory.open(temp.toPath())) {
            try (IndexManager indexManager = new IndexManager(dir)) {
                indexManager.setBackgroundCommitIndexPeriodMs(0)
                        .setBackgroundRefreshIndexSearcherPeriodMs(0).setNrtIndexSearcher(true);
                indexManager.init();

                IndexWriter indexWriter = indexManager.getIndexWriter();
                for (int i = 0; i < 10; i++) {
                    Document doc = new Document();
                    doc.add(new StringField("class", "test", Store.YES));
                    doc.add(new StringField("id", String.valueOf(i), Store.YES));
                    indexWriter.addDocument(doc);
                }

                countDocs(indexManager);

                indexWriter.prepareCommit();
                for (int i = 0; i < 10; i++) {
                    Document doc = new Document();
                    doc.add(new StringField("class", "test", Store.YES));
                    doc.add(new StringField("id", String.valueOf(i + 10), Store.YES));
                    indexWriter.addDocument(doc);
                }
                countDocs(indexManager);
                indexWriter.rollback();
                countDocs(indexManager);
            }
        }
    }

}
