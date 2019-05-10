package com.github.ddth.lucext.qnd;

import java.util.Date;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.QueryBuilder;

import com.github.ddth.commons.utils.DateFormatUtils;

public class QndQueryBuilder {
    public static void main(String[] args) {
        QueryParser qp = new QueryParser("id", new StandardAnalyzer());

        QueryBuilder qb = new QueryBuilder(new StandardAnalyzer());
        {
            Query q = qb.createBooleanQuery("id", "1");
            System.out.println(q);
        }
        {
            Query q = qb.createBooleanQuery("id", "1 2 3");
            System.out.println(q);
        }
        {
            Query q = qb.createBooleanQuery("id", "1 AND 2 not 3");
            System.out.println(q);
        }
        {
            String DATETIME_FORMAT = "yyyyMMddHHmmss";
            String term = DateFormatUtils.toString(new Date(), DATETIME_FORMAT);
            Query q = qb.createBooleanQuery("id", term);
            System.out.println(q);
        }
        {
            Query q = qb.createPhraseQuery("id", "nguyen ba thanh");
            System.out.println(q);
        }
        {
            Query q = IntPoint.newExactQuery("id", 1);
            System.out.println(q);
        }

    }
}
