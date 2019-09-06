package com.wugui.sparkstarter.ml;

import org.apache.spark.ml.feature.Word2Vec;
import org.apache.spark.ml.feature.Word2VecModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.*;

import java.util.Arrays;
import java.util.List;


public class Word2VecExample
{
    public static void main(String[] args)
    {
        SparkSession spark = SparkSession
                .builder()
                .appName("TFIDF")
                .master("local")
                .getOrCreate();

        List<Row> data = Arrays.asList(
                RowFactory.create(Arrays.asList("Hi I herd about spark".split(" "))),
                RowFactory.create(Arrays.asList("I wish Java could use case classes".split(" "))),
                RowFactory.create(Arrays.asList("Logistic regression models are neat".split(" ")))
        );

        StructType schema = new StructType(new StructField[]{
                new StructField("text", new ArrayType(DataTypes.StringType, true), false, Metadata.empty())
        });

        Dataset<Row> documentDF = spark.createDataFrame(data, schema);

        Word2Vec word2Vec = new Word2Vec()
                .setInputCol("text")
                .setOutputCol("result")
                .setVectorSize(7)
                .setMinCount(0);

        Word2VecModel model = word2Vec.fit(documentDF);
        Dataset<Row> result = model.transform(documentDF);
        result.show();
//        result.write().text("wor2vec");
        for (Row r : result.takeAsList(10))
        {
            System.out.println(r);
        }

    }
}