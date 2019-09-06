package com.wugui.sparkstarter.ml;

import org.apache.spark.ml.feature.Tokenizer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.List;

public class TokenizerExample
{
    public static void main(String[] args)
    {
        SparkSession spark = SparkSession
                .builder()
                .appName("Tokenizer")
                .master("local")
                .getOrCreate();

        List<Row> data = java.util.Arrays.asList(
                RowFactory.create(0, "Hi I heard about spark"),
                RowFactory.create(1, "I wish Java could use case classes"),
                RowFactory.create(2, "Logistic,regression,models,are,neat")
        );

        StructType schema = new StructType(new StructField[]{
                new StructField("label", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("sentence",DataTypes.StringType, false, Metadata.empty())
        });

        Dataset<Row> sentenceDataFrame = spark.createDataFrame(data, schema);

        Tokenizer tokenizer = new Tokenizer().setInputCol("sentence").setOutputCol("words");

        Dataset<Row> wordsData = tokenizer.transform(sentenceDataFrame);
        for (Row r : wordsData.select("words","label").takeAsList(3))
        {
            List<String> words = r.getList(0);
            for (String word : words) {
                System.out.println(word);
            }
        }
    }
}