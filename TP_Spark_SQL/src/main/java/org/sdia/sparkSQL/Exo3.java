package org.sdia.sparkSQL;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.HashMap;
import java.util.Map;

public class Exo3 {

    public static SparkSession spark;

    public static void main(String[] args) {

        spark = SparkSession.builder()
                .appName("Spark SQL")
                .master("local[*]")
                .getOrCreate();

        // Charge les données des consultations depuis la base de données
        Dataset<Row> profileDF = loadTableFromDatabase(spark, "profile");
        profileDF.show();

    }

    private static Dataset<Row> loadTableFromDatabase(SparkSession spark, String tableName) {
        Map<String, String> options = new HashMap<>();
        options.put("driver", "com.mysql.cj.jdbc.Driver");
        options.put("url", "jdbc:mysql://0.0.0.0:3306/new-db");
        options.put("dbtable", tableName);
        options.put("user", "youssfi");
        options.put("password", "1234");

        return spark.read().option("header", true).format("jdbc").options(options).load();
    }
}
