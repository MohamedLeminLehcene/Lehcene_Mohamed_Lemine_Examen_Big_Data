package org.sdia.sparkSQL;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

// Press Shift twice to open the Search Everywhere dialog and type `show whitespaces`,
// then press Enter. You can now see whitespace characters in your code.
public class Main {

    public static SparkSession spark;

    public static void main(String[] args) {

        spark = SparkSession.builder()
                .appName("Spark SQL")
                 .master("spark://spark-master:7077")
                //.master("local[*]")
                .getOrCreate();

        // Charge les données des incidents à partir du ficher extension csv
        Dataset<Row> df = loadDataFromFileExCSV(spark);

        df.show();


        // Afficher le produit le plus vendu en termes de montant total

        afficherProduitPlusVenduTermMontantToal(df);
        //displayMostSoldProduct(df);

        // Afficher les 3 produits les plus vendus en termes de montant total
        afficher3PlusVendusTermsMontantTotal(df);
        //displayTop3SoldProducts(df);

        // Afficher le montant total des achats pour chaque produit
        afficherMontantTotalAchtsChaqueProduit(df);
       // displayTotalAmountPerProduct(df);


    }

    private static Dataset<Row> loadDataFromFileExCSV(SparkSession spark) {

       // return spark.read().option("header", true).csv("myFile.csv");
        return spark.read().format("csv").option("header", true).load("/bitnami/myFile.csv");
    }

    private static void afficherProduitPlusVenduTermMontantToal(Dataset<Row> df) {
        // Utiliser Spark SQL pour effectuer l'analyse
        df.createOrReplaceTempView("ventes");  // Créer une vue temporaire pour la requête SQL

        // Requête SQL pour obtenir le produit le plus vendu en termes de montant total
        Dataset<Row> result = spark.sql(
                "SELECT produit_id, SUM(montant) AS MontantTotalVente FROM ventes GROUP BY produit_id ORDER BY MontantTotalVente DESC LIMIT 1");

        // Afficher le résultat
        result.show();
    }

    private static void afficher3PlusVendusTermsMontantTotal(Dataset<Row> df) {
        // Utiliser Spark SQL pour effectuer l'analyse
        df.createOrReplaceTempView("ventes");  // Créer une vue temporaire pour la requête SQL

        // Requête SQL pour obtenir les 3 produits les plus vendus en termes de montant total
        Dataset<Row> result = spark.sql(
                "SELECT produit_id, SUM(montant) AS MontantTotalVente  FROM ventes GROUP BY produit_id ORDER BY MontantTotalVente DESC LIMIT 3");

        // Afficher le résultat
        result.show();
    }

    private static void afficherMontantTotalAchtsChaqueProduit(Dataset<Row> df) {
        // Utiliser Spark SQL pour effectuer l'analyse
        df.createOrReplaceTempView("ventes");  // Créer une vue temporaire pour la requête SQL

        // Requête SQL pour obtenir le montant total des achats pour chaque produit
        Dataset<Row> result = spark.sql(
                "SELECT produit_id, SUM(montant) AS MontantTotalAchats FROM ventes  GROUP BY produit_id ORDER BY produit_id");

        // Afficher le résultat
        result.show();
    }


}