package edu.colostate.cs.cs435.josiahm.pa3.drivers;

import static org.apache.spark.sql.functions.col;

import com.google.common.collect.Iterables;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;
import org.apache.spark.HashPartitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

/**
 * This is a modification of the given page rank example to match the lessons in the class
 * In class we were told the sum of the total page ranks should be equal to 1, and the example
 * produced a sum that is equal to the total number of articles
 */
public final class JavaPageRankClassLessons {
  private static final Pattern SPACES = Pattern.compile("\\s+");
  private static final Pattern COLONS = Pattern.compile(":");
  private static final String ROCKY_ID = "4290745";


  private static class Sum implements Function2<Double, Double, Double> {
    @Override
    public Double call(Double a, Double b) {
      return a + b;
    }
  }

  public static void main(String[] args) throws Exception {

    if (args.length < 2) {
      System.err.println("Usage: JavaPageRank <file> <number_of_iterations>");
      System.exit(1);
    }


    SparkSession spark = SparkSession
        .builder()
        //.master("local") // for running in ide
        .appName("Page Rank")
        .config("spark.sql.broadcastTimeout",  36000)
        //.config("spark.rpc.askTimeout", "600s") // joins of dataSets taking too long
        .getOrCreate();

    spark.sparkContext().setLogLevel("ERROR");

    JavaRDD<String> lines = spark.read().textFile(args[0]).javaRDD();

    // Read id: tolink tolink ...
    JavaPairRDD<String, Iterable<String>> links = createLinksPairRDD(lines);

    // Read Titles
    Dataset<Row> TITLE_TABLE = createTitlesDataSet(spark, args[1]);
    System.out.println("Titles");
    TITLE_TABLE.show(20);

    // Find no tax
    JavaPairRDD<String, Double> pageRankNoTax = findPageRank(args[2], links, false);

    findTopTenAndSave(spark, pageRankNoTax, TITLE_TABLE, "The Top 10 page ranks with no Tax", "PageRankNoTax");

    // Find with tax
    JavaPairRDD<String, Double> pageRankWithTax = findPageRank(args[2], links, true);

    findTopTenAndSave(spark, pageRankWithTax, TITLE_TABLE, "The Top 10 page ranks with Tax", "PageRankWithTax");


    // Create Google bomb links
    JavaPairRDD<String, Iterable<String>> googleBomb = getGoogleBombLinks(links, TITLE_TABLE);

    JavaPairRDD<String, Double> googleBombResults = findPageRank(args[2], googleBomb, true);

    findTopTenAndSave(spark, googleBombResults, TITLE_TABLE, "The Top 10 With google bomb", "GoogleBomb");


    System.out.println("This is the end!!!!!!!");

    spark.stop();
  }


  /**
   * Finds Rocky_Mountain_National_Park's id and all the links with surfing in the title, and creates
   * a modified PairRDD with all links with surfing directing to the ID of Rocky_Mountain_National_Park
   * @param links all the links file
   * @param TITLE_TABLE The DataSet with all the titles
   * @return the modified links for the google bomb
   */
  private static JavaPairRDD<String, Iterable<String>> getGoogleBombLinks(
      JavaPairRDD<String, Iterable<String>> links,
      Dataset<Row> TITLE_TABLE) {

    // Find the surfing and Rocky IDs
    Dataset<Row> findSurfing = TITLE_TABLE.filter("Title like '%surfing%'");
    Dataset<Row> findRocky = TITLE_TABLE.filter("Title like 'Rocky_Mountain_National_Park'");
    findRocky.show(10);
    findSurfing.show(10);

    Dataset<Row> surfIds = findSurfing.unionByName(findRocky);

    // Find the rocky key
    List<Row>  rockyIdList= findRocky.select("ID").toJavaRDD().take(1);
    // Get the string value of the key
    String rockyId = (String) rockyIdList.get(0).get(0);

    // Create a JavaPairRDD of ID's for all the surfing titles
    JavaPairRDD<String, String> keysToUse = surfIds.toJavaRDD().mapToPair(new PairFunction<Row, String, String>() {
      public Tuple2<String, String> call(Row row) throws Exception {
        return new Tuple2<String, String>((String) row.get(0), new String());
      }
    });
    // First Subtract to find all the links not containing surfing, second to remove the links

    return links
        .subtractByKey(links.subtractByKey(keysToUse)).mapValues(x -> {
          List<String> fixedList = new ArrayList<>();
          for (String outLink : x) {
            fixedList.add(outLink);
          }
          fixedList.add(rockyId);
          return fixedList;
        });
  }

  /**
   * Reads the titles file and makes a DataSet with the ID:Title
   * @param spark context to create the DataSet
   * @param fileLocation file to read
   * @return the DataSet
   */
  private static Dataset<Row> createTitlesDataSet(SparkSession spark, String fileLocation) {

    JavaPairRDD<String,String> titles = spark.read().textFile(fileLocation).javaRDD()
        .zipWithIndex().mapToPair(x-> new Tuple2 <>(String.valueOf(x._2()+1),x._1()));

    return spark.createDataset(JavaPairRDD.toRDD(titles), Encoders
        .tuple(Encoders.STRING(),Encoders.STRING())).toDF("Id","Title").persist();
  }

  /**
   * Creates a PairRDD with the id and [] of all outgoing links
   * @param lines all the lines in link file
   * @return the links in PairRDD form
   */
  private static JavaPairRDD<String, Iterable<String>> createLinksPairRDD(JavaRDD<String> lines) {
    return lines.flatMap(s -> {

      List<String> pairs = new ArrayList<>();
      String[] parts = COLONS.split(s);
      String[] outGoingLinks = SPACES.split(parts[1].trim());
      for(String link: outGoingLinks)
        pairs.add(parts[0] + " " + link);

      return pairs.iterator();

    }).mapToPair(s -> {
      String[] parts = SPACES.split(s);
      return new Tuple2<>(parts[0], parts[1]);

    }).distinct().groupByKey().partitionBy(new HashPartitioner(100)).cache();
  }

  /**
   * Finds the top 10 page ranks and saves them with there article title
   * @param spark Spark Session for making dataset
   * @param pageRank the page rank for the IDS
   * @param TITLE_TABLE the table for the Titles and Ids
   * @param message Message for the output to show what type of values they are
   * @param saveLocation where the save the top 10 page ranks
   */
  private static void findTopTenAndSave(SparkSession spark, JavaPairRDD<String, Double> pageRank,
      Dataset<Row> TITLE_TABLE, String message, String saveLocation) {

    // Find the page rank values
    System.out.println( pageRank.values().fold(0.0, (x,y )-> x+y).doubleValue());

    // Convert Page ranks to table
    Dataset<Row> RANK_TABLE = spark.createDataset(JavaPairRDD.toRDD(pageRank), Encoders
        .tuple(Encoders.STRING(),Encoders.DOUBLE())).toDF("Idr","Rank");

    // Get the top 10
    Dataset<Row> sorted =  RANK_TABLE.select(col("*")).orderBy(col("Rank").desc()).limit(10);

    System.out.println(message);
    // Combine the Names to the IDs
    Dataset<Row> results = sorted.join(TITLE_TABLE, sorted.col("Idr").equalTo(TITLE_TABLE.col("Id"))).drop("Idr");
    results = results.orderBy(col("Rank").desc());
    results.show();

    // Save the results
    results.coalesce(1).write().mode(SaveMode.Overwrite).format("json").save(saveLocation);
  }

  /**
   * Finds the page rank for a given set of links
   * @param reps Number of repetitions to run the page rank
   * @param links the links to run the page rank on
   * @param withTax tax or no tax
   * @return the links with their page rank
   */
  private static JavaPairRDD<String, Double> findPageRank(String reps,
      JavaPairRDD<String, Iterable<String>> links, boolean withTax) {
    JavaPairRDD<String, Double> ranks = links.mapValues(rs -> 1.0);
    long numberOfLinks =links.count();
    System.out.println("Number of links: " + numberOfLinks);
    // Calculates and updates URL ranks continuously using PageRank algorithm with tax.
    for (int current = 0; current < Integer.parseInt(reps); current++) {
      // Calculates URL contributions to the rank of other URLs.
      JavaPairRDD<String, Double> contribs = links.join(ranks).values()
          .flatMapToPair(s -> {
            int urlCount = Iterables.size(s._1());
            List<Tuple2<String, Double>> results = new ArrayList<>();
            for (String n : s._1) {
              results.add(new Tuple2<>(n, s._2() / urlCount));
            }
            return results.iterator();
          });


      // Re-calculates URL ranks based on neighbor contributions.
      if(withTax)
        ranks = contribs.reduceByKey(new Sum()).mapValues(sum -> 0.15 + sum * 0.85 );
      else
        ranks = contribs.reduceByKey(new Sum()).mapValues(sum -> sum);
    }
    // Fold to change reduce to percentage
    return ranks.foldByKey(0.0, (x,y)-> y/numberOfLinks);
  }
}
