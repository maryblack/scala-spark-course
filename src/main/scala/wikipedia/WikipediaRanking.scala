package wikipedia

import org.apache.log4j.lf5.LogLevel
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

case class WikipediaArticle(title: String, text: String) {
  /**
    * @return Whether the text of this article mentions `lang` or not
    * @param lang Language to look for (e.g. "Scala")
    */
  def mentionsLanguage(lang: String): Boolean = text.split(' ').contains(lang)
}

object WikipediaRanking extends WikipediaRankingInterface {

  val langs = List(
    "JavaScript", "Java", "PHP", "Python", "C#", "C++", "Ruby", "CSS",
    "Objective-C", "Perl", "Scala", "Haskell", "MATLAB", "Clojure", "Groovy")

  val conf = new SparkConf()
    .setMaster("local[2]")
    .setAppName("CountingSheep")
  val sc = new SparkContext(conf)
  sc.setLogLevel(LogLevel.WARN.toString)

  val wikiRdd: RDD[WikipediaArticle] = sc.parallelize(WikipediaData.lines.map(s => WikipediaData.parse(s))).persist()

  /** Returns the number of articles on which the language `lang` occurs.
    * Hint1: consider using method `aggregate` on RDD[T].
    * Hint2: consider using method `mentionsLanguage` on `WikipediaArticle`
    */
  def occurrencesOfLang(lang: String, rdd: RDD[WikipediaArticle]): Int = {
    rdd.aggregate(0)(
      (langCount, article) =>
        if (article.mentionsLanguage(lang)) {
          langCount + 1
        } else {
          langCount
        },
      (x, y) => x + y)
    //    wikiRdd.map(a => a.mentionsLanguage(lang))
    //        .filter(b => b)
    //        .count()
    //        .toInt
  }

  def rankLangs(langs: List[String], rdd: RDD[WikipediaArticle]): List[(String, Int)] = {
    langs.map(l => (l, WikipediaRanking.occurrencesOfLang(l, rdd)))
      .sortBy(_._2)(Ordering[Int].reverse)
  }

  /* Compute an inverted index of the set of articles, mapping each language
   * to the Wikipedia pages in which it occurs.
   */
  def makeIndex(langs: List[String], rdd: RDD[WikipediaArticle]): RDD[(String, Iterable[WikipediaArticle])] = {
    //    langs.map(lang => rdd.groupBy(article => lang))
    val res = langs.map(lang => (lang, rdd.filter(article => article.mentionsLanguage(lang)).toLocalIterator.toIterable))
    sc.parallelize(res)
  }

  def makeIndexList(langs: List[String], rdd: RDD[WikipediaArticle]): RDD[(String, List[WikipediaArticle])] = {
    val res = langs.map(lang => (lang, rdd.filter(article => article.mentionsLanguage(lang)).collect().toList))
    sc.parallelize(res)
  }

  /* (2) Compute the language ranking again, but now using the inverted index. Can you notice
   *     a performance improvement?
   *
   *   Note: this operation is long-running. It can potentially run for
   *   several seconds.
   */
  def rankLangsUsingIndex(index: RDD[(String, Iterable[WikipediaArticle])]): List[(String, Int)] = {
        index.mapValues(articles => articles.size).collect().sortBy(_._2)(Ordering[Int].reverse).toList
  }

  def rankLangsUsingIndexList(index: RDD[(String, List[WikipediaArticle])]): List[(String, Int)] = {
        index.mapValues(articles => articles.size).collect().sortBy(_._2)(Ordering[Int].reverse).toList
  }


  /* (3) Use `reduceByKey` so that the computation of the index and the ranking are combined.
   *     Can you notice an improvement in performance compared to measuring *both* the computation of the index
   *     and the computation of the ranking? If so, can you think of a reason?
   *
   *   Note: this operation is long-running. It can potentially run for
   *   several seconds.
   */
  def rankLangsReduceByKey(langs: List[String], rdd: RDD[WikipediaArticle]): List[(String, Int)] = {
//    langs.map(lang => (lang, rdd.red))
    sc.parallelize(langs).cartesian(rdd)
      .map(x => (x._1, if (x._2.mentionsLanguage(x._1)) 1 else 0))
      .reduceByKey((val1, val2) => val1 + val2)
      .sortBy(_._2, ascending = false)
      .collect()
//      .sortBy(_._2)(Ordering[Int].reverse)
      .toList
  }

  def main(args: Array[String]): Unit = {

    /* Languages ranked according to (1) */
    val langsRanked: List[(String, Int)] = timed("Part 1: naive ranking", rankLangs(langs, wikiRdd))

    /* An inverted index mapping languages to wikipedia pages on which they appear */
    def index: RDD[(String, Iterable[WikipediaArticle])] = makeIndex(langs, wikiRdd)

    /* Languages ranked according to (2), using the inverted index */
    val langsRanked2: List[(String, Int)] = timed("Part 2: ranking using inverted index", rankLangsUsingIndex(index))

    /* Languages ranked according to (3) */
    val langsRanked3: List[(String, Int)] = timed("Part 3: ranking using reduceByKey", rankLangsReduceByKey(langs, wikiRdd))

    /* Output the speed of each ranking */
    println(timing)
    sc.stop()
  }

  val timing = new StringBuffer

  def timed[T](label: String, code: => T): T = {
    val start = System.currentTimeMillis()
    val result = code
    val stop = System.currentTimeMillis()
    timing.append(s"Processing $label took ${stop - start} ms.\n")
    result
  }

  //  override def wikiRdd:  RDD[WikipediaArticle] = ???
}
