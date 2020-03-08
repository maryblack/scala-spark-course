package wikipedia

object WikiDebug extends App{
  override def main(args: Array[String]): Unit = {
    val l = WikipediaRanking.langs
    val sample = WikipediaRanking.occurrencesOfLang("Java", WikipediaRanking.wikiRdd)
    val rating = WikipediaRanking.rankLangs(l, WikipediaRanking.wikiRdd)
    val inverted = WikipediaRanking.makeIndexList(l, WikipediaRanking.wikiRdd)
    println("==============")
    println(sample)
    println(rating)
    println(inverted)
    val rating2 = WikipediaRanking.rankLangsUsingIndexList(inverted)
    println(">>>", rating2)
    val rating3 = WikipediaRanking.rankLangsReduceByKey(l, WikipediaRanking.wikiRdd)
    println(rating3)
  }

}
