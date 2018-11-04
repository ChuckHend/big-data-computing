import sqlContext.implicits._
import org.apache.spark.sql.functions.rank
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.expressions.Window

// print out the most common, uncommon word in two files
// most common word has the higheest minumum count of the two files
// min(count_1, count_2)

def getWordCountUncommon(file: String) : org.apache.spark.rdd.RDD[(String, Int)] = {
    // read the common words file
    val common = sc.textFile("common.txt").map(x => (x.toUpperCase(),1))

    val inRdd = sc.textFile(file1) //read file
            .flatMap(line => line.split("\\n|\\t|\\s+")) //split text on new line, tab, space
            .map(word => (word.toUpperCase(),1)) //cast to upper
            .subtractByKey(common) //drop the words if exist in common
            .reduceByKey{case (x, y) => x + y} //group by word and add the values

    return inRdd
}

def similarWord(file1: String, file2: String) : Unit = {

    val t1 = getWordCountUncommon(file1) //get wordcount
    val t2 = getWordCountUncommon(file2) //get wordcount
    val tu = t1.union(t2) //combind word counts

    // reduce values to array, take min from array
    val minWords = tu.reduceByKey{case (x,y) => Array(x,y).reduceLeft(_ min _)}
    
    // order the counts descending
    // lets us select highest if ties
    val myWindow = Window.orderBy(col("_2").desc)

    minWords
        .toDF() //conver to spark df
        .filter("_1 !=''") //filter out empty/null
        .select($"_1",$"_2", rank.over(myWindow).alias("rank")) //compute rank
        .filter("rank==1") // select where rank is 1
        .select("_1") // select the word, then print it
        .collect()
        .foreach(println)
}
