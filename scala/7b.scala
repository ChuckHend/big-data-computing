// Print out the vowel combination that appears in the most words

sc.textFile("wap.txt") // read file
    .flatMap(line => line.split("\\n|\\t|\\s+")) //split each line on newline, tab, space
    .map(word => (word // map each word to, (vowel pattern, 1):
            .toUpperCase() //upper case
            .replaceAll("[^AEIOU]","") //drop all non-vowels
            .toList //convert to list
            .sorted //sort list alpha, to get unique vowel keys
            mkString "", 1)) //collapse back to string
    .reduceByKey{case (x, y) => x + y} //reduce by key
    .sortBy[Int](x=>x._2,false) //sort by the value count desc
    .take(1) // take the first value
    .map(x => x._1) //select just the key from key, value
    .foreach(println) //print it