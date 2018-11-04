//print out words that occur between 5 and 7 times

sc.textFile("wap.txt") //read file
    .flatMap(line => line.split("\\n|\\t|\\s+")) //split text on new line, tab, space
    .map(word => word.toUpperCase()) //cast to upper
    .map(word => (word, 1)) //map each word to word and the value of 1
    .reduceByKey{case (x, y) => x + y} //group by word and add the values
    .filter { case (key, value) => value >= 5 & value <= 9 } //filter results where: 5 >= value <= 9 
    .collect() //collect the result
    .foreach(println) //print the results