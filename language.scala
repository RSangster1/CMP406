import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.SparkFiles
import scala.io.Source
import java.io._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path

object DetectLanguage {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Language detection").setMaster("yarn")
    val sc = new SparkContext(conf)
    val hadoopConf = new Configuration()
    val fs = FileSystem.get(hadoopConf)
    val os = fs.create(new Path("output/output.txt"))
    val path = "books"
    val p = new Path(path)
    val ls = fs.listStatus(p)

    // Location of files in HDFS
    val hdfsDirectory = "hdfs://hadoopmaster:9000/user/hadoop/books/"

    // Read the files from HDFS
    val files = sc.wholeTextFiles(hdfsDirectory)

    // Arrays for each language
    val englishWords = Array("be", "and", "have", "he", "she", "that")
    val frenchWords = Array("bonjour", "bonsoir", "qui", "non", "merci", "avoir")
    val germanWords = Array("wandern", "sitzen", "bleiben", "krank", "ich")

    // Check each file with the arrays
    def detectLanguage(text: String): Option[String] = {
      val lowercaseText = text.toLowerCase()
      val words = lowercaseText.split("\\s+")

      if (englishWords.exists(words.contains)) Some("english")
      else if (frenchWords.exists(words.contains)) Some("french")
      else if (germanWords.exists(words.contains)) Some("german")
      else None
    }

    val languageCounts = files.flatMap { case (_, content) =>
      detectLanguage(content).map(language => (language, 1))
    }

    // This counts the number of files that are in each language
    val languageFileCount = languageCounts.reduceByKey(_ + _)

        // Output results
    val resultArray = languageFileCount.collect()
    val englishCount = resultArray.find(_._1 == "english").map(_._2).getOrElse(0)
    val frenchCount = resultArray.find(_._1 == "french").map(_._2).getOrElse(0)
    val germanCount = resultArray.find(_._1 == "german").map(_._2).getOrElse(0)

    val englishString = s"English count: $englishCount \n"
    val frenchString = s"French count: $frenchCount \n"
    val germanString = s"German count: $germanCount \n"


    //store output in output file.
    os.write(englishString.getBytes)
    os.write(frenchString.getBytes)
    os.write(germanString.getBytes)
 




    // Stop the Spark context
    sc.stop()
  }
}


