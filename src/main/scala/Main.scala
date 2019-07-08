import org.hablapps.sparkOptics.Lens
import org.hablapps.sparkOptics.syntax._
import utils.SparkSessionWrapper
import org.apache.spark.sql.functions._


object Main extends SparkSessionWrapper {
  case class Author(name: String, nPublishedBooks: Int)
  case class Book(name: String, author: Author)
  case class Editorial(name: String, book: Book)


  def main(args: Array[String]): Unit = {
    import spark.implicits._

    val author01 = new Author("Stephen King", 54)
    val author02 = new Author("H.P.Lovecraft", 32)
    val author03 = new Author("E.Alan Poe", 42)

    val book01 = Book("Pet Sematary", author01)
    val book02 = Book("Chulthu", author02)
    val book03 = Book("Horror Tails", author03)


    val authorDF = Seq(author01, author02, author03)

    val editorial01 = new Editorial("Lumen", book01)
    val editorial02 = new Editorial("Edebe", book02)

    val editorialDF = Seq(editorial01, editorial02).toDF()
    editorialDF.show(false)


    val numberOfBooksLens = Lens("book.author.nPublishedBooks")(editorialDF.schema)

    val modifiedDF = editorialDF.select(numberOfBooksLens.get)
    modifiedDF.show(false)

  }
}