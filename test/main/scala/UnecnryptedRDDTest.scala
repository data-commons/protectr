
import org.apache.spark.SparkConf
import org.scalatest.{FunSuite, ShouldMatchers}

class UnecnryptedRDDTest extends FunSuite with ShouldMatchers {

  test("something!") {
    val conf = new SparkConf().setMaster("local").setAppName(getClass.getName)
    assert(conf != null)
  }
}
