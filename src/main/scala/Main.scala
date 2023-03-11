import DistinctCount.{ClearspringConfig, UltraLogLogConfig}
import cats.Monad
import cats.data.NonEmptyList
import cats.effect._
import cats.effect.std.Random
import cats.implicits._
import com.clearspring.analytics.stream.cardinality.HyperLogLog
import com.dynatrace.hash4j.distinctcount.UltraLogLog
import org.github.jamm.MemoryMeter
import fs2._
import fs2.data.csv.{CellEncoder, RowEncoder}
import fs2.data.csv.generic.semiauto._
import fs2.data.csv._
import squants.information._

import scala.collection.mutable

object Main extends IOApp.Simple {

  val ONE_MILLION = 1000000

  def data[F[_]: Async](seed: Long) = {
    Stream.eval(Random.scalaUtilRandomSeedLong[F](seed)).flatMap { r =>
      Stream.eval(r.nextLong).repeatN(ONE_MILLION)
    }
  }


  private val humanize: Information => Information = {
    case i if i < Kilobytes(1) => i.in(Bytes)
    case i if i < Megabytes(1) => i.in(Kilobytes)
    case i if i < Gigabytes(1) => i.in(Megabytes)
    case i => i.in(Gigabytes)
  }

  case class CsvResult(`type`: String, errorPercentage: Double, bytes: Information)
  object CsvResult {
    implicit val infoEncoder: CellEncoder[Information] = CellEncoder.longEncoder.contramap(_.toBytes.toLong)
    implicit val encoder: RowEncoder[CsvResult] = deriveRowEncoder
  }

  case class Results(name: String, estimate: Double,  size: Information) {
    def error = (ONE_MILLION - estimate).abs / ONE_MILLION * 100
    override def toString: String = s"$name provided an estimate of $estimate and used $size memory with an error margin of $error%"
    def toCsvResult: CsvResult = CsvResult(name, error, size)
  }

  def estimate[F[_]: Async, T](name: String, seed: Long)(implicit dc: DistinctCount[T]): F[Results] = {
    for {
      acc <- data[F](123).fold(dc.empty)(dc.add).compile.lastOrError
      estimate = dc.estimate(acc)
      size = Bytes(new MemoryMeter().measureDeep(acc))
    } yield Results(name, estimate, humanize(size))
  }


  override def run: IO[Unit] = {
    val seed = 1234L
    val results = {
      Stream.eval(estimate[IO, mutable.Set[Long]]("Set[Long]", seed)) ++
        Stream.emits((3 to 26).toList).covary[IO].evalMap { p =>
          implicit val config: UltraLogLogConfig = UltraLogLogConfig(p)
          estimate[IO, UltraLogLog](s"UltraLogLog($p)", seed)
        } ++
        Stream.emits((3 to 26).toList).covary[IO].evalMap { p =>
          implicit val config: ClearspringConfig = ClearspringConfig(p)
          estimate[IO, HyperLogLog](s"Clearspring($p)", seed)
        }
    }
    results
      .map(_.toCsvResult)
      .through(encodeGivenHeaders[CsvResult](NonEmptyList.of("type", "errorPercentage", "bytes")))
      .through(fs2.io.stdoutLines[IO, String]())
      .compile
      .drain
  }

}
