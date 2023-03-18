import FoldExtras.{StreamSyntax, foldApplicative}
import cats.data.NonEmptyList
import cats.effect._
import cats.effect.std.Random
import cats.implicits._
import org.github.jamm.MemoryMeter
import fs2._
import fs2.data.csv.{CellEncoder, RowEncoder}
import fs2.data.csv.generic.semiauto._
import fs2.data.csv._
import org.atnos.origami.Fold
import squants.information._

import scala.util.hashing.MurmurHash3


object Main extends IOApp.Simple {

  val ONE_MILLION = 1000000

  def data[F[_]: Async](seed: Long, size: Int) = {
    Stream.eval(Random.scalaUtilRandomSeedLong[F](seed)).flatMap { r =>
      Stream.eval(r.nextLong).repeatN(size)
    }
  }


  private val humanize: Information => Information = {
    case i if i < Kilobytes(1) => i.in(Bytes)
    case i if i < Megabytes(1) => i.in(Kilobytes)
    case i if i < Gigabytes(1) => i.in(Megabytes)
    case i => i.in(Gigabytes)
  }

  case class CsvResult(`type`: String, inputSize: Int, errorPercentage: Double, bytes: Information)
  object CsvResult {
    implicit val infoEncoder: CellEncoder[Information] = CellEncoder.longEncoder.contramap(_.toBytes.toLong)
    implicit val encoder: RowEncoder[CsvResult] = deriveRowEncoder
  }

  case class Results(name: String, inputSize: Int, estimate: Double,  size: Information) {
    def error = (inputSize - estimate).abs / inputSize * 100
    override def toString: String = s"$name provided an estimate of $estimate and used $size memory with an error margin of $error%"
    def toCsvResult: CsvResult = CsvResult(name, inputSize, error, size)
  }

  override def run: IO[Unit] = {
    val seed = 1234L
    val folds: Fold[IO, Long, List[(String, (Long, AnyRef))]] = {
      import DistinctCount._
      List(set.tupleLeft("Set")) ++
        (8 to 16).map(i => clearspring(i).tupleLeft(s"clearspring($i)")).toList ++
        (8 to 16).map(i => ull(i).tupleLeft(s"ULL($i)")).toList ++
        (4 to 12).map(i => theta(i).tupleLeft(s"Theta($i)")).toList
    }.sequence

    val memoryMeter = new MemoryMeter()

    val sizes: List[Int] = (1 to 20).map(1 << _).toList

    Stream.emits(sizes).covary[IO]
      .flatMap { size =>
        Stream.evals(data[IO](seed, size).foldWith(folds))
          .map { case (name, (result, obj)) =>
            Results(name, size, result.toDouble, Bytes(memoryMeter.measureDeep(obj)))
              .toCsvResult
          }
      }
      .through(encodeGivenHeaders[CsvResult](NonEmptyList.of("type", "size", "errorPercentage", "bytes")))
      .through(fs2.io.stdoutLines[IO, String]())
      .compile
      .drain
  }

}
