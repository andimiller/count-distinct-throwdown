import cats.effect.std.Random
import cats.effect.{ExitCode, IO, IOApp, Sync}
import cats.implicits._
import cats.{Eval, Monad}
import com.clearspring.analytics.stream.cardinality.HyperLogLog
import com.dynatrace.hash4j.distinctcount.UltraLogLog
import org.atnos.origami.Fold
import fs2._
import org.apache.datasketches.theta.{Sketch, Sketches, UpdateSketch}

import scala.collection.mutable

object DistinctCount {

  def set = new Fold[IO, Long, (Long, AnyRef)] {
    type S = mutable.Set[Long]

    override def monad: Monad[IO] = implicitly

    override def start: IO[mutable.Set[Long]] = IO.pure(mutable.Set.empty[Long])
    override def fold: (mutable.Set[Long], Long) => IO[mutable.Set[Long]] = { (s, l) =>
      IO {
        s.addOne(l)
      }
    }
    override def end(s: mutable.Set[Long]): IO[(Long, AnyRef)] = IO {
      (s.size.toLong, s)
    }
  }

  def clearspring(rsd: Int) = new Fold[IO, Long, (Long, AnyRef)] {

    type S = HyperLogLog

    override def monad: Monad[IO] = implicitly

    override def start: IO[HyperLogLog] = IO {
      new HyperLogLog(rsd)
    }

    override def fold: (HyperLogLog, Long) => IO[HyperLogLog] = { case (hll, long) =>
      IO {
        hll.offerHashed(long)
        hll
      }
    }

    override def end(s: HyperLogLog): IO[(Long, AnyRef)] = IO {
      (s.cardinality(), s)
    }
  }

  def ull(p: Int) = new Fold[IO, Long, (Long, AnyRef)] {
    type S = UltraLogLog

    override def monad: Monad[IO] = implicitly

    override def start: IO[UltraLogLog] = IO {
      UltraLogLog.create(p)
    }

    override def fold: (UltraLogLog, Long) => IO[UltraLogLog] = { case (ull, long) =>
      IO {
        ull.add(long)
        ull
      }
    }

    override def end(s: UltraLogLog): IO[(Long, AnyRef)] = IO {
      (s.getDistinctCountEstimate.toLong, s)
    }
  }

  def theta(lgK: Int) = new Fold[IO, Long, (Long, AnyRef)] {
    override def monad: Monad[IO] = implicitly

    override type S = UpdateSketch

    override def start: IO[UpdateSketch] = IO {
      UpdateSketch.builder().setLogNominalEntries(lgK).build()
    }

    override def fold: (UpdateSketch, Long) => IO[UpdateSketch] = { case (s, l) =>
      IO {
        s.update(l)
        s
      }
    }

    override def end(s: UpdateSketch): IO[(Long, AnyRef)] = IO {
      (
        s.getEstimate.toLong,
        s.compact()
      )
    }
  }



}
