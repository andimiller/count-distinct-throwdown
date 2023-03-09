import cats.Monoid
import com.clearspring.analytics.stream.cardinality.HyperLogLog
import com.dynatrace.hash4j.distinctcount.UltraLogLog

import scala.collection.mutable

trait DistinctCount[T] extends Monoid[T] {
  def add(t: T, hash: Long): T
  def estimate(t: T): Double
}

object DistinctCount {
  def apply[T](implicit dc: DistinctCount[T]): DistinctCount[T] = dc

  implicit val distinctCountLongSet: DistinctCount[mutable.Set[Long]] = new DistinctCount[mutable.Set[Long]] {
    override def add(t: mutable.Set[Long], hash: Long): mutable.Set[Long] = t.addOne(hash)
    override def empty: mutable.Set[Long] = mutable.Set.empty
    override def combine(x: mutable.Set[Long], y: mutable.Set[Long]): mutable.Set[Long] = x ++ y
    override def estimate(t: mutable.Set[Long]): Double = t.size.toDouble
  }

  case class ClearspringConfig(rsd: Int)
  implicit def clearspring(implicit cc: ClearspringConfig): DistinctCount[HyperLogLog] = new DistinctCount[HyperLogLog] {
    override def add(t: HyperLogLog, hash: Long): HyperLogLog = {
      t.offerHashed(hash)
      t
    }
    override def estimate(t: HyperLogLog): Double = t.cardinality().toDouble
    override def empty: HyperLogLog = new HyperLogLog(cc.rsd)
    override def combine(x: HyperLogLog, y: HyperLogLog): HyperLogLog = {
      x.addAll(y)
      x
    }
  }

  // p is 3 to 26
  case class UltraLogLogConfig(p: Int)
  implicit def ultraloglog(implicit ullc: UltraLogLogConfig): DistinctCount[UltraLogLog] = new DistinctCount[UltraLogLog] {
    override def add(t: UltraLogLog, hash: Long): UltraLogLog = t.add(hash)
    override def estimate(t: UltraLogLog): Double = t.getDistinctCountEstimate
    override def empty: UltraLogLog = UltraLogLog.create(ullc.p)
    override def combine(x: UltraLogLog, y: UltraLogLog): UltraLogLog = {
      x.add(y)
      x
    }
  }
}