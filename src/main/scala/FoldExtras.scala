import cats.{Applicative, Monad}
import fs2._
import cats.effect._
import org.atnos.origami.{Fold, fold}

object FoldExtras {

  implicit class StreamSyntax[F[_] : Sync, A](p: Stream[F, A]) {
    def foldWith[B](fold: Fold[F, A, B]): F[B] = {
      for {
        s <- Stream.eval(fold.start)
        s2 <- p.evalScan(s)(fold.fold).last
        e <- Stream.eval(fold.end(s2.getOrElse(s)))
      } yield e
    }.compile.lastOrError
  }

  implicit def foldApplicative[F[_]: Monad, Z]: Applicative[Fold[F, Z, *]] = new Applicative[Fold[F, Z, *]] {
    override def pure[A](x: A): Fold[F, Z, A] = fold.fromStart(Monad[F].pure(x))

    override def ap[A, B](ff: Fold[F, Z, A => B])(fa: Fold[F, Z, A]): Fold[F, Z, B] =
      ff.zip(fa).map { case (f, a) => f(a) }
  }
}
