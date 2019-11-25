package com.spotify.scio.values
import com.google.common.hash.{Funnel, Funnels}

//FIXME - remove this to use Magnolify instead
trait GuavaFunnelInstances {
  implicit val intF: Funnel[Int] = Funnels.integerFunnel.asInstanceOf[Funnel[Int]]

  implicit def charSeqLikeF[T <: CharSequence]: Funnel[T] =
    Funnels.unencodedCharsFunnel().asInstanceOf[Funnel[T]]

  implicit val longF: Funnel[Long] = Funnels.longFunnel().asInstanceOf[Funnel[Long]]
}
