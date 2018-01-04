package org.saalfeldlab.watersheds.pipeline;

import org.apache.spark.api.java.function.Function;

import scala.Tuple2;

public class PermuteFirstAndSecond< A, B > implements Function< Tuple2< A, B >, Tuple2< B, A > >
{

	@Override
	public Tuple2< B, A > call( final Tuple2< A, B > t ) throws Exception
	{
		return new Tuple2<>( t._2(), t._1() );
	}

}
