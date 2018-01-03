package de.hanslovsky.examples.pipeline;

import org.apache.spark.api.java.function.Function;

import scala.Tuple2;
import scala.Tuple3;

public class Tuple3Helpers
{

	public static < A, B, C > Function< Tuple3< A, B, C >, C > getLast()
	{
		return new GetLast<>();
	}

	public static < A, B, C > Function< Tuple3< A, B, C >, Tuple2< A, B > > dropLast()
	{
		return new DropLast<>();
	}

	public static < A, B, C > Function< Tuple3< A, B, C >, Tuple2< A, C > > dropMiddle()
	{
		return new DropMiddle<>();
	}

	private static class GetLast< A, B, C > implements Function< Tuple3< A, B, C >, C >
	{

		@Override
		public C call( final Tuple3< A, B, C > t ) throws Exception
		{
			return t._3();
		}
	}

	private static class DropMiddle< A, B, C > implements Function< Tuple3< A, B, C >, Tuple2< A, C > >
	{

		@Override
		public Tuple2< A, C > call( final Tuple3< A, B, C > t ) throws Exception
		{
			return new Tuple2<>( t._1(), t._3() );
		}
	}

	private static class DropLast< A, B, C > implements Function< Tuple3< A, B, C >, Tuple2< A, B > >
	{

		@Override
		public Tuple2< A, B > call( final Tuple3< A, B, C > t ) throws Exception
		{
			return new Tuple2<>( t._1(), t._2() );
		}
	}


}
