package de.hanslovsky.examples.pipeline;

import org.apache.spark.api.java.function.Function;

import scala.Tuple3;
import scala.Tuple4;

public class Tuple4Helpers
{

	public static < A, B, C, D > Function< Tuple4< A, B, C, D >, D > getLast()
	{
		return new GetLast<>();
	}

	public static < A, B, C, D > Function< Tuple4< A, B, C, D >, Tuple3< A, B, C > > dropLast()
	{
		return new DropLast<>();
	}

	private static class GetLast< A, B, C, D > implements Function< Tuple4< A, B, C, D >, D >
	{

		@Override
		public D call( final Tuple4< A, B, C, D > t ) throws Exception
		{
			return t._4();
		}
	}

	private static class DropLast< A, B, C, D > implements Function< Tuple4< A, B, C, D >, Tuple3< A, B, C > >
	{

		@Override
		public Tuple3< A, B, C > call( final Tuple4< A, B, C, D > t ) throws Exception
		{
			return new Tuple3<>( t._1(), t._2(), t._3() );
		}
	}


}
