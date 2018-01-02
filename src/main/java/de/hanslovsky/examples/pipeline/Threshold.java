package de.hanslovsky.examples.pipeline;

import java.util.function.Predicate;

import net.imglib2.type.numeric.RealType;

public class Threshold
{
	public static < T extends RealType< T > > Predicate< T > threshold( final double threshold, final boolean lessThan )
	{
		return Double.isFinite( threshold ) ? getThreshold( threshold, lessThan ) : new NoThreshold<>();
	}

	private static < T extends RealType< T > > Predicate< T > getThreshold( final double threshold, final boolean lessThan )
	{
		return lessThan ? new LessThanThreshold<>( threshold ) : new MoreThanThreshold<>( threshold );
	}

	private static class NoThreshold< T > implements Predicate< T >
	{

		@Override
		public boolean test( final T t )
		{
			return false;
		}
	}

	protected static class MoreThanThreshold< T extends RealType< T > > implements Predicate< T >
	{

		private final double threshold;

		public MoreThanThreshold( final double threshold )
		{
			super();
			this.threshold = threshold;
		}

		@Override
		public boolean test( final T t )
		{
			return t.getRealDouble() > threshold;
		}
	}

	protected static class LessThanThreshold< T extends RealType< T > > implements Predicate< T >
	{

		private final double threshold;

		public LessThanThreshold( final double threshold )
		{
			super();
			this.threshold = threshold;
		}

		@Override
		public boolean test( final T t )
		{
			return t.getRealDouble() < threshold;
		}
	}
}
