package org.saalfeldlab.watersheds.pipeline;

import java.lang.invoke.MethodHandles;
import java.util.function.Predicate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.imglib2.type.numeric.RealType;

public class Threshold
{

	private static final Logger LOG = LoggerFactory.getLogger( MethodHandles.lookup().lookupClass() );

	public static < T extends RealType< T > > Predicate< T > threshold( final double threshold, final boolean lessThan )
	{
		return lessThan ? new LessThanThreshold<>( threshold ) : new MoreThanThreshold<>( threshold );
	}

	protected static class MoreThanThreshold< T extends RealType< T > > implements Predicate< T >
	{

		private final double threshold;

		public MoreThanThreshold( final double threshold )
		{
			super();
			LOG.debug( "Creating {} with threshold={}", this.getClass().getSimpleName(), threshold );
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
			LOG.debug( "Creating {} with threshold={}", this.getClass().getSimpleName(), threshold );
			this.threshold = threshold;
		}

		@Override
		public boolean test( final T t )
		{
			return t.getRealDouble() < threshold;
		}
	}
}
