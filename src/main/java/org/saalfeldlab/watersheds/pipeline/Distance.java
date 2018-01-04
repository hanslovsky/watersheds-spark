package org.saalfeldlab.watersheds.pipeline;

import java.util.function.ToDoubleBiFunction;

import net.imglib2.type.numeric.RealType;

public class Distance
{

	public static < T extends RealType< T > > ToDoubleBiFunction< T, T > get( final boolean invert )
	{
		return invert ? new InvertedDistance<>() : new RegularDistance<>();
	}

	private static class InvertedDistance< T extends RealType< T > > implements ToDoubleBiFunction< T, T >
	{

		@Override
		public double applyAsDouble( final T arg0, final T arg1 )
		{
			return -arg0.getRealDouble();
		}

	}

	private static class RegularDistance< T extends RealType< T > > implements ToDoubleBiFunction< T, T >
	{

		@Override
		public double applyAsDouble( final T arg0, final T arg1 )
		{
			return arg0.getRealDouble();
		}

	}

}
