package de.hanslovsky.examples;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;

public class Util
{

	public static String HOME_DIR = System.getProperty( "user.home" );

	public static int[] getFlipPermutation( final int numDimensions )
	{
		final int[] perm = new int[ numDimensions ];
		for ( int d = 0, flip = numDimensions - 1; d < numDimensions; ++d, --flip )
			perm[ d ] = flip;
		return perm;
	}

	public static class ToStringFrom
	{

		private final Supplier< String > toString;

		public ToStringFrom( final Supplier< String > toString )
		{
			super();
			this.toString = toString;
		}

		@Override
		public String toString()
		{
			return toString.get();
		}

	}

	public static < T > List< T > collectAllOffsets( final long[] dimensions, final int[] blockSize, final Function< long[], T > func )
	{
		final List< T > blocks = new ArrayList<>();
		final int nDim = dimensions.length;
		final long[] offset = new long[ nDim ];
		for ( int d = 0; d < nDim; )
		{
			final long[] target = offset.clone();
			blocks.add( func.apply( target ) );
			for ( d = 0; d < nDim; ++d )
			{
				offset[ d ] += blockSize[ d ];
				if ( offset[ d ] < dimensions[ d ] )
					break;
				else
					offset[ d ] = 0;
			}
		}

		return blocks;
	}


}
