package org.saalfeldlab.watersheds.pipeline.overlap.match;

import java.util.function.BiConsumer;

import org.saalfeldlab.watersheds.UnionFindSparse;

import scala.Tuple2;

public class FindAllMatches implements BiConsumer< Tuple2< long[], long[] >, UnionFindSparse >
{

	@Override
	public void accept( final Tuple2< long[], long[] > data, final UnionFindSparse uf )
	{

		final long[] lowerData = data._1();
		final long[] upperData = data._2();

		for ( int i = 0; i < upperData.length; ++i )
		{
			final long ud = upperData[ i ];
			final long ld = lowerData[ i ];
			if ( ud != 0 && ld != 0 )
			{
				final long r1 = uf.findRoot( ud );
				final long r2 = uf.findRoot( ld );
				uf.join( r1, r2 );
			}
		}

	}
}
