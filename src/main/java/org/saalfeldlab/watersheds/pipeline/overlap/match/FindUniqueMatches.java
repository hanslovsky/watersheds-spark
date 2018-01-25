package org.saalfeldlab.watersheds.pipeline.overlap.match;

import java.util.function.BiConsumer;

import org.saalfeldlab.watersheds.UnionFindSparse;

import gnu.trove.iterator.TLongLongIterator;
import gnu.trove.map.hash.TLongLongHashMap;
import scala.Tuple2;

public class FindUniqueMatches implements BiConsumer< Tuple2< long[], long[] >, UnionFindSparse >
{

	@Override
	public void accept( final Tuple2< long[], long[] > data, final UnionFindSparse uf )
	{

		final long[] lowerData = data._1();
		final long[] upperData = data._2();

		final TLongLongHashMap forwardAssignments = new TLongLongHashMap();
		final TLongLongHashMap backwardAssignments = new TLongLongHashMap();
		final TLongLongHashMap assignmentCounts = new TLongLongHashMap();

		// TODO we do not use the assignment counts anywhere, remove?

		for ( int i = 0; i < upperData.length; ++i )
		{
			final long ud = upperData[ i ];
			final long ld = lowerData[ i ];
			if ( ud != 0 && ld != 0 )
				if ( forwardAssignments.contains( ud ) && backwardAssignments.contains( ld ) )
				{
					// found non-unique overlap (previous assignments do not
					// agree with what we see now)
					if ( forwardAssignments.get( ud ) != ld || backwardAssignments.get( ld ) != ud )
					{
						forwardAssignments.put( ud, -1 );
						backwardAssignments.put( ld, -1 );
					}
					else
					{
						assignmentCounts.put( ud, assignmentCounts.get( ud ) + 1 );
						assignmentCounts.put( ld, assignmentCounts.get( ld ) + 1 );
					}
				}
				else if ( forwardAssignments.contains( ud ) )
				{
					if ( forwardAssignments.get( ud ) != ld )
						forwardAssignments.put( ud, -1 );
					backwardAssignments.put( ld, ud );
				}
				else if ( backwardAssignments.contains( ld ) )
				{
					if ( backwardAssignments.get( ld ) != ud )
						backwardAssignments.put( ld, -1 );
					forwardAssignments.put( ud, ld );
				}

				else
				{
					forwardAssignments.put( ud, ld );
					backwardAssignments.put( ld, ud );
					assignmentCounts.put( ud, 1 );
				}
		}
		for ( final TLongLongIterator it = forwardAssignments.iterator(); it.hasNext(); )
		{
			it.advance();
			final long k = it.key();
			final long v = it.value();
			if ( v != -1 && backwardAssignments.get( v ) == k )
				uf.join( uf.findRoot( k ), uf.findRoot( v ) );
		}

	}
}
