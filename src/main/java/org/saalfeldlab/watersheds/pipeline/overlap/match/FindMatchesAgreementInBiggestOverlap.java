package org.saalfeldlab.watersheds.pipeline.overlap.match;

import java.util.function.BiConsumer;

import org.saalfeldlab.watersheds.UnionFindSparse;

import gnu.trove.iterator.TLongLongIterator;
import gnu.trove.iterator.TLongObjectIterator;
import gnu.trove.list.array.TLongArrayList;
import gnu.trove.map.hash.TLongLongHashMap;
import gnu.trove.map.hash.TLongObjectHashMap;
import scala.Tuple2;

public class FindMatchesAgreementInBiggestOverlap implements BiConsumer< Tuple2< long[], long[] >, UnionFindSparse >
{

	private static void addMatch( final TLongObjectHashMap< TLongLongHashMap > counts, final long id1, final long id2 )
	{
		if ( !counts.contains( id1 ) )
			counts.put( id1, new TLongLongHashMap() );
		final TLongLongHashMap c = counts.get( id1 );
		c.put( id2, c.containsKey( id2 ) ? c.get( id2 ) + 1 : 1 );
	}

	private static TLongObjectHashMap< TLongArrayList > getMaxOverlap( final TLongObjectHashMap< TLongLongHashMap > counts )
	{
		final TLongObjectHashMap< TLongArrayList > overlapArgMax = new TLongObjectHashMap<>();

		for ( final TLongObjectIterator< TLongLongHashMap > it = counts.iterator(); it.hasNext(); )
		{
			it.advance();
			final long id1 = it.key();
			final TLongArrayList matchesList = new TLongArrayList();
			overlapArgMax.put( id1, matchesList );
			long maxCount = 0;
			for ( final TLongLongIterator otherIt = it.value().iterator(); otherIt.hasNext(); )
			{
				otherIt.advance();
				final long id2 = otherIt.key();
				final long count = otherIt.value();
				if ( count > maxCount )
				{
					maxCount = count;
					matchesList.clear();
					matchesList.add( id2 );
				}
				else if ( count == maxCount )
					matchesList.add( id2 );
			}
		}

		return overlapArgMax;
	}

	private static void findUniqueMatches(
			final UnionFindSparse uf,
			final TLongObjectHashMap< TLongArrayList > forwardMaxOverlaps,
			final TLongObjectHashMap< TLongArrayList > backwardMaxOverlaps )
	{
		for ( final TLongObjectIterator< TLongArrayList > it = forwardMaxOverlaps.iterator(); it.hasNext(); )
		{
			it.advance();
			final TLongArrayList list1 = it.value();
			if ( list1 != null && list1.size() == 1 )
			{
				final long id1 = it.key();
				final long id2 = list1.get( 0 );
				final TLongArrayList list2 = backwardMaxOverlaps.get( id2 );
				if ( list2 != null && list2.size() == 1 && list2.get( 0 ) == id1 )
					uf.join( uf.findRoot( id1 ), uf.findRoot( id2 ) );
			}
		}
	}

	@Override
	public void accept( final Tuple2< long[], long[] > data, final UnionFindSparse uf )
	{

		final TLongObjectHashMap< TLongLongHashMap > forwardCounts = new TLongObjectHashMap<>();
		final TLongObjectHashMap< TLongLongHashMap > backwardCounts = new TLongObjectHashMap<>();

		final long[] lowerData = data._1();
		final long[] upperData = data._2();

		for ( int i = 0; i < upperData.length; ++i )
		{
			final long ud = upperData[ i ];
			final long ld = lowerData[ i ];
			if ( ud != 0 && ld != 0 )
			{
				addMatch( forwardCounts, ud, ld );
				addMatch( backwardCounts, ld, ud );
			}
		}

		final TLongObjectHashMap< TLongArrayList > forwardMaxOverlaps = getMaxOverlap( forwardCounts );
		final TLongObjectHashMap< TLongArrayList > backwardMaxOverlaps = getMaxOverlap( backwardCounts );

		final TLongLongHashMap parents = new TLongLongHashMap();
		final TLongLongHashMap ranks = new TLongLongHashMap();

		findUniqueMatches( uf, forwardMaxOverlaps, backwardMaxOverlaps );
		// should not be necessary because only symmetric matches are valid
//			findUniqueMatches( uf, backwardMaxOverlaps, forwardMaxOverlaps );

		for ( final long p : parents.keys() )
			uf.findRoot( p );

	}
}
