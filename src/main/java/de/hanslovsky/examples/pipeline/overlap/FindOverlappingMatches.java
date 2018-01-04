package de.hanslovsky.examples.pipeline.overlap;

import java.util.function.BiFunction;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;
import org.janelia.saalfeldlab.n5.AbstractDataBlock;
import org.janelia.saalfeldlab.n5.N5FSWriter;
import org.janelia.saalfeldlab.n5.N5Writer;

import bdv.bigcat.viewer.viewer3d.util.HashWrapper;
import de.hanslovsky.examples.UnionFindSparse;
import gnu.trove.iterator.TLongLongIterator;
import gnu.trove.iterator.TLongObjectIterator;
import gnu.trove.list.array.TLongArrayList;
import gnu.trove.map.hash.TLongLongHashMap;
import gnu.trove.map.hash.TLongObjectHashMap;
import net.imglib2.img.cell.CellGrid;
import scala.Tuple2;

public class FindOverlappingMatches implements Function< HashWrapper< long[] >, Tuple2< long[], long[] > >
{

	public static FindOverlappingMatches onlyUnique(
			final JavaSparkContext sc,
			final String group,
			final Broadcast< CellGrid > wsGridBC,
			final int dimension,
			final String n5TargetUpper,
			final String n5TargetLower )
	{
		return new FindOverlappingMatches( group, wsGridBC, dimension, n5TargetUpper, n5TargetLower, sc.broadcast( new FindOnlyUniqueMatches() ) );
	}

	public static FindOverlappingMatches agreeInBiggestOverlap(
			final JavaSparkContext sc,
			final String group,
			final Broadcast< CellGrid > wsGridBC,
			final int dimension,
			final String n5TargetUpper,
			final String n5TargetLower )
	{
		return new FindOverlappingMatches( group, wsGridBC, dimension, n5TargetUpper, n5TargetLower, sc.broadcast( new FindMatchesAgreementInBiggestOverlap() ) );
	}

	public static FindOverlappingMatches minimumOverlap(
			final JavaSparkContext sc,
			final String group,
			final Broadcast< CellGrid > wsGridBC,
			final int dimension,
			final String n5TargetUpper,
			final String n5TargetLower,
			final int threshold )
	{
		return new FindOverlappingMatches( group, wsGridBC, dimension, n5TargetUpper, n5TargetLower, sc.broadcast( new FindMatchesMinimumOverlap( threshold ) ) );
	}

	private final String group;

	private final Broadcast< CellGrid > wsGridBC;

	private final int dimension;

	private final String n5TargetUpper;

	private final String n5TargetLower;

	private final Broadcast< BiFunction< long[], long[], TLongLongHashMap > > findMatches;

	public FindOverlappingMatches(
			final String group,
			final Broadcast< CellGrid > wsGridBC,
			final int dimension,
			final String n5TargetUpper,
			final String n5TargetLower,
			final Broadcast< BiFunction< long[], long[], TLongLongHashMap > > findMatches )
	{
		super();
		this.group = group;
		this.wsGridBC = wsGridBC;
		this.dimension = dimension;
		this.n5TargetUpper = n5TargetUpper;
		this.n5TargetLower = n5TargetLower;
		this.findMatches = findMatches;
	}

	@SuppressWarnings( "unchecked" )
	@Override
	public Tuple2< long[], long[] > call( final HashWrapper< long[] > offsetWrapper ) throws Exception
	{
		final N5Writer localWriter = new N5FSWriter( group );
		final CellGrid grid = wsGridBC.getValue();
		final long[] cellPos = new long[ grid.numDimensions() ];
		grid.getCellPosition( offsetWrapper.getData().clone(), cellPos );
		final long[] upperCellPos = cellPos.clone();
		upperCellPos[ dimension ] += 1;
		final TLongLongHashMap matches = new TLongLongHashMap();
		if ( upperCellPos[ dimension ] < grid.gridDimension( dimension ) )
		{

			// System.out.println( "READING AT " + Arrays.toString( cellPos ) +
			// " " + Arrays.toString( upperCellPos ) );
			// will need lower plane from block with higher
			// index and vice versa
			final AbstractDataBlock< long[] > upperPlaneInLowerCell = ( AbstractDataBlock< long[] > ) localWriter.readBlock( n5TargetUpper, localWriter.getDatasetAttributes( n5TargetUpper ), cellPos );
			final AbstractDataBlock< long[] > lowerPlaneInUpperCell = ( AbstractDataBlock< long[] > ) localWriter.readBlock( n5TargetLower, localWriter.getDatasetAttributes( n5TargetLower ), upperCellPos );

			final long[] upperData = upperPlaneInLowerCell.getData();
			final long[] lowerData = lowerPlaneInUpperCell.getData();
			matches.putAll( findMatches.getValue().apply( upperData, lowerData ) );
		}
		return new Tuple2<>( matches.keys(), matches.values() );

	}

	private static class FindOnlyUniqueMatches implements BiFunction< long[], long[], TLongLongHashMap >
	{

		@Override
		public TLongLongHashMap apply( final long[] upperData, final long[] lowerData )
		{


			final TLongLongHashMap forwardAssignments = new TLongLongHashMap();
			final TLongLongHashMap backwardAssignments = new TLongLongHashMap();
			final TLongLongHashMap assignmentCounts = new TLongLongHashMap();
			final TLongLongHashMap parents = new TLongLongHashMap();
			final TLongLongHashMap ranks = new TLongLongHashMap();
			final UnionFindSparse localUnionFind = new UnionFindSparse( parents, ranks, 0 );

			// TODO we do not use the assignment counts anywhere, remove?

			for ( int i = 0; i < upperData.length; ++i )
			{
				final long ud = upperData[ i ];
				final long ld = lowerData[ i ];
				// if ( ud == 0 || ld == 0 )
				// System.out.println( "WHY ZERO? " + ud + " " + ld );
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
					localUnionFind.join( localUnionFind.findRoot( k ), localUnionFind.findRoot( v ) );
			}
//			System.out.println( forwardAssignments + " " + backwardAssignments );
			return parents;
		}

	}

	private static class FindMatchesAgreementInBiggestOverlap implements BiFunction< long[], long[], TLongLongHashMap >
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
		public TLongLongHashMap apply( final long[] upperData, final long[] lowerData )
		{

			final TLongObjectHashMap< TLongLongHashMap > forwardCounts = new TLongObjectHashMap<>();
			final TLongObjectHashMap< TLongLongHashMap > backwardCounts = new TLongObjectHashMap<>();

			for ( int i = 0; i < upperData.length; ++i )
			{
				final long ud = upperData[ i ];
				final long ld = lowerData[ i ];
				if ( ud != 0 && ld != 0 ) {
					addMatch( forwardCounts, ud, ld );
					addMatch( backwardCounts, ld, ud );
				}
			}

			final TLongObjectHashMap< TLongArrayList > forwardMaxOverlaps = getMaxOverlap( forwardCounts );
			final TLongObjectHashMap< TLongArrayList > backwardMaxOverlaps = getMaxOverlap( backwardCounts );

			final TLongLongHashMap parents = new TLongLongHashMap();
			final TLongLongHashMap ranks = new TLongLongHashMap();
			final UnionFindSparse uf = new UnionFindSparse( parents, ranks, 0 );

			findUniqueMatches( uf, forwardMaxOverlaps, backwardMaxOverlaps );
			findUniqueMatches( uf, backwardMaxOverlaps, forwardMaxOverlaps );

			for ( final long p : parents.keys() )
				uf.findRoot( p );

			return parents;
		}
	}

	private static class FindMatchesMinimumOverlap implements BiFunction< long[], long[], TLongLongHashMap >
	{

		private final long overlap;

		public FindMatchesMinimumOverlap( final long overlap )
		{
			super();
			this.overlap = overlap;
		}

		private static void addMatch( final TLongObjectHashMap< TLongLongHashMap > counts, final long id1, final long id2 )
		{
			if ( !counts.contains( id1 ) )
				counts.put( id1, new TLongLongHashMap() );
			final TLongLongHashMap c = counts.get( id1 );
			c.put( id2, c.containsKey( id2 ) ? c.get( id2 ) + 1 : 1 );
		}

		private static void filterMatches( final UnionFindSparse uf, final TLongObjectHashMap< TLongLongHashMap > counts, final long threshold ) {
			for ( final TLongObjectIterator< TLongLongHashMap > it = counts.iterator(); it.hasNext(); )
			{
				it.advance();
				for ( final TLongLongIterator otherIt = it.value().iterator(); otherIt.hasNext(); )
				{
					otherIt.advance();
					if ( otherIt.value() > threshold )
						uf.join( uf.findRoot( it.key() ), uf.findRoot( otherIt.key() ) );
				}
			}
		}

		@Override
		public TLongLongHashMap apply( final long[] upperData, final long[] lowerData )
		{

			final TLongObjectHashMap< TLongLongHashMap > forwardCounts = new TLongObjectHashMap<>();
			final TLongObjectHashMap< TLongLongHashMap > backwardCounts = new TLongObjectHashMap<>();

			for ( int i = 0; i < upperData.length; ++i )
			{
				final long ud = upperData[ i ];
				final long ld = lowerData[ i ];
				if ( ud != 0 && ld != 0 ) {
					addMatch( forwardCounts, ud, ld );
					addMatch( backwardCounts, ld, ud );
				}
			}

			final TLongLongHashMap parents = new TLongLongHashMap();
			final TLongLongHashMap ranks = new TLongLongHashMap();
			final UnionFindSparse uf = new UnionFindSparse( parents, ranks, 0 );

			filterMatches( uf, forwardCounts, overlap );
			filterMatches( uf, backwardCounts, overlap );

			for ( final long p : parents.keys() )
				uf.findRoot( p );

			return parents;
		}
	}

//	public static void main( final String[] args )
//	{
//		final long[] vals2 = { 2, 2, 2, 3, 3, 3, 4, 4, 4, 5, 5, 5, 5 };
//		final long[] vals1 = LongStream.generate( () -> 1 ).limit( vals2.length ).toArray();
//		final FindOnlyUniqueMatches matcher1 = new FindOnlyUniqueMatches();
//		final FindMatchesAgreementInBiggestOverlap matcher2 = new FindMatchesAgreementInBiggestOverlap();
//		final FindMatchesMinimumOverlap matcher3 = new FindMatchesMinimumOverlap( 2 );
//		final FindMatchesMinimumOverlap matcher4 = new FindMatchesMinimumOverlap( 3 );
//		final FindMatchesMinimumOverlap matcher5 = new FindMatchesMinimumOverlap( 4 );
//
//		System.out.println( matcher1.apply( vals1, vals2 ) );
//		System.out.println( matcher2.apply( vals1, vals2 ) );
//		System.out.println( matcher3.apply( vals1, vals2 ) );
//		System.out.println( matcher4.apply( vals1, vals2 ) );
//		System.out.println( matcher5.apply( vals1, vals2 ) );
//	}
}
