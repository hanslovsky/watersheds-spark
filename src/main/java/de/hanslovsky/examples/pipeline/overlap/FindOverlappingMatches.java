package de.hanslovsky.examples.pipeline.overlap;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;
import org.janelia.saalfeldlab.n5.AbstractDataBlock;
import org.janelia.saalfeldlab.n5.N5Writer;

import bdv.bigcat.viewer.viewer3d.util.HashWrapper;
import de.hanslovsky.examples.UnionFindSparse;
import gnu.trove.iterator.TLongLongIterator;
import gnu.trove.map.hash.TLongLongHashMap;
import net.imglib2.img.cell.CellGrid;
import scala.Tuple2;

public class FindOverlappingMatches implements Function< HashWrapper< long[] >, Tuple2< long[], long[] > >
{

	private final Broadcast< N5Writer > writerBC;

	private final Broadcast< CellGrid > wsGridBC;

	private final int dimension;

	private final String n5TargetUpper;

	private final String n5TargetLower;

	public FindOverlappingMatches( final Broadcast< N5Writer > writerBC, final Broadcast< CellGrid > wsGridBC, final int dimension, final String n5TargetUpper, final String n5TargetLower )
	{
		super();
		this.writerBC = writerBC;
		this.wsGridBC = wsGridBC;
		this.dimension = dimension;
		this.n5TargetUpper = n5TargetUpper;
		this.n5TargetLower = n5TargetLower;
	}

	@SuppressWarnings( "unchecked" )
	@Override
	public Tuple2< long[], long[] > call( final HashWrapper< long[] > offsetWrapper ) throws Exception
	{
		final long[] offset = offsetWrapper.getData().clone();
		final N5Writer localWriter = writerBC.getValue();
		final TLongLongHashMap parents = new TLongLongHashMap();
		final TLongLongHashMap ranks = new TLongLongHashMap();
		final UnionFindSparse localUnionFind = new UnionFindSparse( parents, ranks, 0 );
		final CellGrid grid = wsGridBC.getValue();
		// if ( offset[ finalD ] + grid.cellDimension( finalD ) <
		// grid.imgDimension( finalD ) )
		// {
		final long[] cellPos = new long[ offset.length ];
		grid.getCellPosition( offset, cellPos );
		final long[] upperCellPos = cellPos.clone();
		upperCellPos[ dimension ] += 1;
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

			final TLongLongHashMap forwardAssignments = new TLongLongHashMap();
			final TLongLongHashMap backwardAssignments = new TLongLongHashMap();
			final TLongLongHashMap assignmentCounts = new TLongLongHashMap();

			for ( int i = 0; i < upperData.length; ++i )
			{
				final long ud = upperData[ i ];
				final long ld = lowerData[ i ];
				// if ( ud == 0 || ld == 0 )
				// System.out.println( "WHY ZERO? " + ud + " " + ld );
				if ( ud != 0 && ld != 0 )
					if ( forwardAssignments.contains( ud ) && backwardAssignments.contains( ld ) )
					{
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
		}
		// }
		return new Tuple2<>( parents.keys(), parents.values() );
	}

}
