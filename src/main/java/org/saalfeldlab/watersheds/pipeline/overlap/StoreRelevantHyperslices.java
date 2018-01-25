package org.saalfeldlab.watersheds.pipeline.overlap;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;
import org.janelia.saalfeldlab.n5.N5FSWriter;
import org.janelia.saalfeldlab.n5.N5Writer;
import org.janelia.saalfeldlab.n5.imglib2.N5Utils;

import bdv.bigcat.viewer.viewer3d.util.HashWrapper;
import net.imglib2.FinalInterval;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.img.cell.CellGrid;
import net.imglib2.type.NativeType;
import net.imglib2.util.ConstantUtils;
import net.imglib2.view.Views;
import scala.Tuple2;

public class StoreRelevantHyperslices< T extends NativeType< T > > implements Function< Tuple2< HashWrapper< long[] >, RandomAccessibleInterval< T > >, Boolean >
{

	private final Broadcast< CellGrid > gridBC;

	private final String group;

	private final Broadcast< T > invalidExtension;

	private final int dimension;

	private final String n5TargetUpper;

	private final String n5TargetLower;

	public StoreRelevantHyperslices( final Broadcast< CellGrid > wsGridBC, final String group, final Broadcast< T > invalidExtension, final int dimension, final String n5TargetUpper, final String n5TargetLower )
	{
		super();
		this.gridBC = wsGridBC;
		this.group = group;
		this.invalidExtension = invalidExtension;
		this.dimension = dimension;
		this.n5TargetUpper = n5TargetUpper;
		this.n5TargetLower = n5TargetLower;
	}

	@Override
	public Boolean call( final Tuple2< HashWrapper< long[] >, RandomAccessibleInterval< T > > t ) throws Exception
	{

		final CellGrid grid = gridBC.getValue();
		final N5Writer writer = new N5FSWriter( group );
		final RandomAccessibleInterval< T > rai = t._2();
		final long[] pos = t._1().getData().clone();
		final long[] min = pos.clone();
		final long[] max = new long[ min.length ];
		for ( int k = 0; k < max.length; ++k )
			max[ k ] = Math.min( min[ k ] + grid.cellDimension( k ), grid.imgDimension( k ) ) - 1;
		max[ dimension ] += 1;

		// create view of last plane along finalD within interval
		min[ dimension ] = max[ dimension ];
		final FinalInterval lastPlaneInterval = new FinalInterval( min, max );
		final RandomAccessibleInterval< T > upper;
		if ( max[ dimension ] > rai.max( dimension ) )
			upper = Views.zeroMin( ConstantUtils.constantRandomAccessibleInterval( invalidExtension.getValue(), min.length, lastPlaneInterval ) );
		else
			upper = Views.zeroMin( Views.interval( rai, lastPlaneInterval ) );

		// create view of first plane along finalD within interval
		min[ dimension ] = pos[ dimension ];
		max[ dimension ] = min[ dimension ];
		final FinalInterval firstPlaneInterval = new FinalInterval( min, max );
		final RandomAccessibleInterval< T > lower = Views.zeroMin( Views.interval( rai, firstPlaneInterval ) );

		final long[] cellPos = new long[ pos.length ];
		grid.getCellPosition( pos, cellPos );
		N5Utils.saveBlock( upper, writer, n5TargetUpper, cellPos );
		N5Utils.saveBlock( lower, writer, n5TargetLower, cellPos );

		return true;
	}
}
