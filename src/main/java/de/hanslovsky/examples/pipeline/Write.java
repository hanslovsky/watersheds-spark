package de.hanslovsky.examples.pipeline;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;
import org.janelia.saalfeldlab.n5.N5Writer;
import org.janelia.saalfeldlab.n5.imglib2.N5Utils;

import bdv.bigcat.viewer.viewer3d.util.HashWrapper;
import net.imglib2.FinalInterval;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.img.cell.CellGrid;
import net.imglib2.type.NativeType;
import net.imglib2.view.Views;
import scala.Tuple2;

public class Write< T extends NativeType< T > > implements Function< Tuple2< HashWrapper< long[] >, RandomAccessibleInterval< T > >, Boolean >
{

	private final Broadcast< N5Writer > writer;

	private final Broadcast< String > dataset;

	private final Broadcast< CellGrid > grid;

	public Write( final JavaSparkContext sc, final N5Writer writer, final String dataset, final CellGrid grid )
	{
		super();
		this.writer = sc.broadcast( writer );
		this.dataset = sc.broadcast( dataset );
		this.grid = sc.broadcast( grid );
	}

	@Override
	public Boolean call( final Tuple2< HashWrapper< long[] >, RandomAccessibleInterval< T > > t ) throws Exception
	{
		final long[] min = t._1().getData().clone();

		final CellGrid grid = this.grid.getValue();
		final N5Writer writer = this.writer.getValue();
		final String dataset = this.dataset.getValue();

		assert min.length == grid.numDimensions();

		final int[] blockSize = new int[ min.length ];
		grid.cellDimensions( blockSize );

		final long[] blockPosition = new long[ min.length ];
		final long[] max = new long[ min.length ];
		for ( int d = 0; d < max.length; ++d )
		{
			max[ d ] = Math.min( min[ d ] + blockSize[ d ], grid.imgDimension( d ) ) - 1;
			blockPosition[ d ] = min[ d ] / blockSize[ d ];
		}
		N5Utils.saveBlock( Views.interval( t._2(), new FinalInterval( min, max ) ), writer, dataset, blockPosition );

		return true;
	}

}
