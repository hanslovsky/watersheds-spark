package org.saalfeldlab.watersheds.pipeline.overlap.hierarchical;

import java.io.File;
import java.io.FileInputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.function.BiFunction;
import java.util.stream.IntStream;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.LongArrayDataBlock;
import org.janelia.saalfeldlab.n5.N5FSWriter;
import org.saalfeldlab.watersheds.UnionFindSparse;

import bdv.bigcat.viewer.viewer3d.util.HashWrapper;
import net.imglib2.Cursor;
import net.imglib2.FinalInterval;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.img.cell.CellGrid;
import net.imglib2.type.numeric.integer.UnsignedLongType;
import net.imglib2.util.Intervals;
import net.imglib2.view.Views;
import scala.Tuple2;

public class ApplyHierarchicalUnionFind
{

	public static void apply(
			final JavaSparkContext sc,
			final JavaPairRDD< HashWrapper< long[] >, RandomAccessibleInterval< UnsignedLongType > > watershedBlocks,
			final CellGrid grid,
			final String group,
			final String mergedBlocksDataset,
			final BiFunction< Integer, long[], String > unionFindSerializationPattern )
	{

		final long[] dims = grid.getImgDimensions();
		final int[] blockSize = IntStream.range( 0, dims.length ).map( grid::cellDimension ).toArray();
		final int multiplier = 2;

		watershedBlocks.map( new DoIt( sc, group, mergedBlocksDataset, grid, dims, blockSize, multiplier, unionFindSerializationPattern ) ).count();


	}

	public static final class DoIt implements Function< Tuple2< HashWrapper< long[] >, RandomAccessibleInterval< UnsignedLongType > >, Boolean >
	{

		private final String group;

		private final String dataset;

		private final Broadcast< CellGrid > grid;

		private final long[] dims;

		private final int[] blockSize;

		private final int multiplier;

		private final BiFunction< Integer, long[], String > serializationPattern;

		public DoIt( final JavaSparkContext sc, final String group, final String dataset, final CellGrid grid, final long[] dims, final int[] blockSize, final int multiplier, final BiFunction< Integer, long[], String > serializationPattern )
		{
			super();
			this.group = group;
			this.dataset = dataset;
			this.grid = sc.broadcast( grid );
			this.dims = dims;
			this.blockSize = blockSize;
			this.multiplier = multiplier;
			this.serializationPattern = serializationPattern;
		}

		@Override
		public Boolean call( final Tuple2< HashWrapper< long[] >, RandomAccessibleInterval< UnsignedLongType > > watershedBlock ) throws Exception
		{
			final HashWrapper< long[] > block = watershedBlock._1();
			final RandomAccessibleInterval< UnsignedLongType > data = watershedBlock._2();
			final N5FSWriter writer = new N5FSWriter( group );
			final DatasetAttributes attrsOut = writer.getDatasetAttributes( dataset );

			final int[] bs = blockSize.clone();

			final UnionFindSparse uf = new UnionFindSparse();

			final ArrayList< long[] > cellPositions = new ArrayList<>();

			for ( int factor = 2; HierarchicalUnionFindInOverlaps.checkIfMoreThanOneBlock( dims, bs ); factor *= multiplier )
			{
				final long[] position = block.getData().clone();
				final long[] cellPos = new long[ position.length ];
				grid.getValue().getCellPosition( position, cellPos );
				for ( int d = 0; d < bs.length; ++d )
				{
					bs[ d ] *= multiplier;
					cellPos[ d ] /= factor;
				}
				cellPositions.add( cellPos );
				final File f = new File( serializationPattern.apply( factor, cellPos ) );
				try (final FileInputStream fis = new FileInputStream( f ))
				{
					final byte[] fileData = new byte[ ( int ) f.length() ];
					fis.read( fileData );
					final ByteBuffer wrappedData = ByteBuffer.wrap( fileData );
					final int numMatches = wrappedData.getInt();

					final long[] keys = new long[ numMatches ];
					final long[] values = new long[ numMatches ];

					for ( int i = 0; i < numMatches; ++i )
						keys[ i ] = wrappedData.getLong();

					for ( int i = 0; i < numMatches; ++i )
						values[ i ] = wrappedData.getLong();

					for ( int i = 0; i < numMatches; ++i )
					{
						final long r1 = uf.findRoot( keys[ i ] );
						final long r2 = uf.findRoot( values[ i ] );
						uf.join( r1, r2 );
					}

				}
			}

			final long[] min = block.getData().clone();
			final long[] max = new long[ data.numDimensions() ];
			for ( int d = 0; d < max.length; ++d )
				max[ d ] = Math.min( min[ d ] + grid.getValue().cellDimension( d ), grid.getValue().imgDimension( d ) ) - 1;
			final FinalInterval fi = new FinalInterval( min, max );

			final int[] dataBlockSize = Intervals.dimensionsAsIntArray( fi );
			final long[] dataArray = new long[ Arrays.stream( dataBlockSize ).reduce( 1, ( i1, i2 ) -> i1 * i2 ) ];
			final Cursor< UnsignedLongType > dataCursor = Views.flatIterable( Views.interval( data, fi ) ).cursor();
			for ( int i = 0; dataCursor.hasNext(); ++i )
			{
				final long v = dataCursor.next().getIntegerLong();
				if ( v != 0 )
				{
					final long r = uf.findRoot( v );
					dataArray[ i ] = r;
				}
			}
			final long[] cellPos = block.getData().clone();
			grid.getValue().getCellPosition( block.getData().clone(), cellPos );
			writer.writeBlock( dataset, attrsOut, new LongArrayDataBlock( dataBlockSize, cellPos, dataArray ) );

			return true;
		}

	}

}
