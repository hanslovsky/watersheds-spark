package org.saalfeldlab.watersheds.pipeline.overlap;

import java.io.IOException;
import java.io.Serializable;
import java.lang.invoke.MethodHandles;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.janelia.saalfeldlab.n5.CompressionType;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.N5FSWriter;
import org.janelia.saalfeldlab.n5.N5Writer;
import org.saalfeldlab.watersheds.UnionFindSparse;
import org.saalfeldlab.watersheds.pipeline.overlap.hierarchical.ApplyHierarchicalUnionFind;
import org.saalfeldlab.watersheds.pipeline.overlap.hierarchical.HierarchicalUnionFindInOverlaps;
import org.saalfeldlab.watersheds.pipeline.overlap.match.FindMatchesAgreementInBiggestOverlap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import bdv.bigcat.viewer.viewer3d.util.HashWrapper;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.img.cell.CellGrid;
import net.imglib2.type.numeric.integer.UnsignedLongType;
import scala.Tuple2;

public class MergeOverlappingBlocks
{

	private static final Logger LOG = LoggerFactory.getLogger( MethodHandles.lookup().lookupClass() );

	public static void mergeOverlap(
			final JavaSparkContext sc,
			final JavaPairRDD< HashWrapper< long[] >, RandomAccessibleInterval< UnsignedLongType > > remapped,
			final String group,
			final String tmpGroup,
			final String n5DatasetPatternUpper,
			final String n5DatasetPatternLower,
			final String n5Target,
			final CellGrid wsGrid
			) throws IOException
	{

		final Broadcast< CellGrid > wsGridBC = sc.broadcast( wsGrid );
		final Broadcast< UnsignedLongType > invalidExtensionBC = sc.broadcast( new UnsignedLongType( 0 ) );

		for ( int d = 0; d < wsGrid.numDimensions(); ++d ) {

			final int finalD = d;
			final String n5TargetUpper = String.format( n5DatasetPatternUpper, d );
			// n5Target + "-upper-" + d;
			final String n5TargetLower = String.format( n5DatasetPatternLower, d );
			// n5Target + "-lower-" + d;

			// size along current dimension is equal to number of cells in this
			// dimension (also, cell size along this dimension is 1)
			final long[] imgSize = wsGrid.getImgDimensions();
			imgSize[ d ] = wsGrid.gridDimension( d );
			final int[] blockSize = new int[ wsGrid.numDimensions() ];
			for ( int k = 0; k < blockSize.length; ++k )
				blockSize[ k ] = k == d ? 1 : wsGrid.cellDimension( k );

			final N5Writer tmpWriter = new N5FSWriter( tmpGroup );
			tmpWriter.createDataset( n5TargetUpper, imgSize, blockSize, DataType.UINT64, CompressionType.RAW );
			tmpWriter.createDataset( n5TargetLower, imgSize, blockSize, DataType.UINT64, CompressionType.RAW );
			remapped.map( new StoreRelevantHyperslices<>( wsGridBC, tmpGroup, invalidExtensionBC, finalD, n5TargetUpper, n5TargetLower ) ).count();
		}

		final BiConsumer< Tuple2< long[], long[] >, UnionFindSparse > matcher = new FindMatchesAgreementInBiggestOverlap();
		HierarchicalUnionFindInOverlaps.createOverlaps(
				sc,
				wsGrid,
				matcher,
				tmpGroup,
				n5DatasetPatternUpper,
				n5DatasetPatternLower,
				new UnionFindSerializationPattern( tmpGroup ) );

		ApplyHierarchicalUnionFind.apply( sc, remapped, wsGrid, group, n5Target, new UnionFindSerializationPattern( tmpGroup ) );
	}

	public static class UnionFindSerializationPattern implements BiFunction< Integer, long[], String >, Serializable
	{

		private final String group;

		public UnionFindSerializationPattern( final String group )
		{
			super();
			this.group = group;
		}

		@Override
		public String apply( final Integer factor, final long[] position  )
		{
			return group + "/unionfind/" + factor + "/" + String.format( "%d/%d/%d/", position[ 0 ], position[ 1 ], position[ 2 ] );
		}

	}
}
