package org.saalfeldlab.watersheds.pipeline.overlap;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.janelia.saalfeldlab.n5.CompressionType;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.N5FSWriter;
import org.janelia.saalfeldlab.n5.N5Writer;
import org.saalfeldlab.watersheds.UnionFindSparse;
import org.saalfeldlab.watersheds.pipeline.Write;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import bdv.bigcat.viewer.viewer3d.util.HashWrapper;
import gnu.trove.map.hash.TLongLongHashMap;
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

		final TLongLongHashMap sparseUFParents = new TLongLongHashMap();
		final TLongLongHashMap sparseUFRanks = new TLongLongHashMap();
		final UnionFindSparse sparseUnionFind = new UnionFindSparse( sparseUFParents, sparseUFRanks, 0 );

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

			LOG.debug( "DIMENSION " + d );

			final List< Tuple2< long[], long[] > > assignments = remapped
					.keys()
					.map( FindOverlappingMatches.agreeInBiggestOverlap( sc, tmpGroup, wsGridBC, finalD, n5TargetUpper, n5TargetLower ) )
					//					.map( FindOverlappingMatches.minimumOverlap( sc, writerBC, wsGridBC, finalD, n5TargetUpper, n5TargetLower, 10 ) )
					.collect();


			final long numAssignments = assignments.stream().mapToLong( t -> t._1().length ).sum();
			LOG.warn( "Got {} assignments.", numAssignments );
			for ( final Tuple2< long[], long[] > assignment : assignments )
			{
				final long[] keys = assignment._1();
				final long[] vals = assignment._2();
				LOG.debug( "Got matches: {} {}", keys, vals );
				for ( int i = 0; i < keys.length; ++i )
					sparseUnionFind.join( sparseUnionFind.findRoot( keys[i] ), sparseUnionFind.findRoot( vals[i] ) );
			}
		}

		final int setCount = sparseUnionFind.setCount();
		final Broadcast< Tuple2< long[], long[] > > parentsBC = sc.broadcast( new Tuple2<>( sparseUFParents.keys(), sparseUFParents.values() ) );
		final Broadcast< Tuple2< long[], long[] > > ranksBC = sc.broadcast( new Tuple2<>( sparseUFRanks.keys(), sparseUFRanks.values() ) );
//		System.out.println( "SPARSE PARENTS ARE " + sparseUFParents );


		remapped
		.mapToPair( new CropBlocks<>( wsGridBC ) )
		.mapValues( new RemapMatching<>( parentsBC, ranksBC, setCount ) )
		.map( new Write<>( sc, group, n5Target, wsGrid ) )
		.count()
		;

	}
}
