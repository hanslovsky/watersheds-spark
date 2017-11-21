package de.hanslovsky.examples;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.Arrays;
import java.util.List;
import java.util.function.Predicate;
import java.util.function.ToDoubleBiFunction;
import java.util.stream.IntStream;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.serializer.KryoRegistrator;
import org.apache.spark.storage.StorageLevel;
import org.janelia.saalfeldlab.n5.AbstractDataBlock;
import org.janelia.saalfeldlab.n5.CompressionType;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.N5FSReader;
import org.janelia.saalfeldlab.n5.N5FSWriter;
import org.janelia.saalfeldlab.n5.imglib2.N5CellLoader;
import org.janelia.saalfeldlab.n5.imglib2.N5Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import bdv.bigcat.viewer.viewer3d.util.HashWrapper;
import gnu.trove.map.hash.TLongLongHashMap;
import gnu.trove.map.hash.TObjectLongHashMap;
import gnu.trove.set.hash.TLongHashSet;
import net.imglib2.FinalInterval;
import net.imglib2.Interval;
import net.imglib2.RandomAccessible;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.algorithm.morphology.watershed.HierarchicalPriorityQueueQuantized;
import net.imglib2.algorithm.morphology.watershed.HierarchicalPriorityQueueQuantized.Factory;
import net.imglib2.algorithm.morphology.watershed.PriorityQueueFactory;
import net.imglib2.cache.Cache;
import net.imglib2.cache.img.CachedCellImg;
import net.imglib2.cache.img.CellLoader;
import net.imglib2.cache.img.LoadedCellCacheLoader;
import net.imglib2.cache.ref.SoftRefLoaderCache;
import net.imglib2.img.array.ArrayImgs;
import net.imglib2.img.basictypeaccess.array.ArrayDataAccess;
import net.imglib2.img.basictypeaccess.array.FloatArray;
import net.imglib2.img.cell.Cell;
import net.imglib2.img.cell.CellGrid;
import net.imglib2.type.NativeType;
import net.imglib2.type.Type;
import net.imglib2.type.numeric.RealType;
import net.imglib2.type.numeric.integer.UnsignedLongType;
import net.imglib2.type.numeric.real.FloatType;
import net.imglib2.util.Intervals;
import net.imglib2.view.Views;
import net.imglib2.view.composite.Composite;
import scala.Tuple2;

public class WatershedsOnDistanceTransform
{

	private static final Logger LOG = LoggerFactory.getLogger( MethodHandles.lookup().lookupClass() );

	public static < T extends RealType< T > & NativeType< T >, A extends ArrayDataAccess< A > > JavaRDD< Tuple2< RandomAccessibleInterval< UnsignedLongType >, Long > > flood(
			final JavaSparkContext sc,
			final CellLoader< T > affinityLoader,
			final CellGrid affinitiesGrid,
			final CellGrid watershedsGrid,
			final T extension,
			final A access,
			final boolean processBySlice,
			final Predicate< T > threshold,
			final ToDoubleBiFunction< T, T > dist,
			final PriorityQueueFactory factory )
	{
		final Broadcast< CellGrid > gridBroadcast = sc.broadcast( affinitiesGrid );
		final Broadcast< CellLoader< T > > loaderBroadcast = sc.broadcast( affinityLoader );

		final List< HashWrapper< long[] > > offsets = Util.collectAllOffsets(
				watershedsGrid.getImgDimensions(),
				IntStream.range( 0, watershedsGrid.numDimensions() ).map( d -> watershedsGrid.cellDimension( d ) ).toArray(),
				a -> HashWrapper.longArray( a ) );

		final JavaPairRDD< HashWrapper< long[] >, RandomAccessible< T > > affinities = sc
				.parallelize( offsets )
				.mapToPair( new RAIFromLoader< T, A >( sc, gridBroadcast, loaderBroadcast, extension, access ) )
				.mapValues( new Extend<>( sc.broadcast( extension ) ) );
		return flood( sc, affinities, watershedsGrid, processBySlice, threshold, dist, factory );
	}

	public static < T extends RealType< T > & NativeType< T > > JavaRDD< Tuple2< RandomAccessibleInterval< UnsignedLongType >, Long > > flood(
			final JavaSparkContext sc,
			final JavaPairRDD< HashWrapper< long[] >, RandomAccessible< T > > affinities,
			final CellGrid watershedsGrid,
			final boolean processBySlice,
			final Predicate< T > threshold,
			final ToDoubleBiFunction< T, T > dist,
			final PriorityQueueFactory factory )
	{
		final RunWatershedsOnDistanceTransform< T, UnsignedLongType > rw = new RunWatershedsOnDistanceTransform<>( sc, processBySlice, threshold, dist, new UnsignedLongType(), factory );
		final JavaRDD< Tuple2< RandomAccessibleInterval< UnsignedLongType >, Long > > mapped = affinities
				.map( new ToInterval<>( sc, watershedsGrid ) )
				.map( rw );
		return mapped;
	}

	public static void main( final String[] args ) throws IOException
	{

		final String n5Path = "/groups/saalfeld/home/hanslovskyp/from_heinrichl/distance/gt/n5";
		final String n5Dataset = "distance-transform";
		final String n5TargetNonBlocked = "spark-supervoxels";
		final String n5Target = "spark-supervoxels-merged";
		final int[] wsBlockSize = new int[] { 25, 25, 25 };
		final double minVal = 0;// 0.035703465;
		final double maxVal = 1.0;// 1.4648689;
		final double threshold = 0.9;// 0.5 * ( minVal + maxVal );
		final boolean processSliceBySlice = false;

		final Factory queueFactory = new HierarchicalPriorityQueueQuantized.Factory( 256, -maxVal, -minVal );

		final N5FSReader reader = new N5FSReader( n5Path );
		final DatasetAttributes attrs = reader.getDatasetAttributes( n5Dataset );
		final CellGrid affinitiesGrid = new CellGrid( attrs.getDimensions(), attrs.getBlockSize() );
		final N5CellLoader< FloatType > affinitiesLoader = new N5CellLoader<>( reader, n5Dataset, attrs.getBlockSize() );

		final CellGrid wsGrid = new CellGrid( Arrays.stream( attrs.getDimensions() ).limit( 3 ).toArray(), wsBlockSize );

		final SparkConf conf = new SparkConf()
				.setAppName( MethodHandles.lookup().lookupClass().getName() )
				.setMaster( "local[*]" )
				.set( "spark.serializer", "org.apache.spark.serializer.KryoSerializer" )
				.set( "spark.kryo.registrator", Registrator.class.getName() )
				;

		final JavaSparkContext sc = new JavaSparkContext( conf );
		sc.setLogLevel( "ERROR" );

		final Predicate< FloatType > thresholdPredicate = c -> c.getRealDouble() > threshold;

		final ToDoubleBiFunction< FloatType, FloatType > dist = ( c, r ) -> c.getRealDouble() > minVal ? -c.getRealDouble() : Double.POSITIVE_INFINITY;

		final JavaRDD< Tuple2< RandomAccessibleInterval< UnsignedLongType >, Long > > data = flood(
				sc,
				affinitiesLoader,
				affinitiesGrid,
				wsGrid,
				new FloatType( 0.0f ),
				new FloatArray( 1 ),
				processSliceBySlice,
				thresholdPredicate,
				dist,
				queueFactory );
		data.persist( StorageLevel.DISK_ONLY() ).count();

		final N5FSWriter writer = new N5FSWriter( n5Path );
		writer.createDataset( n5Target, wsGrid.getImgDimensions(), wsBlockSize, DataType.UINT64, CompressionType.GZIP );
		writer.createDataset( n5TargetNonBlocked, wsGrid.getImgDimensions(), wsBlockSize, DataType.UINT64, CompressionType.GZIP );
		final DatasetAttributes wsAttrs = writer.getDatasetAttributes( n5Target );
		final Broadcast< CellGrid > wsGridBC = sc.broadcast( wsGrid );

		final List< Tuple2< long[], Long > > counts = data
				.map( t -> new Tuple2<>( Intervals.minAsLongArray( t._1() ), t._2() ) )
				.collect();

		long totalCount = 0;
		final TObjectLongHashMap< HashWrapper< long[] > > offsetMap = new TObjectLongHashMap<>();
		for ( final Tuple2< long[], Long > count : counts )
		{
			offsetMap.put( HashWrapper.longArray( count._1() ), totalCount );
			totalCount += count._2();
		}
		LOG.info( "Got {} labels ({} {})", totalCount, 1.0 * totalCount / Integer.MAX_VALUE, 1.0 * totalCount / Long.MAX_VALUE );
		final Broadcast< TObjectLongHashMap< HashWrapper< long[] > > > offsetMapBC = sc.broadcast( offsetMap );

		final JavaRDD< RandomAccessibleInterval< UnsignedLongType > > remapped = data
				.map( t -> {
					final RandomAccessibleInterval< UnsignedLongType > source = t._1();
					final HashWrapper< long[] > key = HashWrapper.longArray( Intervals.minAsLongArray( source ) );
					final long offset = offsetMapBC.getValue().get( key );
					final UnsignedLongType zero = new UnsignedLongType( 0 );
					for ( final UnsignedLongType s : Views.flatIterable( source ) )
						if ( !zero.valueEquals( s ) )
							s.setInteger( s.get() + offset );
					return source;
				} )
				.persist( StorageLevel.DISK_ONLY() );
		remapped.count();
		data.unpersist();

		remapped
		.filter( rai -> Arrays.equals( Intervals.minAsLongArray( rai ), new long[] { 225, 225, 225 } ) )
		.map( rai -> {
			final N5FSWriter firstBlockWriter = new N5FSWriter( "first-block" );
			final String firstBlockDS = "fb";
			firstBlockWriter.createDataset( firstBlockDS, Intervals.dimensionsAsLongArray( rai ), Intervals.dimensionsAsIntArray( rai ), DataType.UINT64, CompressionType.RAW );
			N5Utils.saveBlock( rai, firstBlockWriter, firstBlockDS, new long[] { 0, 0, 0 } );

			return true;
		} ).count();

		final TLongHashSet firstLabels = new TLongHashSet();
		final long[] firstMin = Intervals.minAsLongArray( remapped.first() );
		for ( final UnsignedLongType label : Views.flatIterable( remapped.first() ) )
			firstLabels.add( label.get() );

		remapped.filter( rai -> {
			for ( int d = 0; d < firstMin.length; ++d )
				if ( firstMin[ d ] != rai.min( d ) )
					return true;
			return false;
		} ).map( rai -> {
			for ( final UnsignedLongType label : Views.flatIterable( rai ) )
				if ( firstLabels.contains( label.get() ) )
				{
					System.out.println( "LABEL " + label + " CONTAINED IN " + Arrays.toString( Intervals.minAsLongArray( rai ) ) );
					return true;
				}
			return false;
		} ).count();

		final TLongLongHashMap sparseUFParents = new TLongLongHashMap();
		final TLongLongHashMap sparseUFRanks= new TLongLongHashMap();
		final UnionFindSparse sparseUnionFind = new UnionFindSparse( sparseUFParents, sparseUFRanks, 0 );
		final Broadcast< N5FSWriter > writerBC = sc.broadcast( writer );

		for ( int d = 0; d < wsGrid.numDimensions(); ++d ) {
			final int finalD = d;
			final String n5TargetUpper = n5Target + "-upper-" + d;
			final String n5TargetLower = n5Target + "-lower-" + d;
			final long[] imgSize = wsGrid.getImgDimensions();
			imgSize[ d ] = wsGrid.gridDimension( d );
			final int[] blockSize = new int[ wsGrid.numDimensions() ];
			for ( int k = 0; k < blockSize.length; ++k )
				blockSize[ k ] = k == d ? 1 : wsGrid.cellDimension( k );
			// use
			writer.createDataset( n5TargetUpper, imgSize, blockSize, DataType.UINT64, CompressionType.RAW );
			writer.createDataset( n5TargetLower, imgSize, blockSize, DataType.UINT64, CompressionType.RAW );
			remapped.map( rai -> {
				final long[] min = Intervals.minAsLongArray( rai );
				final long[] max = Intervals.maxAsLongArray( rai );

				// reduce from cell size + 1 to cell size for each dimension
				// except current working dimension
				for ( int k = 0; k < max.length; ++k )
					if ( k != finalD )
						max[ k ] = Math.min( min[ k ] + wsGridBC.getValue().cellDimension( k ), wsGridBC.getValue().imgDimension( k ) ) - 1;

				// create view of last plane along finalD within interval
				min[ finalD ] = max[ finalD ];
				final RandomAccessibleInterval< UnsignedLongType > upper = max[ finalD ] + 1 == wsGridBC.getValue().imgDimension( finalD ) ? ArrayImgs.unsignedLongs( Intervals.dimensionsAsLongArray( new FinalInterval( min, max ) ) ) : Views.zeroMin( Views.interval( rai, new FinalInterval( min, max ) ) );

				// create view of first plane along finalD within interval
				max[ finalD ] = rai.min( finalD );
				min[ finalD ] = max[ finalD ];
				final RandomAccessibleInterval< UnsignedLongType > lower = Views.zeroMin( Views.interval( rai, new FinalInterval( min, max ) ) );

				final long[] cellPos = new long[ min.length ];
				wsGridBC.getValue().getCellPosition( Intervals.minAsLongArray( rai ), cellPos );

//				final LongArrayDataBlock upperBlock = new LongArrayDataBlock( Intervals.dimensionsAsIntArray( upper ), cellPos, new long[ ( int ) Intervals.numElements( upper ) ] );
//				final LongArrayDataBlock lowerBlock = new LongArrayDataBlock( Intervals.dimensionsAsIntArray( lower ), cellPos, new long[ ( int ) Intervals.numElements( lower ) ] );
//
//				final long[] upperData = upperBlock.getData();
//				final long[] lowerData = lowerBlock.getData();
//				final Cursor< UnsignedLongType > cUpper = Views.flatIterable( upper ).cursor();
//				final Cursor< UnsignedLongType > cLower = Views.flatIterable( lower ).cursor();
//				for ( int i = 0; cUpper.hasNext(); ++i )
//				{
//					upperData[ i ] = cUpper.next().get();
//					lowerData[ i ] = cLower.next().get();
//				}
//				writerBC.getValue().writeBlock( n5TargetUpper, writerBC.getValue().getDatasetAttributes( n5TargetUpper ), upperBlock );
//				writerBC.getValue().writeBlock( n5TargetLower, writerBC.getValue().getDatasetAttributes( n5TargetLower ), lowerBlock );

				N5Utils.saveBlock( upper, writerBC.getValue(), n5TargetUpper, cellPos );
				N5Utils.saveBlock( lower, writerBC.getValue(), n5TargetLower, cellPos );

				return true;
			} ).count();

			final Broadcast< DatasetAttributes > upperAttributesBC = sc.broadcast( writer.getDatasetAttributes( n5TargetUpper ) );
			final Broadcast< DatasetAttributes > lowerAttributesBC = sc.broadcast( writer.getDatasetAttributes( n5TargetLower ) );

			final List< Tuple2< long[], long[] > > assignments = remapped
					.map( rai -> Intervals.minAsLongArray( rai ) )
					.map( offset -> {
						final TLongLongHashMap parents = new TLongLongHashMap();
						final TLongLongHashMap ranks = new TLongLongHashMap();
						final UnionFindSparse localUnionFind = new UnionFindSparse( parents, ranks, 0 );
						final CellGrid grid = wsGridBC.getValue();
						if ( offset[ finalD ] + grid.cellDimension( finalD ) < grid.imgDimension( finalD ) )
						{
							final long[] cellPos = new long[ offset.length ];
							grid.getCellPosition( offset, cellPos );
							final long[] upperCellPos = cellPos.clone();
							upperCellPos[ finalD ] += 1;
							if ( upperCellPos[ finalD ] < grid.gridDimension( finalD ) )
							{

								// will need lower plane from block with higher
								// index and vice versa
								final AbstractDataBlock< long[] > upperPlaneInLowerCell = ( AbstractDataBlock< long[] > ) writerBC.getValue().readBlock( n5TargetUpper, upperAttributesBC.getValue(), cellPos );
								final AbstractDataBlock< long[] > lowerPlaneInUpperCell = ( AbstractDataBlock< long[] > ) writerBC.getValue().readBlock( n5TargetLower, lowerAttributesBC.getValue(), upperCellPos );

								final long[] upperData = upperPlaneInLowerCell.getData();
								final long[] lowerData = lowerPlaneInUpperCell.getData();

								for ( int i = 0; i < upperData.length; ++i )
								{
									final long ud = upperData[ i ];
									final long ld = lowerData[ i ];
									//									if ( ud == 0 || ld == 0 )
									//										System.out.println( "WHY ZERO? " + ud + " " + ld );
									if ( ud != 0 && ld != 0 )
										localUnionFind.join( localUnionFind.findRoot( ud ), localUnionFind.findRoot( ld ) );
								}
							}
						}
						return new Tuple2<>( parents.keys(), parents.values() );
					} )
					.collect();

			System.out.println( "DIMENSION " + d );
			int zeroMappingsCount = 0;

			for ( final Tuple2< long[], long[] > assignment : assignments )
			{
				final long[] keys = assignment._1();
				final long[] vals = assignment._2();
				if ( keys.length == 0 )
					++zeroMappingsCount;
				for ( int i = 0; i < keys.length; ++i )
					sparseUnionFind.join( sparseUnionFind.findRoot( keys[i] ), sparseUnionFind.findRoot( vals[i] ) );
			}
			System.out.println( "zeroMappingsCount " + zeroMappingsCount );
			System.out.println();

		}

		final int setCount = sparseUnionFind.setCount();
		final Broadcast< TLongLongHashMap > parentsBC = sc.broadcast( sparseUFParents );
		final Broadcast< TLongLongHashMap > ranksBC = sc.broadcast( sparseUFRanks );
//		System.out.println( "SPARSE PARENTS ARE " + sparseUFParents );


		remapped
				.map( rai -> {
					final long[] blockMax = Intervals.maxAsLongArray( rai );
					final long[] blockMin = Intervals.minAsLongArray( rai );
					for ( int d = 0; d < blockMin.length; ++d )
						blockMax[ d ] = Math.min( blockMin[ d ] + wsGridBC.getValue().cellDimension( d ), wsGridBC.getValue().imgDimension( d ) ) - 1;
					final RandomAccessibleInterval< UnsignedLongType > target = Views.interval( rai, new FinalInterval( blockMin, blockMax ) );
					return target;
				} )
				.map( new WriteN5<>( sc, writer, n5TargetNonBlocked, wsAttrs.getBlockSize() ) )
				.count();


		remapped
		.map( rai -> {
			final UnionFindSparse uf = new UnionFindSparse( new TLongLongHashMap( parentsBC.getValue() ), new TLongLongHashMap( ranksBC.getValue() ), setCount );
			//					System.out.println( "JOINING HERE: " + parentsBC.getValue() );

			for ( final UnsignedLongType t : Views.flatIterable( rai ) )
				if ( t.getIntegerLong() != 0 )
					t.set( uf.findRoot( t.getIntegerLong() ) );
			final long[] blockMax = Intervals.maxAsLongArray( rai );
			final long[] blockMin = Intervals.minAsLongArray( rai );
			for ( int d = 0; d < blockMin.length; ++d )
				blockMax[ d ] = Math.min( blockMin[ d ] + wsGridBC.getValue().cellDimension( d ), wsGridBC.getValue().imgDimension( d ) ) - 1;
			final RandomAccessibleInterval< UnsignedLongType > target = Views.interval( rai, new FinalInterval( blockMin, blockMax ) );
			return target;
		} )
		.map( new WriteN5<>( sc, writer, n5Target, wsAttrs.getBlockSize() ) )
		.count();



		sc.close();
	}

	public static class RAIFromLoader< T extends RealType< T > & NativeType< T >, A extends ArrayDataAccess< A > > implements PairFunction< HashWrapper< long[] >, HashWrapper< long[] >, RandomAccessibleInterval< T > >
	{

		private final Broadcast< CellGrid > gridBroadcast;

		private final Broadcast< CellLoader< T > > loaderBroadcast;

		private final Broadcast< T > extension;

		private final A access;

		public RAIFromLoader( final JavaSparkContext sc, final Broadcast< CellGrid > gridBroadcast, final Broadcast< CellLoader< T > > loaderBroadcast, final T extension, final A access )
		{
			super();
			this.gridBroadcast = gridBroadcast;
			this.loaderBroadcast = loaderBroadcast;
			this.extension = sc.broadcast( extension );
			this.access = access;
		}

		@Override
		public Tuple2< HashWrapper< long[] >, RandomAccessibleInterval< T > > call( final HashWrapper< long[] > offset ) throws Exception
		{
			final CellGrid grid = gridBroadcast.getValue();
			final CellLoader< T > loader = loaderBroadcast.getValue();
			final Cache< Long, Cell< A > > cache = new SoftRefLoaderCache< Long, Cell< A > >().withLoader( LoadedCellCacheLoader.get( grid, loader, extension.getValue().createVariable() ) );
			final CachedCellImg< T, A > img = new CachedCellImg<>( grid, extension.getValue(), cache, access );
			return new Tuple2<>( offset, img );
		}

	}

	public static class ToInterval< V > implements Function< Tuple2< HashWrapper< long[] >, V >, Tuple2< Interval, V > >
	{

		private final Broadcast< CellGrid > watershedsGrid;

		public ToInterval( final JavaSparkContext sc, final CellGrid watershedsGrid )
		{
			super();
			this.watershedsGrid = sc.broadcast( watershedsGrid );
		}

		@Override
		public Tuple2< Interval, V > call( final Tuple2< HashWrapper< long[] >, V > t ) throws Exception
		{
			final long[] min = t._1().getData();
			final long[] max = IntStream.range( 0, min.length ).mapToLong( d -> Math.min( min[ d ] + watershedsGrid.getValue().cellDimension( d ) + 1, watershedsGrid.getValue().imgDimension( d ) ) - 1 ).toArray();
			final Interval interval = new FinalInterval( min, max );
			return new Tuple2<>( interval, t._2() );
		}

	}

	public static class Extend< T extends Type< T > > implements Function< RandomAccessibleInterval< T >, RandomAccessible< T > >
	{

		private final Broadcast< T > extension;

		public Extend( final Broadcast< T > extension )
		{
			super();
			this.extension = extension;
		}

		@Override
		public RandomAccessible< T > call( final RandomAccessibleInterval< T > rai ) throws Exception
		{
			return Views.extendValue( rai, extension.getValue() );
		}

	}

	public static class WriteN5< T extends NativeType< T > > implements Function< RandomAccessibleInterval< T >, Boolean >
	{

		private final Broadcast< N5FSWriter > writer;

		private final String n5Target;

		private final int[] cellSize;

		public WriteN5( final JavaSparkContext sc, final N5FSWriter writer, final String n5Target, final int[] cellSize )
		{
			super();
			this.writer = sc.broadcast( writer );
			this.n5Target = n5Target;
			this.cellSize = cellSize;
		}

		@Override
		public Boolean call( final RandomAccessibleInterval< T > rai ) throws Exception
		{
			final long[] offset = IntStream.range( 0, 3 ).mapToLong( d -> rai.min( d ) / cellSize[ d ] ).toArray();
			LOG.debug( "Saving block with offset {}", Arrays.toString( offset ) );
			N5Utils.saveBlock( rai, writer.getValue(), n5Target, offset );
			LOG.debug( "Saved block with offset {}", Arrays.toString( offset ) );
			return true;
		}

	}

	public static class Collapse< T > implements Function< RandomAccessible< T >, RandomAccessible< ? extends Composite< T > > >
	{

		@Override
		public RandomAccessible< ? extends Composite< T > > call( final RandomAccessible< T > ra ) throws Exception
		{
			return Views.collapse( ra );
		}

	}

	public static class Registrator implements KryoRegistrator
	{

		@Override
		public void registerClasses( final Kryo kryo )
		{
			kryo.register( FloatType.class, new FloatTypeSerializer() );
		}

	}

	public static class FloatTypeSerializer extends Serializer< FloatType >
	{

		@Override
		public FloatType read( final Kryo kryo, final Input in, final Class< FloatType > type )
		{
			return new FloatType( in.readFloat() );
		}

		@Override
		public void write( final Kryo kryo, final Output out, final FloatType object )
		{
			out.writeFloat( object.get() );
		}

	}

}
