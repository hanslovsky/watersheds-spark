package de.hanslovsky.examples.pipeline;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.storage.StorageLevel;
import org.janelia.saalfeldlab.n5.CompressionType;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.N5FSReader;
import org.janelia.saalfeldlab.n5.N5FSWriter;
import org.janelia.saalfeldlab.n5.imglib2.N5Utils;
import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import bdv.bigcat.viewer.viewer3d.util.HashWrapper;
import de.hanslovsky.examples.Util;
import de.hanslovsky.examples.WatershedsOn;
import de.hanslovsky.examples.WatershedsOn.Relief;
import de.hanslovsky.examples.kryo.Registrator;
import net.imglib2.Cursor;
import net.imglib2.Interval;
import net.imglib2.Point;
import net.imglib2.RandomAccessible;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.algorithm.localextrema.LocalExtrema;
import net.imglib2.algorithm.morphology.watershed.HierarchicalPriorityQueueQuantized;
import net.imglib2.algorithm.morphology.watershed.PriorityQueueFactory;
import net.imglib2.algorithm.morphology.watershed.PriorityQueueFastUtil;
import net.imglib2.cache.img.CachedCellImg;
import net.imglib2.img.basictypeaccess.array.ArrayDataAccess;
import net.imglib2.img.cell.Cell;
import net.imglib2.img.cell.CellGrid;
import net.imglib2.img.cell.CellRandomAccess;
import net.imglib2.type.NativeType;
import net.imglib2.type.numeric.RealType;
import net.imglib2.type.numeric.integer.UnsignedLongType;
import net.imglib2.view.Views;
import scala.Tuple2;
import scala.Tuple3;

public class WatershedsPipeline
{

	public static Logger LOG = LoggerFactory.getLogger( MethodHandles.lookup().lookupClass() );

	public enum WatershedType
	{
		RELIEF,
		AFFINITIES;
	}

	public static < T > JavaPairRDD< HashWrapper< long[] >, RandomAccessibleInterval< UnsignedLongType > > flood(
			final JavaSparkContext sc,
			final JavaPairRDD< HashWrapper< long[] >, Tuple2< Interval, RandomAccessible< T > > > data,
			final Function< Tuple2< Interval, RandomAccessible< T > >, Tuple3< RandomAccessibleInterval< UnsignedLongType >, RandomAccessible< T >, Long > > seedGenerator,
			final Function< Tuple3< RandomAccessible< T >, RandomAccessibleInterval< UnsignedLongType >, List< Point > >, RandomAccessibleInterval< UnsignedLongType > > watershed,
			final Function< Tuple2< HashWrapper< long[] >, RandomAccessibleInterval< UnsignedLongType > >, Boolean > writer
			)
	{
		// TODO How to persist this?
		final JavaPairRDD< HashWrapper< long[] >, Tuple3< RandomAccessibleInterval< UnsignedLongType >, RandomAccessible< T >, Long > > seeds = data.mapValues( seedGenerator );// .persist(
		// StorageLevel.DISK_ONLY()
		// );
		final List< Tuple2< HashWrapper< long[] >, Long > > counts = seeds.mapValues( Tuple3Helpers.getLast() ).collect();
		final HashMap< HashWrapper< long[] >, Long > offsets = new HashMap<>();
		long offset = 0;
		for ( final Tuple2< HashWrapper< long[] >, Long > count : counts )
		{
			offsets.put( count._1(), offset );
			offset += count._2();
			LOG.debug( "Counting seed labels: {} {} {}", Arrays.toString( count._1().getData() ), count._2(), offset );
		}
		LOG.debug( "Got {} seed labels.", offset );


		final Broadcast< HashMap< HashWrapper< long[] >, Long > > offsetsBC = sc.broadcast( offsets );

		final JavaPairRDD< HashWrapper< long[] >, Tuple2< RandomAccessible< T >, RandomAccessibleInterval< UnsignedLongType > > > offsetSeeds = seeds
				.mapValues( Tuple3Helpers.dropLast() )
				.mapToPair( new Relabel<>( offsetsBC ) )
				.mapValues( new PermuteFirstAndSecond<>() );

		final JavaPairRDD< HashWrapper< long[] >, RandomAccessibleInterval< UnsignedLongType > > watersheds = offsetSeeds
				.mapValues( t -> {
					final RandomAccessibleInterval< UnsignedLongType > labels = t._2();
					final List< Point > seedPoints = new ArrayList<>();
					final UnsignedLongType zero = new UnsignedLongType();
					for ( final Cursor< UnsignedLongType > c = Views.flatIterable( labels ).cursor(); c.hasNext(); )
						if ( !c.next().valueEquals( zero ) )
							seedPoints.add( new Point( c )  );
					return new Tuple3<>( t._1(), labels, seedPoints );
				})
				.mapValues( watershed );

		watersheds.persist( StorageLevel.DISK_ONLY() );
		final JavaRDD< Boolean > written = watersheds.map( writer );
		final long successCount = written.filter( b -> b ).count();
		LOG.info( "Succesfully wrote {}/{} blocks.", successCount, watersheds.count() );
		seeds.unpersist();

		return watersheds;
	}

	public static class Relabel< T > implements PairFunction<
	Tuple2< HashWrapper< long[] >, Tuple2< RandomAccessibleInterval< UnsignedLongType >, RandomAccessible< T > > >, HashWrapper< long[] >,
	Tuple2< RandomAccessibleInterval< UnsignedLongType >, RandomAccessible< T > > >
	{

		private final Broadcast< HashMap< HashWrapper< long[] >, Long > > counts;

		public Relabel( final Broadcast< HashMap< HashWrapper< long[] >, Long > > counts )
		{
			super();
			this.counts = counts;
		}

		@Override
		public Tuple2< HashWrapper< long[] >, Tuple2< RandomAccessibleInterval< UnsignedLongType >, RandomAccessible< T > > > call(
				final Tuple2< HashWrapper< long[] >, Tuple2< RandomAccessibleInterval< UnsignedLongType >, RandomAccessible< T > > > t ) throws Exception
		{
			final long offset = this.counts.getValue().get( t._1() );
			final RandomAccessibleInterval< UnsignedLongType > labels = t._2()._1();
			final UnsignedLongType zero = new UnsignedLongType();
			zero.setZero();
			for ( final UnsignedLongType l : Views.iterable( labels ) )
				if ( !l.valueEquals( zero ) )
					l.set( l.get() + offset );
			return new Tuple2<>( t._1(), new Tuple2<>( labels, t._2()._2() ) );
		}
	}

	public static class WatershedParametersWithHelp
	{

		@Option( name = "--help", aliases = { "-h" }, required = false, usage = "Print help" )
		public Boolean printHelp = false;

		@Option( name = "--group", aliases = { "-g" }, required = false, usage = "N5 group for input data. Defaults to the value of OUTPUT_GROUP" )
		public String n5Group;

		@Option( name = "--dataset", aliases = { "-d" }, required = true, usage = "N5 dataset for input affinities or relief." )
		public String n5dataset;

		@Argument( metaVar = "OUTPUT_GROUP", index = 0, required = true, usage = "N5 group for output data." )
		public String n5GroupOutput;

		@Option( name = "--process-by-slice", aliases = { "-s" }, required = false, usage = "Process data as 2D slices (sliced along z-axis). Defaults to false." )
		public Boolean processBySlice = false;

		@Option( name = "--watershed-block-size", aliases = {"-b"}, required=true, usage = "Watershed block size in the format bx,by,bz" )
		public String watershedBlockSize;

		@Option( name = "--watershed-halo", aliases = { "-H" }, required = false, usage = "Watershed halo (overlap). Defaults to 1." )
		public Integer watershedHalo = 1;

		@Option( name = "--merge-blocks", aliases = { "-B" }, required = false, usage = "Merge watershed blocks." )
		public Boolean mergeBlocks = false;

		@Option( name = "--minimum", aliases = { "-m" }, required = false, usage = "Minimum value in data (defaults to 0)." )
		public Double minimum = 0.0;

		@Option( name = "--maximum", aliases = { "-M" }, required = false, usage = "Maximum value in data (defaults to 1)." )
		public Double maximum = 1.0;

		@Option( name = "--queue-bins", required = false, usage = "Number of bins for hierarchical priority queue (defaults to 255). Set to value smaller than one for a non-quantized queue." )
		public Integer queueBins = 255;

	}

	public static class ReliefParameters extends WatershedParametersWithHelp
	{
		@Option( name = "--threshold", aliases = { "-t" }, required = false, usage = "Threshold at this value. No thresholding if not specified." )
		public Double threshold = null;

		@Option( name = "--invert", aliases = { "-i" }, required = false, usage = "Multiply relief values by minus one." )
		public Boolean invert = false;
	}

	public static boolean parseArgs( final Collection< String > args, final WatershedParametersWithHelp p ) {
		return parseArgs( args, p, par -> par.printUsage( System.err ) );
	}

	public static boolean parseArgs( final Collection< String > args, final WatershedParametersWithHelp p, final Consumer< CmdLineParser > onHelpRequested )
	{
		final CmdLineParser parser = new CmdLineParser( p );
		try
		{
			parser.parseArgument( args );
			p.n5Group = p.n5Group == null ? p.n5GroupOutput : p.n5Group;
		}
		catch ( final CmdLineException e )
		{
			LOG.error( e.getMessage() );
			p.printHelp = true;
		}
		if ( p.printHelp )
			onHelpRequested.accept( parser );
		return !p.printHelp;
	}

	public static void main( final String[] args ) throws IOException, URISyntaxException
	{

		final SparkConf conf = new SparkConf()
				.set( "spark.serializer", "org.apache.spark.serializer.KryoSerializer" )
				.set( "spark.kryo.registrator", Registrator.class.getName() )
				.setAppName( MethodHandles.lookup().lookupClass().getName() );
		try (JavaSparkContext sc = new JavaSparkContext( conf ))
		{
			sc.setLogLevel( "WARN" );
			final Optional< WatershedType > typeOptional = args.length > 0 ? Arrays.stream( WatershedType.values() ).filter( t -> t.name().toLowerCase().equals( args[ 0 ].toLowerCase() ) ).findFirst() : Optional.empty();

			if ( !typeOptional.isPresent() )
			{
				LOG.error( "Watershed type required as first argument. Options are (case insensitive): " + String.join( ", ", Arrays.stream( WatershedType.values() ).map( t -> t.name().toLowerCase() ).toArray( String[]::new ) ) );
				LOG.error( "Usage: {} {} {}", MethodHandles.lookup().lookupClass().getSimpleName(), "WATERSHED_TYPE", "WATERSHED_PARAMETERS" );
				return;
			}

			final WatershedType type = typeOptional.get();

			final Collection< String > watershedArgs = Arrays.asList( args ).subList( 1, args.length );

			switch (type) {
			case RELIEF:
				final ReliefParameters p = new ReliefParameters();
				final boolean isValid = parseArgs( watershedArgs, p );
				if ( isValid )
					watershedsOnRelief( sc, p );
				break;
			default:
				LOG.error( "Watersheds not implemented for type {}", type );
				break;
			}
		}
	}

	public static < T extends RealType< T > & NativeType< T >, A extends ArrayDataAccess< A > > void watershedsOnRelief( final JavaSparkContext sc, final ReliefParameters p ) throws IOException
	{
		final N5FSReader globalReader = new N5FSReader( Optional.ofNullable( p.n5Group ).orElse( p.n5GroupOutput ) );
		final DatasetAttributes globalAttrs = globalReader.getDatasetAttributes( p.n5dataset );
		final long[] dims = globalAttrs.getDimensions();
		final long[] max = Arrays.stream( dims ).map( l -> l - 1 ).toArray();
		final int[] watershedBlockSize = Arrays.stream( p.watershedBlockSize.split( "," ) ).mapToInt( Integer::parseInt ).toArray();
		final List< HashWrapper< long[] > > offsets = Util.collectAllOffsets( dims, watershedBlockSize, HashWrapper::longArray );
		final int halo = p.watershedHalo;
		@SuppressWarnings( "unchecked" )
		final CachedCellImg< T, A > sample = ( CachedCellImg< T, A > ) N5Utils.open( globalReader, p.n5dataset );
		final T extension = net.imglib2.util.Util.getTypeFromInterval( sample ).createVariable();
		extension.setReal( p.invert ? Double.NEGATIVE_INFINITY : Double.POSITIVE_INFINITY );
		final Broadcast< T > extensionBC = sc.broadcast( extension );
		final CellRandomAccess< T, Cell< A > > ra = sample.randomAccess();
		sample.min( ra );
		final Broadcast< A > access = sc.broadcast( sample.update( ra ) );

		final JavaPairRDD< HashWrapper< long[] >, Tuple2< Interval, RandomAccessible< T > > > data = sc
				.parallelize( offsets )
				.mapToPair( new RAIFromLoader< T, A >( sc, p.n5Group, p.n5dataset, extensionBC, access ) )
				.mapValues( new Extend<>( extensionBC ) )
				.mapToPair( new Expand<>( halo, max, watershedBlockSize ) )
				;

		final T minVal = extension.copy();
		minVal.setReal( 0.0 );
		final double threshold = p.threshold == null ? Double.NaN : p.threshold;
		final MakeSeeds.ExtremaAndThreshold< T > seedGenerator = MakeSeeds.localExtremaAndThreshold(
				sc.broadcast( p.invert ? new LocalExtrema.MaximumCheck<>( minVal ) : new LocalExtrema.MinimumCheck<>( minVal ) ),
				sc.broadcast( Threshold.threshold( threshold, !p.invert ) ) );

		final double queueMin = p.invert ? -p.maximum : p.minimum;
		final double queueMax = p.invert ? -p.minimum : p.maximum;
		final PriorityQueueFactory factory = p.queueBins > 0 ? HierarchicalPriorityQueueQuantized.factory( p.queueBins, queueMin, queueMax ) : PriorityQueueFastUtil.FACTORY;

		final Relief< T, UnsignedLongType, Point > watershed = WatershedsOn.relief(
				sc.broadcast( Distance.get( p.invert ) ),
				sc.broadcast( factory ),
				extensionBC,
				sc.broadcast( new UnsignedLongType() ) );
		final String outputDir = "spark-supervoxels";
		final N5FSWriter writer = new N5FSWriter( p.n5GroupOutput );
		writer.createDataset( outputDir, dims, watershedBlockSize, DataType.UINT64, CompressionType.GZIP );
		final JavaPairRDD< HashWrapper< long[] >, RandomAccessibleInterval< UnsignedLongType > > flooded =
				flood( sc, data, seedGenerator, watershed, new Write<>( sc, writer, outputDir, new CellGrid( dims, watershedBlockSize ) ) );
	}

}
