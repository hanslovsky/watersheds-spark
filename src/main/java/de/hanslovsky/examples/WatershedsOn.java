package de.hanslovsky.examples;

import java.lang.invoke.MethodHandles;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.function.ToDoubleBiFunction;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.imglib2.Interval;
import net.imglib2.Localizable;
import net.imglib2.RandomAccessible;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.algorithm.morphology.watershed.AffinityWatersheds;
import net.imglib2.algorithm.morphology.watershed.PriorityQueueFactory;
import net.imglib2.algorithm.morphology.watershed.Watersheds;
import net.imglib2.algorithm.neighborhood.DiamondShape;
import net.imglib2.type.Type;
import net.imglib2.type.numeric.IntegerType;
import net.imglib2.type.numeric.RealType;
import net.imglib2.util.Intervals;
import net.imglib2.view.Views;
import net.imglib2.view.composite.Composite;
import scala.Tuple3;

public class WatershedsOn
{

	private static final Logger LOG = LoggerFactory.getLogger( MethodHandles.lookup().lookupClass() );

	public static < T extends Type< T >, L extends IntegerType< L >, P extends Localizable > Relief< T, L, P > relief(
			final Broadcast< ToDoubleBiFunction< T, T > > dist,
			final Broadcast< PriorityQueueFactory > factory,
			final Broadcast< T > extension,
			final Broadcast< L > labelsExtension )
	{
		return new Relief<>( dist, factory, extension, labelsExtension );
	}

	public static < T extends RealType< T >, C extends Composite< T >, L extends IntegerType< L >, P extends Localizable > Affinities< T, C, L, P > affinities(
			final Broadcast< PriorityQueueFactory > factory,
			final Broadcast< C > extension,
			final Broadcast< L > labelsExtension )
	{
		return new Affinities<>( factory, extension, labelsExtension );
	}

	public static class Relief< T extends Type< T >, L extends IntegerType< L >, P extends Localizable >
	implements Function< Tuple3< RandomAccessible< T >, RandomAccessibleInterval< L >, List< P > >, RandomAccessibleInterval< L > >
	{

		private final Broadcast< ToDoubleBiFunction< T, T > > dist;

		private final Broadcast< PriorityQueueFactory > factory;

		private final Broadcast< T > extension;

		private final Broadcast< L > labelsExtension;

		public Relief(
				final Broadcast< ToDoubleBiFunction< T, T > > dist,
				final Broadcast< PriorityQueueFactory > factory,
				final Broadcast< T > extension,
				final Broadcast< L > labelsExtension )
		{
			super();
			this.dist = dist;
			this.factory = factory;
			this.extension = extension;
			this.labelsExtension = labelsExtension;
		}

		@Override
		public RandomAccessibleInterval< L > call( final Tuple3< RandomAccessible< T >, RandomAccessibleInterval< L >, List< P > > reliefLabelsSeeds ) throws Exception
		{
			LOG.debug( "Calculating watersheds for interval {}", new IntervalsToString( reliefLabelsSeeds._2() ) );
			final RandomAccessible< T > relief = reliefLabelsSeeds._1();
			final RandomAccessibleInterval< L > labels = reliefLabelsSeeds._2();
			final List< P > seeds = reliefLabelsSeeds._3();
			processRelief( relief, labels, seeds, dist.getValue(), factory.getValue(), extension.getValue(), labelsExtension.getValue() );

			return labels;
		}
	}

	private static class Affinities< T extends RealType< T >, C extends Composite< T >, L extends IntegerType< L >, P extends Localizable >
	implements Function< Tuple3< RandomAccessible< C >, RandomAccessibleInterval< L >, List< P > >, RandomAccessibleInterval< L > >
	{

		private final Broadcast< PriorityQueueFactory > factory;

		private final Broadcast< C > extension;

		private final Broadcast< L > labelsExtension;

		public Affinities(
				final Broadcast< PriorityQueueFactory > factory,
				final Broadcast< C > extension,
				final Broadcast< L > labelsExtension )
		{
			super();
			this.factory = factory;
			this.extension = extension;
			this.labelsExtension = labelsExtension;
		}

		@Override
		public RandomAccessibleInterval< L > call( final Tuple3< RandomAccessible< C >, RandomAccessibleInterval< L >, List< P > > affinitiesLabelsSeeds ) throws Exception
		{
			LOG.debug( "Calculating watersheds for interval {}", new IntervalsToString( affinitiesLabelsSeeds._2() ) );
			final RandomAccessible< C > affinities = affinitiesLabelsSeeds._1();
			final RandomAccessibleInterval< L > labels = affinitiesLabelsSeeds._2();
			final List< P > seeds = affinitiesLabelsSeeds._3();
			processAffinities( affinities, labels, seeds, factory.getValue(), extension.getValue(), labelsExtension.getValue() );

			return labels;
		}
	}

	private static < T extends Type< T >, L extends IntegerType< L >, P extends Localizable > void processRelief(
			final RandomAccessible< T > relief,
			final RandomAccessibleInterval< L > labels,
			final List< P > seeds,
			final ToDoubleBiFunction< T, T > dist,
			final PriorityQueueFactory factory,
			final T extension,
			final L labelsExtension ) throws InterruptedException, ExecutionException
	{

		LOG.trace( "Running watersheds for {} {}", Arrays.toString( Intervals.minAsLongArray( labels ) ), Arrays.toString( Intervals.maxAsLongArray( labels ) ) );
		Watersheds.flood(
				Views.extendValue( Views.interval( relief, labels ), extension ),
				Views.extendValue( labels, labelsExtension ),
				labels,
				seeds,
				new DiamondShape( 1 ),
				dist,
				factory );
	}

	private static < T extends RealType< T >, C extends Composite< T >, L extends IntegerType< L >, P extends Localizable > void processAffinities(
			final RandomAccessible< C > affinities,
			final RandomAccessibleInterval< L > labels,
			final List< P > seeds,
			final PriorityQueueFactory factory,
			final C extension,
			final L labelsExtension ) throws InterruptedException, ExecutionException
	{
		LOG.trace( "Running watersheds for {} {}", Arrays.toString( Intervals.minAsLongArray( labels ) ), Arrays.toString( Intervals.maxAsLongArray( labels ) ) );
		AffinityWatersheds.flood( affinities, Views.extendValue( labels, labelsExtension ), labels, seeds, new DiamondShape( 1 ), factory );
	}

	private static class IntervalsToString
	{
		private final Interval interval;

		public IntervalsToString( final Interval interval )
		{
			super();
			this.interval = interval;
		}

		@Override
		public String toString()
		{
			return "( " + Arrays.toString( Intervals.minAsLongArray( interval ) ) + " " + Arrays.toString( Intervals.maxAsLongArray( interval ) ) + " )";
		}
	}

}