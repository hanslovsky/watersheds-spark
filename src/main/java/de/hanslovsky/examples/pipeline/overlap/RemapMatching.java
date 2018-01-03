package de.hanslovsky.examples.pipeline.overlap;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;

import de.hanslovsky.examples.UnionFindSparse;
import gnu.trove.map.hash.TLongLongHashMap;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.type.numeric.IntegerType;
import net.imglib2.view.Views;
import scala.Tuple2;

public class RemapMatching< I extends IntegerType< I > > implements Function< RandomAccessibleInterval< I >, RandomAccessibleInterval< I > >
{

	private final Broadcast< Tuple2< long[], long[] > > parentsBC;

	private final Broadcast< Tuple2< long[], long[] > > ranksBC;

	private final int setCount;

	public RemapMatching( final Broadcast< Tuple2< long[], long[] > > parentsBC, final Broadcast< Tuple2< long[], long[] > > ranksBC, final int setCount )
	{
		super();
		this.parentsBC = parentsBC;
		this.ranksBC = ranksBC;
		this.setCount = setCount;
	}

	@Override
	public RandomAccessibleInterval< I > call( final RandomAccessibleInterval< I > rai ) throws Exception
	{
		final UnionFindSparse uf = new UnionFindSparse( new TLongLongHashMap( parentsBC.getValue()._1(), parentsBC.getValue()._2() ), new TLongLongHashMap( ranksBC.getValue()._1(), ranksBC.getValue()._2() ), setCount );

		for ( final I t : Views.flatIterable( rai ) )
			if ( t.getIntegerLong() != 0 )
				t.setInteger( uf.findRoot( t.getIntegerLong() ) );
		return rai;
	}

}
