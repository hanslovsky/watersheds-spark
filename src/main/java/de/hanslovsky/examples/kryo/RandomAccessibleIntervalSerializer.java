package de.hanslovsky.examples.kryo;

import java.util.stream.IntStream;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import net.imglib2.RandomAccessibleInterval;
import net.imglib2.img.array.ArrayImgs;
import net.imglib2.type.NativeType;
import net.imglib2.type.numeric.integer.UnsignedLongType;
import net.imglib2.util.Intervals;
import net.imglib2.util.Util;
import net.imglib2.view.Views;

public class RandomAccessibleIntervalSerializer< T extends NativeType< T > > extends Serializer< RandomAccessibleInterval< T > >
{
	@SuppressWarnings( "unchecked" )
	@Override
	public RandomAccessibleInterval< T > read( final Kryo kryo, final Input in, final Class< RandomAccessibleInterval< T > > cls )
	{
		final T t = ( T ) kryo.readClassAndObject( in );
		final int nDim = in.readInt( true );
		final long[] min = in.readLongs( nDim );
		final long[] max = in.readLongs( nDim );
		final long[] dim = IntStream.range( 0, nDim ).mapToLong( d -> max[ d ] - min[ d ] + 1 ).toArray();
		final RandomAccessibleInterval< T > rai;
		if ( t instanceof UnsignedLongType )
			rai = ( RandomAccessibleInterval< T > ) ArrayImgs.unsignedLongs( in.readLongs( ( int ) Intervals.numElements( dim ) ), dim );
		else
			rai = null;
		return Views.translate( rai, min );
	}

	@SuppressWarnings( "unchecked" )
	@Override
	public void write( final Kryo kryo, final Output out, final RandomAccessibleInterval< T > rai )
	{
		final T t = Util.getTypeFromInterval( rai ).createVariable();
		kryo.writeObject( out, t );
		out.writeInt( rai.numDimensions(), true );
		out.writeLongs( Intervals.minAsLongArray( rai ) );
		out.writeLongs( Intervals.maxAsLongArray( rai ) );
		if ( t instanceof UnsignedLongType )
			for ( final UnsignedLongType ul : Views.flatIterable( ( RandomAccessibleInterval< UnsignedLongType > ) rai ) )
				out.writeLong( ul.get() );
	}

}