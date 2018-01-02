package de.hanslovsky.examples.kryo;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import net.imglib2.RandomAccessibleInterval;
import net.imglib2.outofbounds.OutOfBoundsFactory;
import net.imglib2.view.ExtendedRandomAccessibleInterval;

public class ExtendedRandomAccessibleIntervalSerializer< T > extends Serializer< ExtendedRandomAccessibleInterval< T, RandomAccessibleInterval< T > > >
{

	@Override
	public void write( final Kryo kryo, final Output output, final ExtendedRandomAccessibleInterval< T, RandomAccessibleInterval< T > > object )
	{
		kryo.writeClassAndObject( output, object.getSource() );
		kryo.writeClassAndObject( output, object.getOutOfBoundsFactory() );
	}

	@SuppressWarnings( "unchecked" )
	@Override
	public ExtendedRandomAccessibleInterval< T, RandomAccessibleInterval< T > > read( final Kryo kryo, final Input input, final Class< ExtendedRandomAccessibleInterval< T, RandomAccessibleInterval< T > > > type )
	{
		return new ExtendedRandomAccessibleInterval<>( ( RandomAccessibleInterval< T > ) kryo.readClassAndObject( input ), ( OutOfBoundsFactory< T, RandomAccessibleInterval< T > > ) kryo.readClassAndObject( input ) );
	}

}
