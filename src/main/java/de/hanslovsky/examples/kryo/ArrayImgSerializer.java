package de.hanslovsky.examples.kryo;

import java.lang.reflect.InvocationTargetException;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import net.imglib2.img.array.ArrayImg;
import net.imglib2.type.NativeType;
import net.imglib2.util.Intervals;

public class ArrayImgSerializer< T extends NativeType< T >, A > extends Serializer< ArrayImg< T, A > >
{

	@Override
	public void write( final Kryo kryo, final Output output, final ArrayImg< T, A > object )
	{
		output.writeInt( object.numDimensions() );
		output.writeLongs( Intervals.dimensionsAsLongArray( object ) );
		kryo.writeClass( output, object.createLinkedType().getClass() );
		kryo.writeClassAndObject( output, object.update( null ) );
	}

	@Override
	public ArrayImg< T, A > read( final Kryo kryo, final Input input, final Class< ArrayImg< T, A > > type )
	{
		final int nDim = input.readInt();
		final long[] dims = input.readLongs( nDim );
		@SuppressWarnings( "unchecked" )
		final Class< T > linkedTypeClass = kryo.readClass( input ).getType();
		try
		{
			final T helperType = linkedTypeClass.getDeclaredConstructor().newInstance();
			@SuppressWarnings( "unchecked" )
			final A access = ( A ) kryo.readClassAndObject( input );
			final ArrayImg< T, A > img = new ArrayImg<>( access, dims, helperType.getEntitiesPerPixel() );
			final T linkedType = linkedTypeClass.getDeclaredConstructor( img.getClass() ).newInstance( img );
			img.setLinkedType( linkedType );
			return img;
		}
		catch ( InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException | NoSuchMethodException | SecurityException e )
		{
			throw new RuntimeException( e );
		}
	}

}
