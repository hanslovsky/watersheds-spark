package de.hanslovsky.examples.kryo;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import net.imglib2.cache.ref.SoftRefLoaderCache;
import net.imglib2.img.cell.Cell;

public class SoftRefLoaderCacheSerializer< K, A > extends Serializer< SoftRefLoaderCache< K, Cell< A > > >
{

	@Override
	public void write( final Kryo kryo, final Output output, final SoftRefLoaderCache< K, Cell< A > > object )
	{
		kryo.writeClass( output, object.getClass() );
	}

	@Override
	public SoftRefLoaderCache< K, Cell< A > > read( final Kryo kryo, final Input input, final Class< SoftRefLoaderCache< K, Cell< A > > > type )
	{
		return new SoftRefLoaderCache<>();
	}

}
