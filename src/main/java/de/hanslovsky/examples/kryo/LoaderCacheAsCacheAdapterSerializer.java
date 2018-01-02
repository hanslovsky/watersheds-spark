package de.hanslovsky.examples.kryo;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import net.imglib2.cache.util.LoaderCacheAsCacheAdapter;
import net.imglib2.img.cell.Cell;

public class LoaderCacheAsCacheAdapterSerializer< A > extends Serializer< LoaderCacheAsCacheAdapter< Long, Cell< A > > > {

	@Override
	public void write( final Kryo kryo, final Output output, final LoaderCacheAsCacheAdapter< Long, Cell< A > > object )
	{

	}

	@Override
	public LoaderCacheAsCacheAdapter< Long, Cell< A > > read( final Kryo kryo, final Input input, final Class< LoaderCacheAsCacheAdapter< Long, Cell< A > > > type )
	{
		// TODO Auto-generated method stub
		return null;
	}

}
