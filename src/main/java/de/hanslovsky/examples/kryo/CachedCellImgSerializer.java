package de.hanslovsky.examples.kryo;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import net.imglib2.cache.Cache;
import net.imglib2.cache.img.CachedCellImg;
import net.imglib2.img.cell.Cell;
import net.imglib2.img.cell.CellGrid;
import net.imglib2.type.NativeType;

public class CachedCellImgSerializer< T extends NativeType< T >, A > extends Serializer< CachedCellImg< T, A > >
{

	@Override
	public void write( final Kryo kryo, final Output output, final CachedCellImg< T, A > img )
	{
		kryo.writeClassAndObject( output, img.createLinkedType() );
		kryo.writeClassAndObject( output, img.getAccessType() );
		kryo.writeClassAndObject( output, img.getCellGrid() );
		kryo.writeClassAndObject( output, img.getCache() );
	}

	@SuppressWarnings( "unchecked" )
	@Override
	public CachedCellImg< T, A > read( final Kryo kryo, final Input input, final Class< CachedCellImg< T, A > > type )
	{
		final T t = ( T ) kryo.readClassAndObject( input );
		final A a = ( A ) kryo.readClassAndObject( input );
		final CellGrid grid = ( CellGrid ) kryo.readClassAndObject( input );
		final Cache< Long, Cell< A > > cache = ( Cache< Long, Cell< A > > ) kryo.readClassAndObject( input );
		return new CachedCellImg< T, A >( grid, t, cache, a );
	}

}
