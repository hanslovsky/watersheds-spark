package de.hanslovsky.examples.kryo;

import java.util.stream.IntStream;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import net.imglib2.img.cell.CellGrid;

public class CellGridSerializer extends Serializer< CellGrid >
{

	@Override
	public CellGrid read( final Kryo kryo, final Input in, final Class< CellGrid > type )
	{
		final int nDim = in.readInt();
		return new CellGrid( in.readLongs( nDim ), in.readInts( nDim ) );
	}

	@Override
	public void write( final Kryo kryo, final Output out, final CellGrid grid )
	{
		out.writeInt( grid.numDimensions() );
		out.writeLongs( grid.getImgDimensions() );
		IntStream.range( 0, grid.numDimensions() ).map( grid::cellDimension ).forEach( out::writeInt );
	}

}