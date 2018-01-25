package org.saalfeldlab.watersheds;

import gnu.trove.map.hash.TLongLongHashMap;

public class UnionFindSparse
{
	private final TLongLongHashMap parents;

	private int nSets;

	public UnionFindSparse()
	{
		this( 0 );
	}

	public UnionFindSparse( final int size )
	{
		this.parents = new TLongLongHashMap();
		this.nSets = size;
		for ( int i = 0; i < size; ++i )
			this.parents.put( i, i );
	}

	public UnionFindSparse( final TLongLongHashMap parents, final int nSets )
	{
		this.parents = parents;
		this.nSets = nSets;
	}

	public long findRoot( final long id )
	{

		if ( !this.parents.containsKey( id ) )
		{
			this.parents.put( id, id );
			++nSets;
			return id;
		}

		long startIndex1 = id;
		long startIndex2 = id;
		long tmp = id;


		// find root
		while ( startIndex1 != parents.get( startIndex1 ) )
			startIndex1 = parents.get( startIndex1 );

		// label all positions on the way to root as parent
		while ( startIndex2 != startIndex1 )
		{
			tmp = parents.get( startIndex2 );
			parents.put( startIndex2, startIndex1 );
			startIndex2 = tmp;
		}

		return startIndex1;

	}

	public long join( final long id1, final long id2 )
	{

		if ( !parents.containsKey( id1 ) )
		{
			parents.put( id1, id1 );
			++nSets;
		}

		if ( !parents.containsKey( id2 ) )
		{
			parents.put( id2, id2 );
			++nSets;
		}

		if ( id1 == id2 )
			//			assert this.parents.contains( id1 ) && this.parents.contains( id2 );
			return id1;

		--nSets;

		if ( id2 < id1 )
		{
			parents.put( id1, id2 );
			return id2;
		}

		else
		{
			parents.put( id2, id1 );
			return id1;
		}

	}

	public int size()
	{
		return parents.size();
	}

	public int setCount()
	{
		return nSets;
	}

	@Override
	public UnionFindSparse clone()
	{
		final TLongLongHashMap parents = new TLongLongHashMap();
		parents.putAll( this.parents );
		return new UnionFindSparse( parents, nSets );
	}

}
