package de.hanslovsky.examples;

import java.util.stream.IntStream;

public class UnionFind
{
	private final int[] parents;

	private final int[] ranks;

	private int nSets;

	public UnionFind()
	{
		this( 0 );
	}

	public UnionFind( final int size )
	{
		this( IntStream.range( 0, size ).toArray(), new int[ size ], size );
	}

	private UnionFind( final int[] parents, final int[] ranks, final int nSets )
	{
		this.parents = parents;
		this.ranks = ranks;
		this.nSets = nSets;
	}

	public int findRoot( final int id )
	{

		int startIndex1 = id;
		int startIndex2 = id;
		int tmp = id;


		// find root
		while ( startIndex1 != parents[ startIndex1 ] )
			startIndex1 = parents[ startIndex1 ];

		// label all positions on the way to root as parent
		while ( startIndex2 != startIndex1 )
		{
			tmp = parents[ startIndex2 ];
			parents[ startIndex2 ] = startIndex1;
			startIndex2 = tmp;
		}

		return startIndex1;

	}

	public int join( final int id1, final int id2 )
	{

		if ( id1 == id2 )
			return id1;

		--nSets;

		final int r1 = ranks[ id1 ];
		final int r2 = ranks[ id2 ];

		if ( r1 < r2 )
		{
			parents[ id1 ] = id2;
			return id2;
		}

		else
		{
			parents[ id2 ] = id1;
			if ( r1 == r2 )
				ranks[ id1 ] = r1 + 1;
			return id1;
		}

	}

	public int size()
	{
		return parents.length;
	}

	public int setCount()
	{
		return nSets;
	}

	@Override
	public UnionFind clone()
	{
		return new UnionFind( parents.clone(), ranks.clone(), nSets );
	}

}
