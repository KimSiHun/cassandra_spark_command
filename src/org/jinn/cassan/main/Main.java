package org.jinn.cassan.main;

import org.jinn.cassan.services.Test;

public class Main
{
	public static void main(String[] args)
	{
		System.out.println("Hello World");
		
		Test t = new Test();
		t.read_queries();
		t.test();
		
	}
}
