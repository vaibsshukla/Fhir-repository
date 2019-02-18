package com.b3ds.ifarm.spark;

public class Demo {

	public static void main(String[] args) {
		B b = new B();
	}

}

class A
{
	public A()
	{
		System.out.println("I am A");
	}
}

class B extends A
{
	public B()
	{
		System.out.println("I am B");
	}
}