using System;
using System.Data;
using System.Dynamic;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Massive.Tests.TableClasses;
using NUnit.Framework;
using SD.Tools.OrmProfiler.Interceptor;

namespace Massive.Tests.Oracle
{
	/// <summary>
	/// Suite of tests for stored procedures and functions on SQL Server database.
	/// </summary>
	/// <remarks>
	/// Runs against functions and procedures which are already in the AdventureWorks test database.
	/// </remarks>
	[TestFixture]
	public class SPTests
	{
		[SetUp]
		public void Setup()
		{
			InterceptorCore.Initialize("Massive SQL Server stored procedure and function tests");
		}

		[Test]
		public void NormalSingleCall()
		{
			// Check that things are up and running normally before trying the new stuff
			var db = new SalesOrderHeader();
			var item = db.Single("SalesOrderID=@0", args: 43659);
			Assert.AreEqual("PO522145787", item.PurchaseOrderNumber);
		}

		public class boolNullParam
		{
			public bool? a { get; set; }
		}

		[Test]
		public void InitialNullBooleanOutputParam()
		{
			var db = new SPTestsDatabase();
			dynamic boolResult = db.ExecuteWithParams("set @a = 1", outParams: new boolNullParam());
			Assert.AreEqual(typeof(bool), boolResult.a.GetType());
		}

		public class intNullParam
		{
			public int? a { get; set; }
		}

		[Test]
		public void InitialNullIntegerOutputParam()
		{
			var db = new SPTestsDatabase();
			dynamic intResult = db.ExecuteWithParams("set @a = 1", outParams: new intNullParam());
			Assert.AreEqual(typeof(int), intResult.a.GetType());
		}

		[Test]
		public void QueryFromStoredProcedure()
		{
			var db = new SPTestsDatabase();
			var people = db.QueryFromProcedure("uspGetEmployeeManagers", new { BusinessEntityID = 35 });
			int count = 0;
			foreach(var person in people)
			{
				Console.WriteLine(person.FirstName + " " + person.LastName);
				count++;
			}
			Assert.AreEqual(3, count);
		}

		[Test]
		public void SingleRowFromTableValuedFunction()
		{
			var db = new SPTestsDatabase();
			// Accessing table value functions on SQL Server (different syntax from Postgres, for example)
			var person = db.QueryWithParams("SELECT * FROM dbo.ufnGetContactInformation(@PersonID)", new { @PersonID = 35 }).FirstOrDefault();
			Assert.AreEqual(typeof(string), person.FirstName.GetType());
		}

		// TO DO: This should be done as Scalar()
		[Test]
		public void DateReturnParameter()
		{
			var db = new SPTestsDatabase();
			// Scalar valued function (returns System.DateTime type)
			dynamic d = new ExpandoObject();
			d.d = true; // NB this is ignored (by the underlying driver)
			var dResult = db.ExecuteAsProcedure("ufnGetAccountingEndDate", returnParams: d);
			Assert.AreEqual(typeof(DateTime), dResult.d.GetType());
		}

		[Test]
		public void QueryMultipleFromTwoResultSets()
		{
			var db = new SPTestsDatabase();
			var twoSets = db.QueryMultiple("select 1 as a, 2 as b; select 3 as c, 4 as d;");
			int sets = 0;
			int[] counts = new int[2];
			foreach(var set in twoSets)
			{
				foreach(var item in set)
				{
					counts[sets]++;
					if(sets == 0) Assert.AreEqual(typeof(int), item.b.GetType());
					else Assert.AreEqual(typeof(int), item.c.GetType());
				}
				sets++;
			}
			Assert.AreEqual(2, sets);
			Assert.AreEqual(1, counts[0]);
			Assert.AreEqual(1, counts[1]);
		}

		//// TO DO: Implement this test fn, which is currently on Postgres
		//public void InputOutputParam()
		//{
		//	////dynamic testResult = db.ExecuteAsProcedure("test_vars", ioParams: new { w = 0 }, outParams: new { x = 0, p2 = 0 });
		//}

		//// TO DO: Implement this test fn, which is currently on Postgres
		//public void InitialNulInputOutputParam()
		//{
		//	////dynamic testResult = db.ExecuteAsProcedure("test_vars", ioParams: new { w = 0 }, outParams: new { x = 0, p2 = 0 });
		//}

		//[Test]
		//public void ScalarFromProcedure()
		//{
		//	var db = new SPTestDatabase();
		//	// TO DO
		//}

		[TearDown]
		public void CleanUp()
		{
		}
	}
}
