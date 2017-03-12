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
			var soh = new SalesOrderHeader();
			var item = soh.Single("SalesOrderID=@0", args: 43659);
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

		public class wArgs
		{
			public int? w { get; set; }
		}

		[Test]
		public void DefaultValueFromNullInputOutputParam()
		{
			var db = new SPTestsDatabase();
			// w := w + 2; v := w - 1; x := w + 1
			dynamic testResult = db.ExecuteAsProcedure("TestVars", ioParams: new wArgs(), outParams: new { v = 0, x = 0 });
			Assert.AreEqual(1, testResult.v);
			Assert.AreEqual(2, testResult.w);
			Assert.AreEqual(3, testResult.x);
		}

		[Test]
		public void ProvideValueToInputOutputParam()
		{
			var db = new SPTestsDatabase();
			// w := w + 2; v := w - 1; x := w + 1
			dynamic testResult = db.ExecuteAsProcedure("TestVars", ioParams: new { w = 2 }, outParams: new { v = 0, x = 0 });
			Assert.AreEqual(3, testResult.v);
			Assert.AreEqual(4, testResult.w);
			Assert.AreEqual(5, testResult.x);
		}

		/// <remarks>
		/// There is no such thing as a Cursor SqlDbType for SqlParameter in System.Data.SqlClient (unlike in Oracle and PostgreSQL).
		/// In T-SQL a cursor is not for passing references to result sets around (T-SQL does that automatically) it is JUST for single
		/// stepping through a result set, and it would be an even worse idea to do that controlled from C#, if it was possible to.
		/// </remarks>
		[Test]
		public void DereferenceCursor()
		{
			// There is probably no situation in which it would make sense to do this (a procedure returning a cursor should be for use by another
			// procedure only - if at all); the remarks above and the example immediately below document why this is the wrong thing to do.
			var db = new SPTestsDatabase();
			var SQL = "DECLARE @MyCursor CURSOR;\r\n" +
					  "EXEC dbo.uspCurrencyCursor @CurrencyCursor = @MyCursor OUTPUT;\r\n" +
					  "WHILE(@@FETCH_STATUS = 0)\r\n" +
					  "BEGIN;\r\n" +
					  "\tFETCH NEXT FROM @MyCursor;\r\n" +
					  "END;\r\n" +
					  "CLOSE @MyCursor;\r\n" +
					  "DEALLOCATE @MyCursor;\r\n";
			dynamic resultSets = db.QueryMultiple(SQL);
			int count = 0;
			foreach(var results in resultSets)
			{
				foreach(var item in results)
				{
					count++;
					Assert.AreEqual(typeof(string), item.CurrencyCode.GetType());
					Assert.AreEqual(typeof(string), item.Name.GetType());
				}
			}
			Assert.AreEqual(105, count);

			// An example of the correct way to do it
			dynamic fastResults = db.QueryFromProcedure("uspCurrencySelect");
			int fastCount = 0;
			foreach(var item in fastResults)
			{
				fastCount++;
				Assert.AreEqual(typeof(string), item.CurrencyCode.GetType());
				Assert.AreEqual(typeof(string), item.Name.GetType());
			}
			Assert.AreEqual(105, fastCount);
		}

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
