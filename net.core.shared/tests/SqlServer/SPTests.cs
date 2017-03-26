using System;
using System.Data;
using System.Dynamic;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Massive.Tests.SqlServer.TableClasses;
using Xunit;
#if !COREFX
using SD.Tools.OrmProfiler.Interceptor;
#endif

namespace Massive.Tests.SqlServer
{
	/// <summary>
	/// Suite of tests for stored procedures and functions on SQL Server database.
	/// </summary>
	/// <remarks>
	/// Runs against functions and procedures which are already in the AdventureWorks test database.
	/// </remarks>
	public class SPTests : IDisposable
	{
		private readonly string OrmProfilerApplicationName = "Massive SqlServer stored procedure and function tests";

		public SPTests()
		{
			Console.WriteLine("Entering " + OrmProfilerApplicationName);
#if !COREFX
			InterceptorCore.Initialize(OrmProfilerApplicationName);
#endif
		}

		public void Dispose()
		{
			Console.WriteLine("Exiting " + OrmProfilerApplicationName);
		}


		[Fact]
		public void NormalSingleCall()
		{
			// Check that things are up and running normally before trying the new stuff
			var soh = new SalesOrderHeader();
			var item = soh.Single("SalesOrderID=@0", args: 43659);
			Assert.Equal("PO522145787", item.PurchaseOrderNumber);
		}

		public class boolNullParam
		{
			public bool? a { get; set; }
		}

		[Fact]
		public void InitialNullBooleanOutputParam()
		{
			var db = new SPTestsDatabase();
			dynamic boolResult = db.ExecuteWithParams("set @a = 1", outParams: new boolNullParam());
			Assert.Equal(typeof(bool), boolResult.a.GetType());
		}

		public class intNullParam
		{
			public int? a { get; set; }
		}

		[Fact]
		public void InitialNullIntegerOutputParam()
		{
			var db = new SPTestsDatabase();
			dynamic intResult = db.ExecuteWithParams("set @a = 1", outParams: new intNullParam());
			Assert.Equal(typeof(int), intResult.a.GetType());
		}

		[Fact]
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
			Assert.Equal(3, count);
		}

		[Fact]
		public void SingleRowFromTableValuedFunction()
		{
			var db = new SPTestsDatabase();
			// Accessing table value functions on SQL Server (different syntax from Postgres, for example)
			var person = db.QueryWithParams("SELECT * FROM dbo.ufnGetContactInformation(@PersonID)", new { @PersonID = 35 }).FirstOrDefault();
			Assert.Equal(typeof(string), person.FirstName.GetType());
		}

		[Fact]
		public void DateReturnParameter()
		{
			var db = new SPTestsDatabase();
			dynamic d = new ExpandoObject();
			d.d = true; // NB the type is ignored (by the underlying driver)
			var dResult = db.ExecuteAsProcedure("ufnGetAccountingEndDate", returnParams: d);
			Assert.Equal(typeof(DateTime), dResult.d.GetType());
		}

		[Fact]
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
					if(sets == 0) Assert.Equal(typeof(int), item.b.GetType());
					else Assert.Equal(typeof(int), item.c.GetType());
				}
				sets++;
			}
			Assert.Equal(2, sets);
			Assert.Equal(1, counts[0]);
			Assert.Equal(1, counts[1]);
		}

		public class wArgs
		{
			public int? w { get; set; }
		}

		[Fact]
		public void DefaultValueFromNullInputOutputParam()
		{
			var db = new SPTestsDatabase();
			// w := w + 2; v := w - 1; x := w + 1
			dynamic testResult = db.ExecuteAsProcedure("TestVars", ioParams: new wArgs(), outParams: new { v = 0, x = 0 });
			Assert.Equal(1, testResult.v);
			Assert.Equal(2, testResult.w);
			Assert.Equal(3, testResult.x);
		}

		[Fact]
		public void ProvideValueToInputOutputParam()
		{
			var db = new SPTestsDatabase();
			// w := w + 2; v := w - 1; x := w + 1
			dynamic testResult = db.ExecuteAsProcedure("TestVars", ioParams: new { w = 2 }, outParams: new { v = 0, x = 0 });
			Assert.Equal(3, testResult.v);
			Assert.Equal(4, testResult.w);
			Assert.Equal(5, testResult.x);
		}

		/// <remarks>
		/// See comments on IsCursor() in Massive.SqlServer.cs
		/// </remarks>
		[Fact]
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
					Assert.Equal(typeof(string), item.CurrencyCode.GetType());
					Assert.Equal(typeof(string), item.Name.GetType());
				}
			}
			Assert.Equal(105, count);

			// An example of the correct way to do it
			dynamic fastResults = db.QueryFromProcedure("uspCurrencySelect");
			int fastCount = 0;
			foreach(var item in fastResults)
			{
				fastCount++;
				Assert.Equal(typeof(string), item.CurrencyCode.GetType());
				Assert.Equal(typeof(string), item.Name.GetType());
			}
			Assert.Equal(105, fastCount);
		}

		[Fact]
		public void ScalarFromProcedure()
		{
			var db = new SPTestsDatabase();
			var value = db.ScalarWithParams("uspCurrencySelect", isProcedure: true);
			Assert.Equal("AFA", value);
		}
	}
}
