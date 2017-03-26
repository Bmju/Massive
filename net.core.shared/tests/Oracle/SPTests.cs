#if !COREFX
using System;
using System.Data;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Massive.Tests.Oracle.TableClasses;
using Xunit;
#if !COREFX
using SD.Tools.OrmProfiler.Interceptor;
#endif

namespace Massive.Tests.Oracle
{
	/// <summary>
	/// Suite of tests for stored procedures, functions and cursors on Oracle database.
	/// </summary>
	/// <remarks>
	/// Runs against functions and procedures which are created by running SPTests.sql script on the test database.
	/// These objects do not conflict with anything in the SCOTT database, and can be added there.
	/// </remarks>
	public class SPTests : IDisposable
	{
		private readonly string OrmProfilerApplicationName = "Massive Oracle stored procedure, function and cursor tests";

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


		public static IEnumerable<object[]> ProviderNames = new[] {
			new object[] { "Oracle.ManagedDataAccess.Client" },
			new object[] { "Oracle.DataAccess.Client" }
		};


		[Theory]
		[MemberData(nameof(ProviderNames))]
		public void NormalWhereCall(string ProviderName)
		{
			// Check that things are up and running normally before trying the new stuff
			var db = new Department(ProviderName);
			var rows = db.All(where: "LOC = :0", args: "Nowhere");
			Assert.Equal(9, rows.ToList().Count);
		}


		[Theory]
		[MemberData(nameof(ProviderNames))]
		public void IntegerOutputParam(string ProviderName)
		{
			var db = new SPTestsDatabase(ProviderName);
			dynamic intResult = db.ExecuteWithParams("begin :a := 1; end;", outParams: new { a = 0 });
			Assert.Equal(1, intResult.a);
		}


		public class dateNullParam
		{
			public DateTime? d { get; set; }
		}


		[Theory]
		[MemberData(nameof(ProviderNames))]
		public void InitialNullDateOutputParam(string ProviderName)
		{
			var db = new SPTestsDatabase(ProviderName);
			dynamic dateResult = db.ExecuteWithParams("begin :d := SYSDATE; end;", outParams: new dateNullParam());
			Assert.Equal(typeof(DateTime), dateResult.d.GetType());
		}


		[Theory]
		[MemberData(nameof(ProviderNames))]
		public void InputAndOutputParams(string ProviderName)
		{
			var db = new SPTestsDatabase(ProviderName);
			dynamic procResult = db.ExecuteAsProcedure("findMin", inParams: new { x = 1, y = 3 }, outParams: new { z = 0 });
			Assert.Equal(1, procResult.z);
		}


		[Theory]
		[MemberData(nameof(ProviderNames))]
		public void InputAndReturnParams(string ProviderName)
		{
			var db = new SPTestsDatabase(ProviderName);
			dynamic fnResult = db.ExecuteAsProcedure("findMax", inParams: new { x = 1, y = 3 }, returnParams: new { returnValue = 0 });
			Assert.Equal(3, fnResult.returnValue);
		}


		public class intNullParam
		{
			public int? x { get; set; }
		}


		[Theory]
		[MemberData(nameof(ProviderNames))]
		public void InputOutputParam(string ProviderName)
		{
			var db = new SPTestsDatabase(ProviderName);
			dynamic squareResult = db.ExecuteAsProcedure("squareNum", ioParams: new { x = 4 });
			Assert.Equal(16, squareResult.x);
		}


		[Theory]
		[MemberData(nameof(ProviderNames))]
		public void InitialNullInputOutputParam(string ProviderName)
		{
			var db = new SPTestsDatabase(ProviderName);
			dynamic squareResult = db.ExecuteAsProcedure("squareNum", ioParams: new intNullParam());
			Assert.Equal(null, squareResult.x);
		}


		[Theory]
		[MemberData(nameof(ProviderNames))]
		public void SingleRowFromTableValuedFunction(string ProviderName)
		{
			var db = new SPTestsDatabase(ProviderName);
			var record = db.QueryWithParams("SELECT * FROM table(GET_EMP(:p_EMPNO))", new { p_EMPNO = 7782 }).FirstOrDefault();
			Assert.Equal(7782, record.EMPNO);
			Assert.Equal("CLARK", record.ENAME);
		}


		[Theory]
		[MemberData(nameof(ProviderNames))]
		public void DereferenceCursorValuedFunction(string ProviderName)
		{
			var db = new SPTestsDatabase(ProviderName);
			// Oracle function one cursor return value
			var employees = db.QueryFromProcedure("get_dept_emps", inParams: new { p_DeptNo = 10 }, returnParams: new { v_rc = new Cursor() });
			int count = 0;
			foreach(var employee in employees)
			{
				Console.WriteLine(employee.EMPNO + " " + employee.ENAME);
				count++;
			}
			Assert.Equal(3, count);
		}


		[Theory]
		[MemberData(nameof(ProviderNames))]
		public void DereferenceCursorOutputParameter(string ProviderName)
		{
			var db = new SPTestsDatabase(ProviderName);
			// Oracle procedure one cursor output variables
			var moreEmployees = db.QueryFromProcedure("myproc", outParams: new { prc = new Cursor() });
			int count = 0;
			foreach(var employee in moreEmployees)
			{
				Console.WriteLine(employee.EMPNO + " " + employee.ENAME);
				count++;
			}
			Assert.Equal(14, count);
		}


		[Theory]
		[MemberData(nameof(ProviderNames))]
		public void QueryMultipleFromTwoOutputCursors(string ProviderName)
		{
			var db = new SPTestsDatabase(ProviderName);
			// Oracle procedure two cursor output variables
			var twoSets = db.QueryMultipleFromProcedure("tworesults", outParams: new { prc1 = new Cursor(), prc2 = new Cursor() });
			int sets = 0;
			int[] counts = new int[2];
			foreach(var set in twoSets)
			{
				foreach(var item in set)
				{
					counts[sets]++;
					if(sets == 0) Assert.Equal(typeof(string), item.ENAME.GetType());
					else Assert.Equal(typeof(string), item.DNAME.GetType());
				}
				sets++;
			}
			Assert.Equal(2, sets);
			Assert.Equal(14, counts[0]);
			Assert.Equal(60, counts[1]);
		}


		[Theory]
		[MemberData(nameof(ProviderNames))]
		public void NonQueryWithTwoOutputCursors(string ProviderName)
		{
			var db = new SPTestsDatabase(ProviderName);
			var twoSetDirect = db.ExecuteAsProcedure("tworesults", outParams: new { prc1 = new Cursor(), prc2 = new Cursor() });
			Assert.Equal("OracleRefCursor", twoSetDirect.prc1.GetType().Name);
			Assert.Equal("OracleRefCursor", twoSetDirect.prc2.GetType().Name);
		}


		[Theory]
		[MemberData(nameof(ProviderNames))]
		public void QueryFromMixedCursorOutput(string ProviderName)
		{
			var db = new SPTestsDatabase(ProviderName);
			var mixedSets = db.QueryMultipleFromProcedure("mixedresults", outParams: new { prc1 = new Cursor(), prc2 = new Cursor(), num1 = 0, num2 = 0 });
			int sets = 0;
			int[] counts = new int[2];
			foreach(var set in mixedSets)
			{
				foreach(var item in set)
				{
					counts[sets]++;
					if(sets == 0) Assert.Equal(typeof(string), item.ENAME.GetType());
					else Assert.Equal(typeof(string), item.DNAME.GetType());
				}
				sets++;
			}
			Assert.Equal(2, sets);
			Assert.Equal(14, counts[0]);
			Assert.Equal(60, counts[1]);
		}


		[Theory]
		[MemberData(nameof(ProviderNames))]
		public void NonQueryFromMixedCursorOutput(string ProviderName)
		{
			var db = new SPTestsDatabase(ProviderName);
			var mixedDirect = db.ExecuteAsProcedure("mixedresults", outParams: new { prc1 = new Cursor(), prc2 = new Cursor(), num1 = 0, num2 = 0 });
			Assert.Equal("OracleRefCursor", mixedDirect.prc1.GetType().Name);
			Assert.Equal("OracleRefCursor", mixedDirect.prc2.GetType().Name);
			Assert.Equal(1, mixedDirect.num1);
			Assert.Equal(2, mixedDirect.num2);
		}


		[Theory]
		[MemberData(nameof(ProviderNames))]
		public void PassingCursorInputParameter(string ProviderName)
		{
			var db = new SPTestsDatabase(ProviderName);
			// To share cursors between commands in Oracle the commands must use the same connection
			using(var conn = db.OpenConnection())
			{
				var res1 = db.ExecuteWithParams("begin open :p_rc for select* from emp where deptno = 10; end;", outParams: new { p_rc = new Cursor() }, connection: conn);
				Assert.Equal("OracleRefCursor", res1.p_rc.GetType().Name);
				// TO DO: This Oracle test procedure writes some data into a table; we should produce some output (e.g. a row count) instead
				var res2 = db.ExecuteAsProcedure("cursor_in_out.process_cursor", inParams: new { p_cursor = res1.p_rc }, connection: conn);
				Assert.Equal(0, ((IDictionary<string, object>)res2).Count);
			}
		}
	}
}
#endif