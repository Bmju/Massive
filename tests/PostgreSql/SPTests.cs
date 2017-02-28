using System;
using System.Data;
using System.Dynamic;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using PostgreSql.TableClasses;
using NUnit.Framework;
using SD.Tools.OrmProfiler.Interceptor;

namespace Massive.Tests.Oracle
{
	/// <summary>
	/// Suite of tests for stored procedures, functions and cursors on PostgreSQL database.
	/// </summary>
	/// <remarks>
	/// Runs against functions and procedures which are created by running SPTests.sql script on the test database.
	/// These objects do not conflict with anything in the Northwind database, and can be added there.
	/// </remarks>
	[TestFixture]
	public class SPTests
	{
		[SetUp]
		public void Setup()
		{
			InterceptorCore.Initialize("Massive PostgrSQL procedure, function & cursor tests");
		}

		[Test]
		public void InitialNullIntegerOutputParam()
		{
			var db = new SPTestsDatabase();
			// NB This is PostgreSql specific; Npgsql completely ignores the output parameter type and sets it (sensibly) from the return type.
			dynamic z = new ExpandoObject();
			z.z = null;
			dynamic procResult = db.ExecuteAsProcedure("find_min", inParams: new { x = 5, y = 3 }, outParams: z);
			Assert.AreEqual(typeof(int), procResult.z.GetType());
			Assert.AreEqual(3, procResult.z);
		}

		[Test]
		public void IntegerReturnParam()
		{
			var db = new SPTestsDatabase();
			// NB Massive is converting all Postgres return params to output params because Npgsql treats all function
			// output and return as output (which is because PostgreSQL itself treats them as the same, really).
			dynamic fnResult = db.ExecuteAsProcedure("find_max", inParams: new { x = 6, y = 7 }, returnParams: new { returnValue = true });
			Assert.AreEqual(7, fnResult.returnValue);
		}

		[Test]
		public void PostgresAnonymousParametersA()
		{
			var db = new SPTestsDatabase();
			// Only PostgreSQL supports anonymous parameters (AFAIK) - we treat object[] in the context of params differently from
			// how it is treated when it appears in args: in the standard Massive API, to provide support for this. (Note, object[]
			// makes no sense in the context of named parameters otherwise, and will throw an exception on the other DBs.)
			dynamic fnResultAnon = db.ExecuteAsProcedure("find_max", inParams: new object[] { 12, 7 }, returnParams: new { returnValue = 0 });
			Assert.AreEqual(12, fnResultAnon.returnValue);
		}

		[Test]
		public void PostgresAnonymousParametersB()
		{
			var db = new SPTestsDatabase();
			// NB This function can't be called except with anonymous parameters.
			// (I believe you can't even do it with a SQL block, because Postgres anonymous SQL blocks do not accept parameters? May be wrong...)
			dynamic addResult = db.ExecuteAsProcedure("add_em", inParams: new object[] { 4, 2 }, returnParams: new { RETURN = 0 });
			Assert.AreEqual(6, addResult.RETURN);
		}

		[Test]
		public void InputOutputParam()
		{
			var db = new SPTestsDatabase();
			dynamic squareResult = db.ExecuteAsProcedure("square_num", ioParams: new { x = 4 });
			Assert.AreEqual(16, squareResult.x);
		}

		[Test]
		public void InitialNullInputOutputParam()
		{
			var db = new SPTestsDatabase();
			dynamic xParam = new ExpandoObject();
			xParam.x = null;
			dynamic squareResult = db.ExecuteAsProcedure("square_num", ioParams: xParam);
			Assert.AreEqual(null, squareResult.x);
		}

		public class dateNullParam
		{
			public DateTime? d { get; set; }
		}

		[Test]
		public void InitialNullDateReturnParamMethod1()
		{
			var db = new SPTestsDatabase();
			dynamic dateResult = db.ExecuteAsProcedure("get_date", returnParams: new dateNullParam());
			Assert.AreEqual(typeof(DateTime), dateResult.d.GetType());
		}

		[Test]
		public void InitialNullDateReturnParamMethod2()
		{
			var db = new SPTestsDatabase();
			// NB This is PostgreSql specific; Npgsql completely ignores the output parameter type and sets it (sensibly) from the return type.
			dynamic dParam = new ExpandoObject();
			dParam.d = null;
			dynamic dateResult = db.ExecuteAsProcedure("get_date", returnParams: dParam);
			Assert.AreEqual(typeof(DateTime), dateResult.d.GetType());
		}

		[Test]
		public void DefaultValueFromNullInputOutputParam()
		{
			var db = new SPTestsDatabase();
			dynamic wArgs = new ExpandoObject();
			wArgs.w = null;
			// w := w + 2; v := w - 1; x := w + 1
			dynamic testResult = db.ExecuteAsProcedure("test_vars", ioParams: wArgs, outParams: new { v = 0, x = 0 });
			Assert.AreEqual(1, testResult.v);
			Assert.AreEqual(2, testResult.w);
			Assert.AreEqual(3, testResult.x);
		}

		[Test]
		public void ProvideValueToInputOutputParam()
		{
			var db = new SPTestsDatabase();
			dynamic wArgs = new ExpandoObject();
			wArgs.w = null;
			// w := w + 2; v := w - 1; x := w + 1
			dynamic testResult = db.ExecuteAsProcedure("test_vars", ioParams: new { w = 2 }, outParams: new { v = 0, x = 0 });
			Assert.AreEqual(3, testResult.v);
			Assert.AreEqual(4, testResult.w);
			Assert.AreEqual(5, testResult.x);
		}

		[Test]
		public void ReadOutputParamsUsingQuery()
		{
			var db = new SPTestsDatabase();
			// Again this is Postgres specific: output params are really part of data row and can be read that way
			var record = db.QueryFromProcedure("test_vars", new { w = 2 }).FirstOrDefault();
			Assert.AreEqual(3, record.v);
			Assert.AreEqual(4, record.w);
			Assert.AreEqual(5, record.x);
		}

		[Test]
		public void QuerySetOfRecordsFromFunction()
		{
			var db = new SPTestsDatabase();
			var setOfRecords = db.QueryFromProcedure("sum_n_product_with_tab", new { x = 10 });
			int count = 0;
			foreach(var innerRecord in setOfRecords)
			{
				Console.WriteLine(innerRecord.sum + "\t|\t" + innerRecord.product);
				count++;
			}
			Assert.AreEqual(4, count);
		}

		[Test]
		public void DereferenceCursorOutputParameter()
		{
			var db = new SPTestsDatabase();
			// Unlike the Oracle data access layer, Npgsql v3 does not dereference cursors parameters.
			// We have added back the support for this which was previously in Npgsql v2.
			var employees = db.QueryFromProcedure("cursor_employees", outParams: new { refcursor = new Cursor() });
			int count = 0;
			foreach(var employee in employees)
			{
				Console.WriteLine(employee.firstname + " " + employee.lastname);
				count++;
			}
			Assert.AreEqual(9, count);
		}

		#region Dereferencing tests
		// Test various dereferencing patters (more relevant since we are coding this ourselves)
		private void CheckMultiResultSetStructure(IEnumerable<IEnumerable<dynamic>> results, int count0 = 1, int count1 = 1, bool breakTest = false)
		{
			int sets = 0;
			int[] counts = new int[2];
			foreach(var set in results)
			{
				foreach(var item in set)
				{
					counts[sets]++;
					if(sets == 0) Assert.AreEqual(typeof(int), item.a.GetType());
					else Assert.AreEqual(typeof(int), item.c.GetType());
					if(breakTest) break;
				}
				sets++;
			}
			Assert.AreEqual(2, sets);
			Assert.AreEqual(breakTest ? 1 : count0, counts[0]);
			Assert.AreEqual(breakTest ? 1 : count1, counts[1]);
		}

		[Test]
		public void DereferenceOneByNFromProcedure()
		{
			var db = new SPTestsDatabase();
			var resultSetOneByN = db.QueryMultipleFromProcedure("cursorOneByN", outParams: new { xyz = new Cursor() });
			CheckMultiResultSetStructure(resultSetOneByN);
		}

		[Test]
		public void DereferenceNByOneFromProcedure()
		{
			var db = new SPTestsDatabase();
			var resultSetNByOne = db.QueryMultipleFromProcedure("cursorNByOne", outParams: new { c1 = new Cursor(), c2 = new Cursor() });
			CheckMultiResultSetStructure(resultSetNByOne);
		}

		[Test]
		public void DereferenceOneByNFromQuery()
		{
			var db = new SPTestsDatabase();
			var resultSetOneByNSQL = db.QueryMultipleWithParams("SELECT * FROM cursorOneByN()",
																outParams: new { anyname = new Cursor() });
			CheckMultiResultSetStructure(resultSetOneByNSQL);
		}

		[Test]
		public void DereferenceNByOneFromQuery()
		{
			var db = new SPTestsDatabase();
			var resultSetNByOneSQL = db.QueryMultipleWithParams("SELECT * FROM cursorNByOne()",
																outParams: new { anyname = new Cursor() });
			CheckMultiResultSetStructure(resultSetNByOneSQL);
		}

		[Test]
		public void QueryMultipleWithBreaks()
		{
			var db = new SPTestsDatabase();
			var resultCTestFull = db.QueryMultipleFromProcedure("cbreaktest", outParams: new { c1 = new Cursor(), c2 = new Cursor() });
			CheckMultiResultSetStructure(resultCTestFull, 10, 11);
			var resultCTestToBreak = db.QueryMultipleFromProcedure("cbreaktest", ioParams: new { c1 = new Cursor(), c2 = new Cursor() });
			CheckMultiResultSetStructure(resultCTestToBreak, breakTest: true);
		}
		#endregion

		[Test]
		public void QueryFromMixedCursorOutput()
		{
			var db = new SPTestsDatabase();
			// Following the Oracle pattern this WILL dereference; so we need some more interesting result sets in there now.
			var firstItemCursorMix = db.QueryFromProcedure("cursor_mix", outParams: new { anyname = new Cursor(), othername = 0 }).FirstOrDefault();
			Assert.AreEqual(11, firstItemCursorMix.a);
			Assert.AreEqual(22, firstItemCursorMix.b);
		}

		[Test]
		public void NonQueryFromMixedCursorOutput()
		{
			var db = new SPTestsDatabase();
			// Following the Oracle pattern this will not dereference: we get a variable value and a cursor ref.
			var itemCursorMix = db.ExecuteAsProcedure("cursor_mix", outParams: new { anyname = new Cursor(), othername = 0 });
			Assert.AreEqual(42, itemCursorMix.othername);
			Assert.AreEqual(typeof(string), itemCursorMix.anyname.GetType()); // NB PostgreSql ref cursors return as string
		}

#if false
		[Test]
		public void HugeCursorTest()
		{
			var db = new SPTestsDatabase();

			//// Huge cursor tests....
			var config = db.Query("SELECT current_setting('work_mem') work_mem, current_setting('log_temp_files') log_temp_files").FirstOrDefault();

#if false
			// huge data from SELECT *
			var resultLargeSelectTest = db.QueryWithParams("SELECT * FROM large");
			foreach(var item in resultLargeSelectTest)
			{
				int a = 1;
			}

			// huge data from (implicit) FETCH ALL
			var resultLargeProcTest = db.QueryFromProcedure("lump", returnParams: new { abc = new Cursor() });
			foreach(var item in resultLargeProcTest)
			{
				break;
			}
#endif

			// one item from cursor
			using(var conn = db.OpenConnection())
			{
				using(var trans = conn.BeginTransaction())
				{
					var result = db.ExecuteAsProcedure("lump", conn, returnParams: new { abc = new Cursor() });
					var singleItemTest = db.QueryWithParams($@"FETCH 5000000 FROM ""{result.abc}"";", conn);
					foreach(var item in singleItemTest)
					{
						Console.WriteLine(item.id);
						break;
					}
					db.Execute($@"CLOSE ""{result.abc}"";", conn);
					trans.Commit();
				}
			}
		}
#endif

		public void ToDo()
		{
			var db = new SPTestsDatabase();

			// AFAIK these will never work (you can't assign to vars in SQL block)
			//dynamic intResult = db.Execute(":a := 1", inParams: new aArgs());
			//dynamic dateResult = db.Execute("begin :d := SYSDATE; end;", outParams: new myParamsD());
		}

		[TearDown]
		public void CleanUp()
		{
		}
	}
}
