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
	/// Specific tests for code which is specific to Oracle. This means there are fewer tests than for SQL Server, as logic that's covered there already doesn't have to be
	/// retested again here, as the tests are meant to see whether a feature works. Tests are designed to touch the code in Massive.Oracle. 
	/// </summary>
	/// <remarks>These tests run on x64 by default, as by default ODP.NET installs x64 only. If you have x86 ODP.NET installed, change the build directive to AnyCPU
	/// in the project settings.<br/>
	/// These tests use the SCOTT test DB shipped by Oracle. Your values may vary though. </remarks>
	public class ReadWriteTests : IDisposable
	{
		public static IEnumerable<object[]> ProviderNames = new[] {
			new object[] { "Oracle.ManagedDataAccess.Client" },
			new object[] { "Oracle.DataAccess.Client" }
		};

		private readonly string OrmProfilerApplicationName = "Massive Oracle read/write tests";


		public ReadWriteTests()
		{
			Console.WriteLine("Entering " + OrmProfilerApplicationName);
#if !COREFX
			InterceptorCore.Initialize(OrmProfilerApplicationName);
#endif
		}


		public void Dispose()
		{
			Console.WriteLine("Exiting " + OrmProfilerApplicationName);

			foreach(object[] ProviderName in ProviderNames)
			{
				// delete all rows with department name 'Massive Dep'. 
				var depts = new Department((string)ProviderName[0]);
				depts.Delete(null, "DNAME=:0", "Massive Dep");
			}
		}


		[Theory]
		[MemberData(nameof(ProviderNames))]
		public void Guid_Arg(string ProviderName)
		{
			// Oracle has no Guid parameter support, Massive maps Guid to string in Oracle
			var db = new DynamicModel(string.Format(TestConstants.ReadWriteTestConnection, ProviderName));
			var guid = Guid.NewGuid();
			var inParams = new { inval = guid };
			var outParams = new { val = new Guid() };
			var command = db.CreateCommandWithParams("begin :val := :inval; end;", inParams: inParams, outParams: outParams);
			Assert.Equal(DbType.String, command.Parameters[0].DbType);
			var item = db.ExecuteWithParams(command);
			Assert.Equal(typeof(string), item.val.GetType());
			Assert.Equal(guid, new Guid(item.val));
		}


		[Theory]
		[MemberData(nameof(ProviderNames))]
		public void All_NoParameters(string ProviderName)
		{
			var depts = new Department(ProviderName);
			var allRows = depts.All().ToList();
			Assert.Equal(60, allRows.Count);
			foreach(var d in allRows)
			{
				Console.WriteLine("{0} {1} {2}", d.DEPTNO, d.DNAME, d.LOC);
			}
		}


		[Theory]
		[MemberData(nameof(ProviderNames))]
		public void All_LimitSpecification(string ProviderName)
		{
			var depts = new Department(ProviderName);
			var allRows = depts.All(limit: 10).ToList();
			Assert.Equal(10, allRows.Count);
		}


		[Theory]
		[MemberData(nameof(ProviderNames))]
		public void All_WhereSpecification_OrderBySpecification(string ProviderName)
		{
			var depts = new Department(ProviderName);
			var allRows = depts.All(orderBy: "DEPTNO DESC", where: "WHERE LOC=:0", args: "Nowhere").ToList();
			Assert.Equal(9, allRows.Count);
			int previous = int.MaxValue;
			foreach(var r in allRows)
			{
				int current = r.DEPTNO;
				Assert.True(current <= previous);
				previous = current;
			}
		}


		[Theory]
		[MemberData(nameof(ProviderNames))]
		public void All_WhereSpecification_OrderBySpecification_LimitSpecification(string ProviderName)
		{
			var depts = new Department(ProviderName);
			var allRows = depts.All(limit: 6, orderBy: "DEPTNO DESC", where: "WHERE LOC=:0", args: "Nowhere").ToList();
			Assert.Equal(6, allRows.Count);
			int previous = int.MaxValue;
			foreach(var r in allRows)
			{
				int current = r.DEPTNO;
				Assert.True(current <= previous);
				previous = current;
			}
		}


		[Theory]
		[MemberData(nameof(ProviderNames))]
		public void Paged_NoSpecification(string ProviderName)
		{
			var depts = new Department(ProviderName);
			// no order by, so in theory this is useless. It will order on PK though
			var page2 = depts.Paged(currentPage: 2, pageSize: 10);
			var pageItems = ((IEnumerable<dynamic>)page2.Items).ToList();
			Assert.Equal(10, pageItems.Count);
			Assert.Equal(60, page2.TotalRecords);
		}


		[Theory]
		[MemberData(nameof(ProviderNames))]
		public void Paged_OrderBySpecification(string ProviderName)
		{
			var depts = new Department(ProviderName);
			var page2 = depts.Paged(orderBy: "DEPTNO DESC", currentPage: 2, pageSize: 10);
			var pageItems = ((IEnumerable<dynamic>)page2.Items).ToList();
			Assert.Equal(10, pageItems.Count);
			Assert.Equal(60, page2.TotalRecords);
		}


		[Theory]
		[MemberData(nameof(ProviderNames))]
		public void Paged_SqlSpecification(string ProviderName)
		{
			var depts = new Department(ProviderName);
			var page2 = depts.Paged(sql: "SELECT * FROM DEPT", primaryKey: "DEPTNO", pageSize: 10, currentPage: 2);
			var pageItems = ((IEnumerable<dynamic>)page2.Items).ToList();
			Assert.Equal(10, pageItems.Count);
			Assert.Equal(60, page2.TotalRecords);
		}


		[Theory]
		[MemberData(nameof(ProviderNames))]
		public void Insert_SingleRow(string ProviderName)
		{
			var depts = new Department(ProviderName);
			var inserted = depts.Insert(new { DNAME = "Massive Dep", LOC = "Beach" });
			Assert.True(inserted.DEPTNO > 0);
			Assert.Equal(1, depts.Delete(inserted.DEPTNO));
		}


		[Theory]
		[MemberData(nameof(ProviderNames))]
		public void Save_SingleRow(string ProviderName)
		{
			var depts = new Department(ProviderName);
			dynamic toSave = new { DNAME = "Massive Dep", LOC = "Beach" }.ToExpando();
			var result = depts.Save(toSave);
			Assert.Equal(1, result);
			Assert.True(toSave.DEPTNO > 0);
			Assert.Equal(1, depts.Delete(toSave.DEPTNO));
		}


		[Theory]
		[MemberData(nameof(ProviderNames))]
		public void Save_MultipleRows(string ProviderName)
		{
			var depts = new Department(ProviderName);
			object[] toSave = new object[]
								   {
									   new {DNAME = "Massive Dep", LOC = "Beach"}.ToExpando(),
									   new {DNAME = "Massive Dep", LOC = "DownTown"}.ToExpando()
								   };
			var result = depts.Save(toSave);
			Assert.Equal(2, result);
			foreach(dynamic o in toSave)
			{
				Assert.True(o.DEPTNO > 0);
			}

			// read them back, update them, save them again, 
			var savedDeps = depts.All(where: "WHERE DEPTNO=:0 OR DEPTNO=:1", args: new object[] { ((dynamic)toSave[0]).DEPTNO, ((dynamic)toSave[1]).DEPTNO }).ToList();
			Assert.Equal(2, savedDeps.Count);
			savedDeps[0].LOC += "C";
			savedDeps[1].LOC += "C";
			result = depts.Save(toSave);
			Assert.Equal(2, result);
			Assert.Equal(1, depts.Delete(savedDeps[0].DEPTNO));
			Assert.Equal(1, depts.Delete(savedDeps[1].DEPTNO));
		}
	}
}
#endif