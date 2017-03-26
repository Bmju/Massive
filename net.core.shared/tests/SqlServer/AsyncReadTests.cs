using System;
using System.Collections;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;
using Massive.Tests.SqlServer.TableClasses;
using Xunit;
#if !COREFX
using SD.Tools.OrmProfiler.Interceptor;
#endif

namespace Massive.Tests.SqlServer
{
	public class AsyncReadTests : IDisposable
    {
		private readonly string OrmProfilerApplicationName = "Massive SqlServer async read tests";

		public AsyncReadTests()
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
		public async Task AllAsync_NoParameters()
		{
			var soh = new SalesOrderHeader();
			var allRows = await soh.AllAsync();
			Assert.Equal(31465, allRows.Count);
		}


		[Fact]
		public async Task AllAsync_NoParameters_Streaming()
		{
			var soh = new SalesOrderHeader();
			var allRows = await soh.AllAsync();
			var count = 0;
			foreach(var r in allRows)
			{
				count++;
				Assert.Equal(26, ((IDictionary<string, object>)r).Count);		// # of fields fetched should be 26
			}
			Assert.Equal(31465, count);
		}


		[Fact]
		public async Task AllAsync_LimitSpecification()
		{
			var soh = new SalesOrderHeader();
			var allRows = await soh.AllAsync(limit: 10);
			Assert.Equal(10, allRows.Count);
		}
		

		[Fact]
		public async Task AllAsync_ColumnSpecification()
		{
			var soh = new SalesOrderHeader();
			var allRows = await soh.AllAsync(columns: "SalesOrderID as SOID, Status, SalesPersonID");
			Assert.Equal(31465, allRows.Count);
			var firstRow = (IDictionary<string, object>)allRows[0];
			Assert.Equal(3, firstRow.Count);
			Assert.True(firstRow.ContainsKey("SOID"));
			Assert.True(firstRow.ContainsKey("Status"));
			Assert.True(firstRow.ContainsKey("SalesPersonID"));
		}


		[Fact]
		public async Task AllAsync_OrderBySpecification()
		{
			var soh = new SalesOrderHeader();
			var allRows = await soh.AllAsync(orderBy: "CustomerID DESC");
			Assert.Equal(31465, allRows.Count);
			int previous = int.MaxValue;
			foreach(var r in allRows)
			{
				int current = r.CustomerID;
				Assert.True(current <= previous);
				previous = current;
			}
		}


		[Fact]
		public async Task AllAsync_WhereSpecification()
		{
			var soh = new SalesOrderHeader();
			var allRows = await soh.AllAsync(where: "WHERE CustomerId=@0", args: 30052);
			Assert.Equal(4, allRows.Count);
		}


		[Fact]
		public async Task AllAsync_WhereSpecification_OrderBySpecification()
		{
			var soh = new SalesOrderHeader();
			var allRows = await soh.AllAsync(orderBy: "SalesOrderID DESC", where: "WHERE CustomerId=@0", args: 30052);
			Assert.Equal(4, allRows.Count);
			int previous = int.MaxValue;
			foreach(var r in allRows)
			{
				int current = r.SalesOrderID;
				Assert.True(current <= previous);
				previous = current;
			}
		}
		

		[Fact]
		public async Task AllAsync_WhereSpecification_ColumnsSpecification()
		{
			var soh = new SalesOrderHeader();
			var allRows = await soh.AllAsync(columns: "SalesOrderID as SOID, Status, SalesPersonID", where: "WHERE CustomerId=@0", args: 30052);
			Assert.Equal(4, allRows.Count);
			var firstRow = (IDictionary<string, object>)allRows[0];
			Assert.Equal(3, firstRow.Count);
			Assert.True(firstRow.ContainsKey("SOID"));
			Assert.True(firstRow.ContainsKey("Status"));
			Assert.True(firstRow.ContainsKey("SalesPersonID"));
		}


		[Fact]
		public async Task AllAsync_WhereSpecification_ColumnsSpecification_LimitSpecification()
		{
			var soh = new SalesOrderHeader();
			var allRows = await soh.AllAsync(limit: 2, columns: "SalesOrderID as SOID, Status, SalesPersonID", where: "WHERE CustomerId=@0", args: 30052);
			Assert.Equal(2, allRows.Count);
			var firstRow = (IDictionary<string, object>)allRows[0];
			Assert.Equal(3, firstRow.Count);
			Assert.True(firstRow.ContainsKey("SOID"));
			Assert.True(firstRow.ContainsKey("Status"));
			Assert.True(firstRow.ContainsKey("SalesPersonID"));
		}

		[Fact]
		public async Task QueryAsync_AllRows()
		{
			var soh = new SalesOrderHeader();
			var allRows = await soh.QueryAsync("SELECT * FROM Sales.SalesOrderHeader");
			Assert.Equal(31465, allRows.Count);
		}


		[Fact]
		public async Task QueryAsync_Filter()
		{
			var soh = new SalesOrderHeader();
			var filteredRows = await soh.QueryAsync("SELECT * FROM Sales.SalesOrderHeader WHERE CustomerID=@0", 30052);
			Assert.Equal(4, filteredRows.Count);
		}


		[Fact]
		public async Task PagedAsync_NoSpecification()
		{
			var soh = new SalesOrderHeader();
			// no order by, so in theory this is useless. It will order on PK though
			var page2 = await soh.PagedAsync(currentPage:2, pageSize: 30);
			var pageItems = ((IEnumerable<dynamic>)page2.Items).ToList();
			Assert.Equal(30, pageItems.Count);
			Assert.Equal(31465, page2.TotalRecords);
		}


		[Fact]
		public async Task PagedAsync_OrderBySpecification()
		{
			var soh = new SalesOrderHeader();
			var page2 = await soh.PagedAsync(orderBy: "CustomerID DESC", currentPage: 2, pageSize: 30);
			var pageItems = ((IEnumerable<dynamic>)page2.Items).ToList();
			Assert.Equal(30, pageItems.Count);
			Assert.Equal(31465, page2.TotalRecords);

			int previous = int.MaxValue;
			foreach(var r in pageItems)
			{
				int current = r.CustomerID;
				Assert.True(current <= previous);
				previous = current;
			}
		}


		[Fact]
		public async Task PagedAsync_OrderBySpecification_ColumnsSpecification()
		{
			var soh = new SalesOrderHeader();
			var page2 = await soh.PagedAsync(columns: "CustomerID, SalesOrderID", orderBy: "CustomerID DESC", currentPage: 2, pageSize: 30);
			var pageItems = ((IEnumerable<dynamic>)page2.Items).ToList();
			Assert.Equal(30, pageItems.Count);
			Assert.Equal(31465, page2.TotalRecords);
			var firstRow = (IDictionary<string, object>)pageItems[0];
			Assert.Equal(2, firstRow.Count);
			int previous = int.MaxValue;
			foreach(var r in pageItems)
			{
				int current = r.CustomerID;
				Assert.True(current <= previous);
				previous = current;
			}
		}


		[Fact]
		public async Task CountAsync_NoSpecification()
		{
			var soh = new SalesOrderHeader();
			var total = await soh.CountAsync();
			Assert.Equal(31465, total);
		}


		[Fact]
		public async Task CountAsync_WhereSpecification()
		{
			var soh = new SalesOrderHeader();
			var total = await soh.CountAsync(where: "WHERE CustomerId=@0", args:30052);
			Assert.Equal(4, total);
		}
	}
}
