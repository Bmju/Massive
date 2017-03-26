using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Dynamic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using Massive.Tests.SqlServer.TableClasses;
using Xunit;
#if !COREFX
using SD.Tools.OrmProfiler.Interceptor;
#endif

namespace Massive.Tests.SqlServer
{
	public class ReadTests : IDisposable
    {
		private readonly string OrmProfilerApplicationName = "Massive SqlServer read tests";

		public ReadTests()
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
		public void Guid_Arg()
		{
			// SQL Server has true Guid type support
			var db = new DynamicModel(TestConstants.ReadTestConnection);
			var guid = Guid.NewGuid();
			var command = db.CreateCommand("SELECT @0 AS val", null, guid);
			Assert.Equal(DbType.Guid, command.Parameters[0].DbType);
			var item = db.Query(command).FirstOrDefault();
			Assert.Equal(guid, item.val);
		}


		[Fact]
		public void MaxOnFilteredSet()
		{
			var soh = new SalesOrderHeader();
			var result = ((dynamic)soh).Max(columns: "SalesOrderID", where: "SalesOrderID < @0", args: 100000);
			Assert.Equal(75123, result);
		}


		[Fact]
		public void MaxOnFilteredSet2()
		{
			var soh = new SalesOrderHeader();
			var result = ((dynamic)soh).Max(columns: "SalesOrderID", TerritoryID: 10);
			Assert.Equal(75117, result);
		}


		[Fact]
		public void EmptyElement_ProtoType()
		{
			var soh = new SalesOrderHeader();
			dynamic defaults = soh.Prototype;
			Assert.True(defaults.OrderDate > DateTime.MinValue);
		}


		[Fact]
		public void SchemaMetaDataRetrieval()
		{
			var soh = new SalesOrderHeader();
			var schema = soh.Schema;
			Assert.NotNull(schema);
			Assert.Equal(26, schema.Count());
			Assert.True(schema.All(v=>v.TABLE_NAME==soh.TableNameWithoutSchema));
		}


		[Fact]
		public void All_NoParameters()
		{
			var soh = new SalesOrderHeader();
			var allRows = soh.All().ToList();
			Assert.Equal(31465, allRows.Count);
		}


		[Fact]
		public void All_NoParameters_Streaming()
		{
			var soh = new SalesOrderHeader();
			var allRows = soh.All();
			var count = 0;
			foreach(var r in allRows)
			{
				count++;
				Assert.Equal(26, ((IDictionary<string, object>)r).Count);		// # of fields fetched should be 26
			}
			Assert.Equal(31465, count);
		}


		[Fact]
		public void All_LimitSpecification()
		{
			var soh = new SalesOrderHeader();
			var allRows = soh.All(limit: 10).ToList();
			Assert.Equal(10, allRows.Count);
		}
		

		[Fact]
		public void All_ColumnSpecification()
		{
			var soh = new SalesOrderHeader();
			var allRows = soh.All(columns: "SalesOrderID as SOID, Status, SalesPersonID").ToList();
			Assert.Equal(31465, allRows.Count);
			var firstRow = (IDictionary<string, object>)allRows[0];
			Assert.Equal(3, firstRow.Count);
			Assert.True(firstRow.ContainsKey("SOID"));
			Assert.True(firstRow.ContainsKey("Status"));
			Assert.True(firstRow.ContainsKey("SalesPersonID"));
		}


		[Fact]
		public void All_OrderBySpecification()
		{
			var soh = new SalesOrderHeader();
			var allRows = soh.All(orderBy: "CustomerID DESC").ToList();
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
		public void All_WhereSpecification()
		{
			var soh = new SalesOrderHeader();
			var allRows = soh.All(where: "WHERE CustomerId=@0", args: 30052).ToList();
			Assert.Equal(4, allRows.Count);
		}


		[Fact]
		public void All_WhereSpecification_OrderBySpecification()
		{
			var soh = new SalesOrderHeader();
			var allRows = soh.All(orderBy: "SalesOrderID DESC", where: "WHERE CustomerId=@0", args: 30052).ToList();
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
		public void All_WhereSpecification_ColumnsSpecification()
		{
			var soh = new SalesOrderHeader();
			var allRows = soh.All(columns: "SalesOrderID as SOID, Status, SalesPersonID", where: "WHERE CustomerId=@0", args: 30052).ToList();
			Assert.Equal(4, allRows.Count);
			var firstRow = (IDictionary<string, object>)allRows[0];
			Assert.Equal(3, firstRow.Count);
			Assert.True(firstRow.ContainsKey("SOID"));
			Assert.True(firstRow.ContainsKey("Status"));
			Assert.True(firstRow.ContainsKey("SalesPersonID"));
		}


		[Fact]
		public void All_WhereSpecification_ColumnsSpecification_LimitSpecification()
		{
			var soh = new SalesOrderHeader();
			var allRows = soh.All(limit: 2, columns: "SalesOrderID as SOID, Status, SalesPersonID", where: "WHERE CustomerId=@0", args: 30052).ToList();
			Assert.Equal(2, allRows.Count);
			var firstRow = (IDictionary<string, object>)allRows[0];
			Assert.Equal(3, firstRow.Count);
			Assert.True(firstRow.ContainsKey("SOID"));
			Assert.True(firstRow.ContainsKey("Status"));
			Assert.True(firstRow.ContainsKey("SalesPersonID"));
		}


#if !COREFX
		[Fact]
		public void All_WhereSpecification_ToDataTable()
		{
			var soh = new SalesOrderHeader();
			var allRows = soh.All(where: "WHERE CustomerId=@0", args: 30052).ToList();
			Assert.Equal(4, allRows.Count);

			var allRowsAsDataTable = soh.All(where: "WHERE CustomerId=@0", args: 30052).ToDataTable();
			Assert.Equal(allRows.Count, allRowsAsDataTable.Rows.Count);
			for(int i = 0; i < allRows.Count; i++)
			{
				Assert.Equal(allRows[i].SalesOrderID, allRowsAsDataTable.Rows[i]["SalesOrderId"]);
				Assert.Equal(30052, allRowsAsDataTable.Rows[i]["CustomerId"]);
			}
		}
#endif


		[Fact]
		public void Find_AllColumns()
		{
			dynamic soh = new SalesOrderHeader();
			var singleInstance = soh.Find(SalesOrderID: 43666);
			Assert.Equal(43666, singleInstance.SalesOrderID);
		}


		[Fact]
		public void Find_OneColumn()
		{
			dynamic soh = new SalesOrderHeader();
			var singleInstance = soh.Find(SalesOrderID: 43666, columns:"SalesOrderID");
			Assert.Equal(43666, singleInstance.SalesOrderID);
			var siAsDict = (IDictionary<string, object>)singleInstance;
			Assert.Equal(1, siAsDict.Count);
		}


		[Fact]
		public void Get_AllColumns()
		{
			dynamic soh = new SalesOrderHeader();
			var singleInstance = soh.Get(SalesOrderID: 43666);
			Assert.Equal(43666, singleInstance.SalesOrderID);
		}


		[Fact]
		public void First_AllColumns()
		{
			dynamic soh = new SalesOrderHeader();
			var singleInstance = soh.First(SalesOrderID: 43666);
			Assert.Equal(43666, singleInstance.SalesOrderID);
		}


		[Fact]
		public void Single_AllColumns()
		{
			dynamic soh = new SalesOrderHeader();
			var singleInstance = soh.Single(SalesOrderID: 43666);
			Assert.Equal(43666, singleInstance.SalesOrderID);
			Assert.Equal(26, ((object)singleInstance).ToDictionary().Count);
		}


		[Fact]
		public void Single_ThreeColumns()
		{
			dynamic soh = new SalesOrderHeader();
			var singleInstance = soh.Single(SalesOrderID: 43666, columns: "SalesOrderID, SalesOrderNumber, OrderDate");
			Assert.Equal(43666, singleInstance.SalesOrderID);
			Assert.Equal("SO43666", singleInstance.SalesOrderNumber);
			Assert.Equal(new DateTime(2011, 5, 31), singleInstance.OrderDate);
			Assert.Equal(3, ((object)singleInstance).ToDictionary().Count);
		}


		[Fact]
		public void Query_AllRows()
		{
			var soh = new SalesOrderHeader();
			var allRows = soh.Query("SELECT * FROM Sales.SalesOrderHeader").ToList();
			Assert.Equal(31465, allRows.Count);
		}


		[Fact]
		public void Query_Filter()
		{
			var soh = new SalesOrderHeader();
			var filteredRows = soh.Query("SELECT * FROM Sales.SalesOrderHeader WHERE CustomerID=@0", 30052).ToList();
			Assert.Equal(4, filteredRows.Count);
		}


		[Fact]
		public void Paged_NoSpecification()
		{
			var soh = new SalesOrderHeader();
			// no order by, so in theory this is useless. It will order on PK though
			var page2 = soh.Paged(currentPage:2, pageSize: 30);
			var pageItems = ((IEnumerable<dynamic>)page2.Items).ToList();
			Assert.Equal(30, pageItems.Count);
			Assert.Equal(31465, page2.TotalRecords);
		}


		[Fact]
		public void Paged_OrderBySpecification()
		{
			var soh = new SalesOrderHeader();
			var page2 = soh.Paged(orderBy: "CustomerID DESC", currentPage: 2, pageSize: 30);
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
		public void Paged_OrderBySpecification_ColumnsSpecification()
		{
			var soh = new SalesOrderHeader();
			var page2 = soh.Paged(columns: "CustomerID, SalesOrderID", orderBy: "CustomerID DESC", currentPage: 2, pageSize: 30);
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
		public void Count_NoSpecification()
		{
			var soh = new SalesOrderHeader();
			var total = soh.Count();
			Assert.Equal(31465, total);
		}


		[Fact]
		public void Count_WhereSpecification_FromArgs()
		{
			var soh = new SalesOrderHeader();
			var total = soh.Count(where: "WHERE CustomerId=@0", args:11212);
			Assert.Equal(17, total);
		}


		[Fact]
		public void Count_WhereSpecification_FromArgsPlusNameValue()
		{
			dynamic soh = new SalesOrderHeader();
			var total = soh.Count(where: "WHERE CustomerId=@0", args: 11212, ModifiedDate: new DateTime(2013, 10, 10));
			Assert.Equal(2, total);
		}


		[Fact]
		public void Count_WhereSpecification_FromNameValuePairs()
		{
			dynamic soh = new SalesOrderHeader();
			var total = soh.Count(CustomerID: 11212, ModifiedDate: new DateTime(2013, 10, 10));
			Assert.Equal(2, total);
		}


		/// <remarks>
		/// With correct brackets round the WHERE condition in the SQL this returns 17, otherwise it returns 31465!
		/// </remarks>
		[Fact]
		public void Count_TestWhereWrapping()
		{
			dynamic soh = new SalesOrderHeader();
			var total = soh.Count(where: "1=1 OR 0=0", CustomerID: 11212);
			Assert.Equal(17, total);
		}


		[Fact]
		public void DefaultValue()
		{
			var soh = new SalesOrderHeader(false);
			var value = soh.DefaultValue("OrderDate");
			Assert.Equal(typeof(DateTime), value.GetType());
		}


		[Fact]
		public void IsValid_SalesPersonIDCheck()
		{
			dynamic soh = new SalesOrderHeader();
			var toValidate = soh.Find(SalesOrderID: 45816);
			// is invalid
			Assert.False(soh.IsValid(toValidate));
			Assert.Equal(1, soh.Errors.Count);

			toValidate = soh.Find(SalesOrderID: 45069);
			// is valid
			Assert.True(soh.IsValid(toValidate));
			Assert.Equal(0, soh.Errors.Count);
		}


		[Fact]
		public void PrimaryKey_Read_Check()
		{
			dynamic soh = new SalesOrderHeader();
			var toValidate = soh.Find(SalesOrderID: 45816);

			Assert.True(soh.HasPrimaryKey(toValidate));

			var pkValue = soh.GetPrimaryKey(toValidate);
			Assert.Equal(45816, pkValue);
		}
	}
}
