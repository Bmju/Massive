using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Dynamic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using Massive.Tests.MySql.TableClasses;
using Xunit;
#if !COREFX
using SD.Tools.OrmProfiler.Interceptor;
#endif

namespace Massive.Tests.MySql
{
	public class ReadTests : IDisposable
	{
		public static IEnumerable<object[]> ProviderNames = new[] {
			new object[] { "MySql.Data.MySqlClient" }
#if !COREFX
		  , new object[] { "Devart.Data.MySql" }
#endif
		};

		private readonly string OrmProfilerApplicationName = "Massive MySql read tests";

		public ReadTests()
		{
			Console.WriteLine("Entering " + OrmProfilerApplicationName);
#if !COREFX
			InterceptorCore.Initialize("Massive MySql read tests");
#endif
		}

		public void Dispose()
		{
			Console.WriteLine("Exiting " + OrmProfilerApplicationName);
		}


		[Theory]
		[MemberData(nameof(ProviderNames))]
		public void Guid_Arg(string ProviderName)
		{
			// MySQL has native Guid parameter support, but the SELECT output is a string
			var db = new DynamicModel(string.Format(TestConstants.ReadTestConnection, ProviderName));
			var guid = Guid.NewGuid();
			var command = db.CreateCommand("SELECT @0 AS val", null, guid);
			Assert.Equal(DbType.Guid, command.Parameters[0].DbType);
			var item = db.Query(command).FirstOrDefault();
			Assert.Equal(typeof(string), item.val.GetType());
			Assert.Equal(guid, new Guid(item.val));
		}


		[Theory]
		[MemberData(nameof(ProviderNames))]
		public void Max_SingleArg(string ProviderName)
		{
			var soh = new Film(ProviderName);
			var result = ((dynamic)soh).Max(columns: "film_id", where: "rental_duration > @0", args: 6);
			Assert.Equal(988, result);
		}


		[Theory]
		[MemberData(nameof(ProviderNames))]
		public void Max_TwoArgs(string ProviderName)
		{
			var soh = new Film(ProviderName);
			var result = ((dynamic)soh).Max(columns: "film_id", where: "rental_duration > @0 AND rental_duration < @1", args: new object[] { 6, 100 });
			Assert.Equal(988, result);
		}


		[Theory]
		[MemberData(nameof(ProviderNames))]
		public void Max_NameValuePair(string ProviderName)
		{
			var film = new Film(ProviderName);
			var result = ((dynamic)film).Max(columns: "film_id", rental_duration: 6);
			Assert.Equal(998, result);
		}


		[Theory]
		[MemberData(nameof(ProviderNames))]
		public void EmptyElement_ProtoType(string ProviderName)
		{
			var film = new Film(ProviderName);
			dynamic defaults = film.Prototype;
			Assert.True(defaults.last_update > DateTime.MinValue);
		}


		[Theory]
		[MemberData(nameof(ProviderNames))]
		public void SchemaMetaDataRetrieval(string ProviderName)
		{
			var film = new Film(ProviderName);
			var schema = film.Schema;
			Assert.NotNull(schema);
			Assert.Equal(13, schema.Count());
			Assert.True(schema.All(v => v.TABLE_NAME == film.TableNameWithoutSchema));
		}


		[Theory]
		[MemberData(nameof(ProviderNames))]
		public void All_NoParameters(string ProviderName)
		{
			var film = new Film(ProviderName);
			var allRows = film.All().ToList();
			Assert.Equal(1000, allRows.Count);
		}


		[Theory]
		[MemberData(nameof(ProviderNames))]
		public void All_NoParameters_Streaming(string ProviderName)
		{
			var film = new Film(ProviderName);
			var allRows = film.All();
			var count = 0;
			foreach(var r in allRows)
			{
				count++;
				Assert.Equal(13, ((IDictionary<string, object>)r).Count);        // # of fields fetched should be 13
			}
			Assert.Equal(1000, count);
		}


		[Theory]
		[MemberData(nameof(ProviderNames))]
		public void All_LimitSpecification(string ProviderName)
		{
			var film = new Film(ProviderName);
			var allRows = film.All(limit: 10).ToList();
			Assert.Equal(10, allRows.Count);
		}


		[Theory]
		[MemberData(nameof(ProviderNames))]
		public void All_ColumnSpecification(string ProviderName)
		{
			var film = new Film(ProviderName);
			var allRows = film.All(columns: "film_id as FILMID, description, language_id").ToList();
			Assert.Equal(1000, allRows.Count);
			var firstRow = (IDictionary<string, object>)allRows[0];
			Assert.Equal(3, firstRow.Count);
			Assert.True(firstRow.ContainsKey("FILMID"));
			Assert.True(firstRow.ContainsKey("description"));
			Assert.True(firstRow.ContainsKey("language_id"));
		}


		[Theory]
		[MemberData(nameof(ProviderNames))]
		public void All_OrderBySpecification(string ProviderName)
		{
			var film = new Film(ProviderName);
			var allRows = film.All(orderBy: "rental_duration DESC").ToList();
			Assert.Equal(1000, allRows.Count);
			int previous = int.MaxValue;
			foreach(var r in allRows)
			{
				int current = r.rental_duration;
				Assert.True(current <= previous);
				previous = current;
			}
		}


		[Theory]
		[MemberData(nameof(ProviderNames))]
		public void All_WhereSpecification(string ProviderName)
		{
			var film = new Film(ProviderName);
			var allRows = film.All(where: "WHERE rental_duration=@0", args: 5).ToList();
			Assert.Equal(191, allRows.Count);
		}


		[Theory]
		[MemberData(nameof(ProviderNames))]
		public void All_WhereSpecification_OrderBySpecification(string ProviderName)
		{
			var film = new Film(ProviderName);
			var allRows = film.All(orderBy: "film_id DESC", where: "WHERE rental_duration=@0", args: 5).ToList();
			Assert.Equal(191, allRows.Count);
			int previous = int.MaxValue;
			foreach(var r in allRows)
			{
				int current = r.film_id;
				Assert.True(current <= previous);
				previous = current;
			}
		}


		[Theory]
		[MemberData(nameof(ProviderNames))]
		public void All_WhereSpecification_ColumnsSpecification(string ProviderName)
		{
			var film = new Film(ProviderName);
			var allRows = film.All(columns: "film_id as FILMID, description, language_id", where: "WHERE rental_duration=@0", args: 5).ToList();
			Assert.Equal(191, allRows.Count);
			var firstRow = (IDictionary<string, object>)allRows[0];
			Assert.Equal(3, firstRow.Count);
			Assert.True(firstRow.ContainsKey("FILMID"));
			Assert.True(firstRow.ContainsKey("description"));
			Assert.True(firstRow.ContainsKey("language_id"));
		}


		[Theory]
		[MemberData(nameof(ProviderNames))]
		public void All_WhereSpecification_ColumnsSpecification_LimitSpecification(string ProviderName)
		{
			var film = new Film(ProviderName);
			var allRows = film.All(limit: 2, columns: "film_id as FILMID, description, language_id", where: "WHERE rental_duration=@0", args: 5).ToList();
			Assert.Equal(2, allRows.Count);
			var firstRow = (IDictionary<string, object>)allRows[0];
			Assert.Equal(3, firstRow.Count);
			Assert.True(firstRow.ContainsKey("FILMID"));
			Assert.True(firstRow.ContainsKey("description"));
			Assert.True(firstRow.ContainsKey("language_id"));
		}


#if !COREFX
		[Theory]
		[MemberData(nameof(ProviderNames))]
		public void All_WhereSpecification_ToDataTable(string ProviderName)
		{
			var film = new Film(ProviderName);
			var allRows = film.All(where: "WHERE rental_duration=@0", args: 5).ToList();
			Assert.Equal(191, allRows.Count);

			var allRowsAsDataTable = film.All(where: "WHERE rental_duration=@0", args: 5).ToDataTable();
			Assert.Equal(allRows.Count, allRowsAsDataTable.Rows.Count);
			for(int i = 0; i < allRows.Count; i++)
			{
				Assert.Equal(allRows[i].film_id, allRowsAsDataTable.Rows[i]["film_id"]);
				Assert.Equal((byte)5, allRowsAsDataTable.Rows[i]["rental_duration"]);
			}
		}
#endif


		[Theory]
		[MemberData(nameof(ProviderNames))]
		public void Find_AllColumns(string ProviderName)
		{
			dynamic film = new Film(ProviderName);
			var singleInstance = film.Find(film_id: 43);
			Assert.Equal(43, singleInstance.film_id);
		}


		[Theory]
		[MemberData(nameof(ProviderNames))]
		public void Find_OneColumn(string ProviderName)
		{
			dynamic film = new Film(ProviderName);
			var singleInstance = film.Find(film_id: 43, columns: "film_id");
			Assert.Equal(43, singleInstance.film_id);
			var siAsDict = (IDictionary<string, object>)singleInstance;
			Assert.Equal(1, siAsDict.Count);
		}


		[Theory]
		[MemberData(nameof(ProviderNames))]
		public void Get_AllColumns(string ProviderName)
		{
			dynamic film = new Film(ProviderName);
			var singleInstance = film.Get(film_id: 43);
			Assert.Equal(43, singleInstance.film_id);
		}


		[Theory]
		[MemberData(nameof(ProviderNames))]
		public void First_AllColumns(string ProviderName)
		{
			dynamic film = new Film(ProviderName);
			var singleInstance = film.First(film_id: 43);
			Assert.Equal(43, singleInstance.film_id);
		}


		[Theory]
		[MemberData(nameof(ProviderNames))]
		public void Single_AllColumns(string ProviderName)
		{
			dynamic film = new Film(ProviderName);
			var singleInstance = film.Single(film_id: 43);
			Assert.Equal(43, singleInstance.film_id);
		}


		[Theory]
		[MemberData(nameof(ProviderNames))]
		public void Query_AllRows(string ProviderName)
		{
			var film = new Film(ProviderName);
			var allRows = film.Query("SELECT * FROM sakila.film").ToList();
			Assert.Equal(1000, allRows.Count);
		}


		[Theory]
		[MemberData(nameof(ProviderNames))]
		public void Query_Filter(string ProviderName)
		{
			var film = new Film(ProviderName);
			var filteredRows = film.Query("SELECT * FROM sakila.film WHERE rental_duration=@0", 5).ToList();
			Assert.Equal(191, filteredRows.Count);
		}


		[Theory]
		[MemberData(nameof(ProviderNames))]
		public void Paged_NoSpecification(string ProviderName)
		{
			var film = new Film(ProviderName);
			// no order by, so in theory this is useless. It will order on PK though
			var page2 = film.Paged(currentPage: 2, pageSize: 30);
			var pageItems = ((IEnumerable<dynamic>)page2.Items).ToList();
			Assert.Equal(30, pageItems.Count);
			Assert.Equal(1000, page2.TotalRecords);
		}


		[Theory]
		[MemberData(nameof(ProviderNames))]
		public void Paged_OrderBySpecification(string ProviderName)
		{
			var film = new Film(ProviderName);
			var page2 = film.Paged(orderBy: "rental_duration DESC", currentPage: 2, pageSize: 30);
			var pageItems = ((IEnumerable<dynamic>)page2.Items).ToList();
			Assert.Equal(30, pageItems.Count);
			Assert.Equal(1000, page2.TotalRecords);

			int previous = int.MaxValue;
			foreach(var r in pageItems)
			{
				int current = r.rental_duration;
				Assert.True(current <= previous);
				previous = current;
			}
		}


		[Theory]
		[MemberData(nameof(ProviderNames))]
		public void Paged_OrderBySpecification_ColumnsSpecification(string ProviderName)
		{
			var film = new Film(ProviderName);
			var page2 = film.Paged(columns: "rental_duration, film_id", orderBy: "rental_duration DESC", currentPage: 2, pageSize: 30);
			var pageItems = ((IEnumerable<dynamic>)page2.Items).ToList();
			Assert.Equal(30, pageItems.Count);
			Assert.Equal(1000, page2.TotalRecords);
			var firstRow = (IDictionary<string, object>)pageItems[0];
			Assert.Equal(2, firstRow.Count);
			int previous = int.MaxValue;
			foreach(var r in pageItems)
			{
				int current = r.rental_duration;
				Assert.True(current <= previous);
				previous = current;
			}
		}


		[Theory]
		[MemberData(nameof(ProviderNames))]
		public void Count_NoSpecification(string ProviderName)
		{
			var film = new Film(ProviderName);
			var total = film.Count();
			Assert.Equal(1000, total);
		}


		[Theory]
		[MemberData(nameof(ProviderNames))]
		public void Count_WhereSpecification(string ProviderName)
		{
			var film = new Film(ProviderName);
			var total = film.Count(where: "WHERE rental_duration=@0", args: 5);
			Assert.Equal(191, total);
		}


		[Theory]
		[MemberData(nameof(ProviderNames))]
		public void DefaultValue(string ProviderName)
		{
			var film = new Film(ProviderName, false);
			var value = film.DefaultValue("last_update");
			Assert.Equal(typeof(DateTime), value.GetType());
		}


		[Theory]
		[MemberData(nameof(ProviderNames))]
		public void IsValid_FilmIDCheck(string ProviderName)
		{
			dynamic film = new Film(ProviderName);
			var toValidate = film.Find(film_id: 72);
			// is invalid
			Assert.False(film.IsValid(toValidate));
			Assert.Equal(1, film.Errors.Count);

			toValidate = film.Find(film_id: 2);
			// is valid
			Assert.True(film.IsValid(toValidate));
			Assert.Equal(0, film.Errors.Count);
		}


		[Theory]
		[MemberData(nameof(ProviderNames))]
		public void PrimaryKey_Read_Check(string ProviderName)
		{
			dynamic film = new Film(ProviderName);
			var toValidate = film.Find(film_id: 45);

			Assert.True(film.HasPrimaryKey(toValidate));

			var pkValue = film.GetPrimaryKey(toValidate);
			Assert.Equal(45, pkValue);
		}
	}
}
