using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using Xunit;
using Massive.Tests.Sqlite.TableClasses;
#if !COREFX
using SD.Tools.OrmProfiler.Interceptor;
#endif

namespace Massive.Tests.Sqlite
{
	/// <summary>
	/// Specific tests for code which is specific to Sqlite. This means there are fewer tests than for SQL Server, as logic that's covered there already doesn't have to be
	/// retested again here, as the tests are meant to see whether a feature works. Tests are designed to touch the code in Massive.Sqlite. 
	/// </summary>
	/// <remarks>Tests use the Chinook example DB (https://chinookdatabase.codeplex.com/releases/view/55681), autonumber variant. 
	/// Writes are done on Playlist, reads on other tables.</remarks>
	public class ReadWriteTests : IDisposable
    {
		private readonly string OrmProfilerApplicationName = "Sqlite Read/Write tests";

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

			// delete all rows with ProductName 'Massive Product'. 
			var playlists = new Playlist();
			playlists.Delete(null, "Name=@0", "MassivePlaylist");
		}


		[Fact]
		public void Guid_Arg()
		{
			var db = new DynamicModel(TestConstants.ReadWriteTestConnection);
			var guid = Guid.NewGuid();
			var command = db.CreateCommand("SELECT @0 AS val", null, guid);
#if COREFX
			// For some reason .NET Core provider doesn't have DbType.Guid support even though .NET Framework provider does
			Assert.Equal(DbType.String, command.Parameters[0].DbType);
#else
			Assert.Equal(DbType.Guid, command.Parameters[0].DbType);
#endif
			var item = db.Query(command).FirstOrDefault();
			// The output from the provider is a bunch of bytes either way, so we stick with the provider
			// default here (especially since it is the same in both cases).
			Assert.Equal(typeof(byte[]), item.val.GetType());
			Assert.Equal(guid, new Guid(item.val));
		}


		[Fact]
		public void All_NoParameters()
		{
			var albums = new Album();
			var allRows = albums.All().ToList();
			Assert.Equal(347, allRows.Count);
			foreach(var a in allRows)
			{
				Console.WriteLine("{0} {1}", a.AlbumId, a.Title);
			}
		}

		[Fact]
		public void All_LimitSpecification()
		{
			var albums = new Album();
			var allRows = albums.All(limit: 10).ToList();
			Assert.Equal(10, allRows.Count);
		}


		[Fact]
		public void All_WhereSpecification_OrderBySpecification()
		{
			var albums = new Album();
			var allRows = albums.All(orderBy: "Title DESC", where: "WHERE ArtistId=@0", args: 90).ToList();
			Assert.Equal(21, allRows.Count);
			string previous = string.Empty;
			foreach(var r in allRows)
			{
				string current = r.Title;
				Assert.True(string.IsNullOrEmpty(previous) || string.Compare(previous, current) > 0);
				previous = current;
			}
		}


		[Fact]
		public void All_WhereSpecification_OrderBySpecification_LimitSpecification()
		{
			var albums = new Album();
			var allRows = albums.All(limit: 6, orderBy: "Title DESC", where: "ArtistId=@0", args: 90).ToList();
			Assert.Equal(6, allRows.Count);
			string previous = string.Empty;
			foreach(var r in allRows)
			{
				string current = r.Title;
				Assert.True(string.IsNullOrEmpty(previous) || string.Compare(previous, current) > 0);
				previous = current;
			}
		}


		[Fact]
		public void Paged_NoSpecification()
		{
			var albums = new Album();
			// no order by, so in theory this is useless. It will order on PK though
			var page2 = albums.Paged(currentPage: 3, pageSize: 13);
			var pageItems = ((IEnumerable<dynamic>)page2.Items).ToList();
			Assert.Equal(13, pageItems.Count);
			Assert.Equal(27, pageItems[0].AlbumId);
			Assert.Equal(347, page2.TotalRecords);
		}


		[Fact]
		public void Paged_OrderBySpecification()
		{
			var albums = new Album();
			var page2 = albums.Paged(orderBy: "Title DESC", currentPage: 3, pageSize: 13);
			var pageItems = ((IEnumerable<dynamic>)page2.Items).ToList();
			Assert.Equal(13, pageItems.Count);
			Assert.Equal(174, pageItems[0].AlbumId);
			Assert.Equal(347, page2.TotalRecords);
		}


		[Fact]
		public void Insert_SingleRow()
		{
			var playlists = new Playlist();
			var inserted = playlists.Insert(new { Name = "MassivePlaylist" });
			Assert.True(inserted.PlaylistId > 0);
		}
    }
}
