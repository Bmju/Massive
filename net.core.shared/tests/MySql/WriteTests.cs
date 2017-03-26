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
	public class WriteTests : IDisposable
	{
		public static IEnumerable<object[]> ProviderNames = new[] {
			new object[] { "MySql.Data.MySqlClient" }
#if !COREFX
		  , new object[] { "Devart.Data.MySql" }
#endif
		};

		private readonly string OrmProfilerApplicationName = "Massive MySql write tests";

		public WriteTests()
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
				var db = new DynamicModel(string.Format(TestConstants.WriteTestConnection, ProviderName[0]));
				db.ExecuteAsProcedure("pr_clearAll");
			}
		}


		[Theory]
		[MemberData(nameof(ProviderNames))]
		public void Insert_SingleRow(string ProviderName)
		{
			var categories = new Category(ProviderName);
			var inserted = categories.Insert(new { CategoryName = "Cool stuff", Description = "You know... cool stuff! Cool. n. stuff." });
			int insertedCategoryID = inserted.CategoryID;
			Assert.True(insertedCategoryID > 0);
		}


		[Theory]
		[MemberData(nameof(ProviderNames))]
		public void Insert_MultipleRows(string ProviderName)
		{
			var categories = new Category(ProviderName);
			var toInsert = new List<dynamic>();
			toInsert.Add(new { CategoryName = "Cat Insert_MR", Description = "cat 1 desc" });
			toInsert.Add(new { CategoryName = "Cat Insert_MR", Description = "cat 2 desc" });
			Assert.Equal(2, categories.SaveAsNew(toInsert.ToArray()));
			var inserted = categories.All(where: "CategoryName=@0", args: (string)toInsert[0].CategoryName).ToList();
			Assert.Equal(2, inserted.Count);
			foreach(var c in inserted)
			{
				Assert.True(c.CategoryID > 0);
				Assert.Equal("Cat Insert_MR", c.CategoryName);
			}
		}


		[Theory]
		[MemberData(nameof(ProviderNames))]
		public void Update_SingleRow(string ProviderName)
		{
			dynamic categories = new Category(ProviderName);
			// insert something to update first. 
			var inserted = categories.Insert(new { CategoryName = "Cool stuff", Description = "You know... cool stuff! Cool. n. stuff." });
			int insertedCategoryID = inserted.CategoryID;
			Assert.True(insertedCategoryID > 0);
			// update it, with a better description
			inserted.Description = "This is all jolly marvellous";
			Assert.Equal(1, categories.Update(inserted, inserted.CategoryID)); // Update should have affected 1 row
			var updatedRow = categories.Find(CategoryID: inserted.CategoryID);
			Assert.NotNull(updatedRow);
			Assert.Equal(inserted.CategoryID, Convert.ToInt32(updatedRow.CategoryID)); // convert from uint
			Assert.Equal(inserted.Description, updatedRow.Description);
			// reset description to NULL
			updatedRow.Description = null;
			Assert.Equal(1, categories.Update(updatedRow, updatedRow.CategoryID)); // Update should have affected 1 row
			var newUpdatedRow = categories.Find(CategoryID: updatedRow.CategoryID);
			Assert.NotNull(newUpdatedRow);
			Assert.Equal(updatedRow.CategoryID, newUpdatedRow.CategoryID);
			Assert.Equal(updatedRow.Description, newUpdatedRow.Description);
		}


		[Theory]
		[MemberData(nameof(ProviderNames))]
		public void Update_MultipleRows(string ProviderName)
		{
			// first insert 2 categories and 4 products, one for each category
			var categories = new Category(ProviderName);
			var insertedCategory1 = categories.Insert(new { CategoryName = "Category 1", Description = "Cat 1 desc" });
			int category1ID = insertedCategory1.CategoryID;
			Assert.True(category1ID > 0);
			var insertedCategory2 = categories.Insert(new { CategoryName = "Category 2", Description = "Cat 2 desc" });
			int category2ID = insertedCategory2.CategoryID;
			Assert.True(category2ID > 0);

			var products = new Product(ProviderName);
			for(int i = 0; i < 4; i++)
			{
				var category = i % 2 == 0 ? insertedCategory1 : insertedCategory2;
				var p = products.Insert(new { ProductName = "Prod" + i, CategoryID = category.CategoryID });
				Assert.True(p.ProductID > 0);
			}
			var allCat1Products = products.All(where: "WHERE CategoryID=@0", args: category1ID).ToArray();
			Assert.Equal(2, allCat1Products.Length);
			foreach(var p in allCat1Products)
			{
				Assert.Equal(category1ID, Convert.ToInt32(p.CategoryID)); // convert from uint
				p.CategoryID = category2ID;
			}
			Assert.Equal(2, products.Save(allCat1Products));
		}


		[Theory]
		[MemberData(nameof(ProviderNames))]
		public void Delete_SingleRow(string ProviderName)
		{
			// first insert 2 categories
			var categories = new Category(ProviderName);
			var insertedCategory1 = categories.Insert(new { CategoryName = "Cat Delete_SR", Description = "cat 1 desc" });
			int category1ID = insertedCategory1.CategoryID;
			Assert.True(category1ID > 0);
			var insertedCategory2 = categories.Insert(new { CategoryName = "Cat Delete_SR", Description = "cat 2 desc" });
			int category2ID = insertedCategory2.CategoryID;
			Assert.True(category2ID > 0);

			Assert.Equal(1, categories.Delete(category1ID)); // Delete should affect 1 row
			var categoriesFromDB = categories.All(where: "CategoryName=@0", args: (string)insertedCategory2.CategoryName).ToList();
			Assert.Equal((long)1, categoriesFromDB.Count);
			Assert.Equal(category2ID, Convert.ToInt32(categoriesFromDB[0].CategoryID)); // convert from uint
		}


		[Theory]
		[MemberData(nameof(ProviderNames))]
		public void Delete_MultiRow(string ProviderName)
		{
			// first insert 2 categories
			var categories = new Category(ProviderName);
			var insertedCategory1 = categories.Insert(new { CategoryName = "Cat Delete_MR", Description = "cat 1 desc" });
			int category1ID = insertedCategory1.CategoryID;
			Assert.True(category1ID > 0);
			var insertedCategory2 = categories.Insert(new { CategoryName = "Cat Delete_MR", Description = "cat 2 desc" });
			int category2ID = insertedCategory2.CategoryID;
			Assert.True(category2ID > 0);

			Assert.Equal(2, categories.Delete(where: "CategoryName=@0", args: (string)insertedCategory1.CategoryName)); // Delete should affect 2 rows
			var categoriesFromDB = categories.All(where: "CategoryName=@0", args: (string)insertedCategory2.CategoryName).ToList();
			Assert.Equal(0, categoriesFromDB.Count);
		}
	}
}
