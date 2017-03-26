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
	public class WriteTests : IDisposable
    {
		private readonly string OrmProfilerApplicationName = "Massive SqlServer write tests";

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

			var db = new DynamicModel(TestConstants.WriteTestConnection);
			db.ExecuteAsProcedure("pr_clearAll");
		}


		[Fact]
		public void Insert_SingleRow()
		{
			var categories = new Category();
			var inserted = categories.Insert(new {CategoryName = "Cool stuff", Description = "You know... cool stuff! Cool. n. stuff."});
			int insertedCategoryID = inserted.CategoryID;
			Assert.True(insertedCategoryID > 0);
		}


		[Fact]
		public void Insert_MultipleRows()
		{
			var categories = new Category();
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


		[Fact]
		public void Update_SingleRow()
		{
			dynamic categories = new Category();
			// insert something to update first. 
			var inserted = categories.Insert(new { CategoryName = "Cool stuff", Description = "You know... cool stuff! Cool. n. stuff." });
			int insertedCategoryID = inserted.CategoryID;
			Assert.True(insertedCategoryID > 0);
			// update it, with a better description
			inserted.Description = "This is all jolly marvellous";
			Assert.Equal(1, categories.Update(inserted, inserted.CategoryID)); // Update should have affected 1 row
			var updatedRow = categories.Find(CategoryID: inserted.CategoryID);
			Assert.NotNull(updatedRow);
			Assert.Equal(inserted.CategoryID, updatedRow.CategoryID);
			Assert.Equal(inserted.Description, updatedRow.Description);
			// reset description to NULL
			updatedRow.Description = null;
			Assert.Equal(1, categories.Update(updatedRow, updatedRow.CategoryID)); // Update should have affected 1 row
			var newUpdatedRow = categories.Find(CategoryID: updatedRow.CategoryID);
			Assert.NotNull(newUpdatedRow);
			Assert.Equal(updatedRow.CategoryID, newUpdatedRow.CategoryID);
			Assert.Equal(updatedRow.Description, newUpdatedRow.Description);
		}


		[Fact]
		public void Update_MultipleRows()
		{
			// first insert 2 categories and 4 products, one for each category
			var categories = new Category();
			var insertedCategory1 = categories.Insert(new {CategoryName = "Category 1", Description = "Cat 1 desc"});
			int category1ID = insertedCategory1.CategoryID;
			Assert.True(category1ID > 0);
			var insertedCategory2 = categories.Insert(new { CategoryName = "Category 2", Description = "Cat 2 desc" });
			int category2ID = insertedCategory2.CategoryID;
			Assert.True(category2ID > 0);

			var products = new Product();
			for(int i = 0; i < 4; i++)
			{
				var category = i % 2 == 0 ? insertedCategory1 : insertedCategory2;
				var p = products.Insert(new {ProductName = "Prod" + i, CategoryID = category.CategoryID});
				Assert.True(p.ProductID > 0);
			}
			var allCat1Products = products.All(where:"WHERE CategoryID=@0", args:category1ID).ToArray();
			Assert.Equal(2, allCat1Products.Length);
			foreach(var p in allCat1Products)
			{
				Assert.Equal(category1ID, p.CategoryID);
				p.CategoryID = category2ID;
			}
			Assert.Equal(2, products.Save(allCat1Products));
		}


		[Fact]
		public void Delete_SingleRow()
		{
			// first insert 2 categories
			var categories = new Category();
			var insertedCategory1 = categories.Insert(new { CategoryName = "Cat Delete_SR", Description = "cat 1 desc" });
			int category1ID = insertedCategory1.CategoryID;
			Assert.True(category1ID > 0);
			var insertedCategory2 = categories.Insert(new { CategoryName = "Cat Delete_SR", Description = "cat 2 desc" });
			int category2ID = insertedCategory2.CategoryID;
			Assert.True(category2ID > 0);

			Assert.Equal(1, categories.Delete(category1ID)); // Delete should affect 1 row
			var categoriesFromDB = categories.All(where:"CategoryName=@0", args:(string)insertedCategory2.CategoryName).ToList();
			Assert.Equal(1, categoriesFromDB.Count);
			Assert.Equal(category2ID, categoriesFromDB[0].CategoryID);
		}


		[Fact]
		public void Delete_MultiRow()
		{
			// first insert 2 categories
			var categories = new Category();
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
