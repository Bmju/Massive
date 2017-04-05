## ** Disclaimer **
This is a beta version fork of https://github.com/FransBouma/Massive ; the changes in here work, and run, with extensive tests, and you're welcome to take it and use it - but a lot of it's not in the main branch (even though *some* is).

Everything in this README about 'Version 3 of Massive' is about this fork, and it's 100% NOT an official version or release of Massive. In fact, the whole 'Version 3' thing needs rethinking, sooner rather than later.

For now, it's more just me playing around with what I'd like Massive to be.

Again though, it works, it's got loads of tests, and you can drop it into an existing Massive codebase and it will do exactly what Massive used to do!

But it's not Massive. You have been warned.


# Welcome to Massive! A small, happy MicroORM (and general purpose .NET data access wrapper) which will love you forever!

## New in 3.0

* .NET Core :-)
* MySQL (already back-ported to v2.0)
* Stored procedures
* Parameter names and directions (only when you need them, old-style Massive calls still work just as before!)
* Cursors (only on Oracle and PostgreSQL - they're not needed elsewhere)
* Multiple result sets
* Simultaneous access to more then one database provider, if you need it

Plus... it's still 100% compatible with Massive! If you already use Massive then your existing code will compile and run just as before. .NET Core support is the only thing here that required actually changing the API (rather than just carefully extending it), and even then the changes are almost entirely non-breaking for existing users (unless you've ever called DbParameter.AddParam(value) directly yourself... but you haven't, because that's exactly what Massive does for you ;) ).

## What is Massive?

### Massive is a MicroORM

Let's say we have a table named "Products". You create a class like this:

````csharp
public class Products:DynamicModel {
	//you don't have to specify the connection name - if not, Massive will use the first one it finds in your config
	public Products():base("northwind", "products","productid") {}
}
````

You could also just instantiate it inline, as needed:

````csharp
var table = new DynamicModel("northwind", tableName:"Products", primaryKeyField:"ProductID");
````

Now you can query thus:

````csharp
var table = new Products();
//grab all the products
var products = table.All();
foreach (var product in products)
{
	Console.WriteLine(product.ProductNumber + ": " + product.Name);
}
//just grab from category 4. This uses named parameters
var productsFour = table.All(columns: "ProductName as Name", where: "WHERE categoryID=@0",args: 4);
````

What's in each `product` object above is dynamic: meaning that it's malleable and exciting. It takes the shape of whatever is required for your query. This is one of the key features of Massive, it's all done using C#4's dynamic objects - which are very flexible and which you can think of as a little bit like JavaScript objects, if you know those.

If you want to have a paged result set - you can:

```csharp
var result = table.Paged(where: "UnitPrice > 20", currentPage:2, pageSize: 20);
```

In this example, ALL of the arguments are optional and default to reasonable values. CurrentPage defaults to 1, pageSize defaults to 20, where defaults to nothing.

What you get back is a Dynamic with three properties: Items, TotalPages and TotalRecords. Items is a result set which is lazily evaluated and which you can enumerate as in the other examples here. TotalPages is the total number of pages in the complete result set and TotalRecords is the total number of records in the result set.

### And it's a lovely ADO.NET data wrapper

You can run ad-hoc queries as needed:

````csharp
var results = tbl.Query("SELECT * FROM Categories");
foreach (var item in results)
{
	Console.WriteLine(item.Name)
}
````

This will pull categories and enumerate the results - Massive streams all of its result sets as opposed to bulk-fetching them (thanks to Jeroen Haegebaert for that code). Since most databases stream their results back in most use cases, and since essentially all ADO.NET providers stream their results, this means that Massive can safely be used for streaming very large data sets, should you need to do so.

You can call stored procedures, with named input, output, input-output and return parameters:

````csharp
var db = new DynamicModel();
// w := w + 2; v := w - 1; x := w + 1
dynamic testResult = db.ExecuteAsProcedure("TestVars", ioParams: new { w = 2 }, outParams: new { v = 0, x = 0 });
Console.WriteLine("v = " + testResult.v);
Console.WriteLine("w = " + testResult.w);
Console.WriteLine("x = " + testResult.x);
````

You can also use named, directional paramaters against arbitary SQL. Here's a simple example of reading back an output parameter, Oracle-style:

````csharp
var db = new DyanmicModel;
dynamic result = db.ExecuteWithParams("BEGIN :a := 1; END;", outParams: new { a = 0 });
Console.WriteLine("a = " + result.a);
````

If you're used to playing around with `DbConnection`, `DbCommand` and `DbParameter` then you'll see that all this is all ... quite a lot nicer. ;) (The above are complete code samples.) The new Massive 3 API really is almost as powerful as using the ADO.NET classes directly: there is very little you can do directly with ADO.NET that you can't do (much more happily!) via Massive.

Interested? Then dive in!

## A Brief History of Massive

Massive v1.0 was a genius MicroORM - and the first ever MicroORM - written by @RobConery (with a few other genius bits and pieces added by friends and open-source collaborators). The first push to GitHub was on February 15th, 2011.

For Massive v2.0, @FransBouma [officially took over](https://twitter.com/robconery/status/573139252487323648) the project on March 4th, 2015. He greatly cleaned up the multiple-database support, added aysnc versions of everything, and has generally taken (and is still taking) excellent care of the codebase.

Massive v3.0 is currently a fork, with all the new code written by @MikeBeaton. A lot of the details of how I've implemented it (most especially, making all the changes absolutely as minimally breaking as possible for existing users) are the result of very, very useful feedback from @FransBouma (you can look at it if you want, it's all in the [Issues]() discussions in v2.0!).

## More Code Please

### Inserts and Updates

Massive is built on top of dynamics - so if you send an object to a table, it will get parsed into a query. If that object has a property on it that matches the primary key, Massive will think you want to update something. Unless you tell it specifically to update it.

You can send just about anything into the MassiveTransmoQueryfier and it will magically get turned into SQL:

```csharp
var table = new Products();
var poopy = new {ProductName = "Chicken Fingers"};
//update Product with ProductID = 12 to have a ProductName of "Chicken Fingers"
table.Update(poopy, 12);
```

This also works if you have a form on your web page with the name "ProductName" - then you submit it:

```csharp
var table = new Products();
//update Product with ProductID = 12 to have a ProductName of whatever was submitted via the form
table.Update(poopy, Request.Form);
```

Insert works the same way:

```csharp
//pretend we have a class like Products but it's called Categories
var table = new Categories();
//do it up - the inserted object will be returned from the query as expando 
var inserted = table.Insert(new {CategoryName = "Buck Fify Stuff", Description = "Things I like"});
// the new PK value is in the field specified as PrimaryKeyField in the constructor of Categories. 
var newID = inserted.CategoryID;
```

Yippee! Now we get to the fun part - and one of the reasons @RobConery had to spend 150 more lines of code on something you probably won't care about. What happens when we send a whole bunch of goodies to the database at once!

```csharp
var table = new Products();
var drinks = table.All("WHERE CategoryID = @0", args: 8);
//what we get back here is an IEnumerable < ExpandoObject > - we can go to town
foreach(var item in drinks.ToArray()){
	//turn them into Haack Snacks
	item.CategoryID = 12;
}
//Let's update these in bulk, in a transaction shall we?
table.Save(drinks.ToArray());
```

### Named Argument Query Syntax

Since early in the original version, Massive has had the ability to run more friendly queries using Named Arguments and C#4's `DynamicObject.TryInvokeMember` method-on-the-fly syntax. In the earliest version this was trying to be like Rails ActiveRecord (so, calls were like `var drinks = table.FindBy_CategoryID(8);`), but Conery figured "C# is NOT Ruby, and Named Arguments can be a lot more clear", so now calls look like `var drinks = table.FindBy(CategoryID:8);` (examples below).

If your needs are more complicated - just pass in your own SQL with Query().

```csharp
//important - must be dynamic
dynamic table = new Products();

var drinks = table.FindBy(CategoryID:8);
//what we get back here is an IEnumerable < ExpandoObject > - we can go to town
foreach(var item in drinks){
	Console.WriteLine(item.ProductName);
}
//returns the first item in the DB for category 8
var first = table.First(CategoryID:8);

//you dig it - the last as sorted by PK
var last = table.Last(CategoryID:8);

//you can order by whatever you like
var firstButReallyLast = table.First(CategoryID:8,OrderBy:"PK DESC");

//only want one column?
var price = table.First(CategoryID:8,Columns:"UnitPrice").UnitPrice;

//Multiple Criteria?
var items = table.Find(CategoryID:5, UnitPrice:100, OrderBy:"UnitPrice DESC");
```

### Aggregates with Named Arguments

You can do the same thing as above for aggregates:

```csharp
var sum = table.Sum(columns:"Price", CategoryID:5);
var avg = table.Avg(columns:"Price", CategoryID:3);
var min = table.Min(columns:"ID");
var max = table.Max(columns:"CreatedOn");
var count = table.Count();
```

### Metadata
If you find that you need to know information about your table - to generate some lovely things like ... whatever - just ask for the Schema property. This will query INFORMATION_SCHEMA for you, and you can take a look at DATA_TYPE, DEFAULT_VALUE, etc for whatever system you're running on.

In addition, if you want to generate an empty instance of a column - you can now ask for a "Prototype()" - which will return all the columns in your table with the defaults set for you (getdate(), raw values, newid(), etc).

### Factory Constructor

One thing that can be useful is to use Massive to just run a quick query. You can do that now by using "Open()" which is a static builder on DynamicModel:

```csharp
var db = Massive.DynamicModel.Open("myConnectionStringName");
```

You can execute whatever you like at that point.

### Validations

One thing that's always needed when working with data is the ability to stop execution if something isn't right. Massive now has Validations, which are built with the Rails approach in mind:

```csharp
public class Productions:DynamicModel {
	public Productions():base("MyConnectionString","Productions","ID") {}
	public override void Validate(dynamic item) {
		ValidatesPresenceOf("Title");
		ValidatesNumericalityOf(item.Price);
		ValidateIsCurrency(item.Price);
		if (item.Price <= 0)
			Errors.Add("Price can't be negative");
	}
}
```

The idea here is that `Validate()` is called prior to Insert/Update. If it fails, an Error collection is populated and an InvalidOperationException is thrown. That simple. With each of the validations above, a message can be passed in.

### CallBacks

Need something to happen after Update/Insert/Delete? Need to halt before save? Massive has callbacks to let you do just that:

```csharp
public class Customers:DynamicModel {
	public Customers():base("MyConnectionString","Customers","ID") {}

	//Add the person to Highrise CRM when they're added to the system...
	public override void Inserted(dynamic item) {
		//send them to Highrise
		var svc = new HighRiseApi();
		svc.AddPerson(...);
	}
}
```

The callbacks you can use are:

 * Inserted
 * Updated
 * Deleted
 * BeforeDelete
 * BeforeSave




### Stored Procedure Support

Massive now fully supports stored procedures.

You may have read on Stack Overflow *et al* that in earlier versions Massive supported stored procedures already... but it kind of didn't *really*. Yes, you could call a stored procedure with input parameters only, on at least some of the supported databases, using a hand-crafted EXECUTE query, and then remembering to call FirstOrDefault() on the result of that query (even if there weren't any results, otherwise nothing would actually happen). Which is not quite the same.

Now Massive really, fully, supports stored procedures, functions, and named and directional parameters.

For example:

````csharp
var db = new DynamicModel();
dynamic squareResult = db.ExecuteAsProcedure("square_num", ioParams: new { x = 4 });
Assert.Equal(16, squareResult.x);
````

Or:

````csharp
var db = new DynamicModel();
dynamic result = db.ExecuteAsProcedure("find_max", inParams: new { x = 6, y = 7 }, returnParams: new { z = 0 });
Console.WriteLine(result.z);
````

Or, on Oracle, with a table-valued function:

````csharp
var record = db.QueryWithParams("SELECT * FROM table(GET_EMP(:p_EMPNO))", new { p_EMPNO = 7782 }).FirstOrDefault();
Console.WriteLine("EMPNO: " + record.EMPNO + ", ENAME: " + record.ENAME);
````

To use a typed return parameter just pass in a dummy value:

````csharp
var db = new DynamicModel();
dynamic dateResult = db.ExecuteAsProcedure("get_date", returnParams: new { d = DateTime.MinValue});
Console.WriteLine(dateResult.d);
````

Massive infers the database types of all of its parameters from the objects passed in. But if you need to pass in typed, null parameters in Massive 3 you can. Just pass in an object with one or more typed, null properties. For example:

````csharp
private class dateNullParam
{
	public DateTime? d { get; set; }
}

...

var db = new DynamicModel();
dynamic dateResult = db.ExecuteAsProcedure("get_date", returnParams: new dateNullParam());
Console.WriteLine(dateResult.d);
````



### Multiple Result Sets

Massive now also allows you to query multiple result sets, as in this example:

````csharp
var db = new DynamicModel();
var resultSets = db.QueryMultiple("select 1 as a, 2 as b; select 3 as c, 4 as d;");
int set = 0;
foreach (var resultSet in resultSets)
{
	foreach (var item in resultSet)
	{
		if (set == 0) Console.WriteLine("a = " + item.a + ", b = " + item.b);
		else Console.WriteLine("c = " + item.c + ", d = " + item.d);
	}
	set++;
}
````

Warning: the pattern for traversing multiple result sets from `QueryMultiple()` in Massive compiles and runs against the output from `Query()` and vice versa (though obviously the results are not what you expect...). This is because Massive achieves all its juicy loveliness using dynamic objects (i.e. with relatively weak, runtime only type support). This can be confusing if you inadverently use the wrong reader pattern, for instance if you realise you need to switch from one to the other but forget to change the reader loop. You have been warned!

### Putting It All Together

In the relatively unlikely (but certainly not impossible) case that you want to read back a query (even a query with multiple result sets) from a stored procedure *and* access the result of any output or return parameters using Massive... you can!

````csharp
var db = new DynamicModel();
// in this case only we must create the command separately from the query
var command = db.CreateCommandWithParams("rewards_report_for_date",
					 inParams: new
					 {
						min_monthly_purchases = 3,
						min_dollar_amount_purchased = 20,
						report_date = new DateTime(2005, 5, 1)
					 },
					 outParams: new
					 {
						count_rewardees = 0
					 },
					 isProcedure: true);
// get ready for the query (remember, delayed execution...)
var resultset = db.Query(command);
// read back the result set
foreach (var item in resultset)
{
	Console.WriteLine(item.first_name + " " + item.last_name + ", " + item.create_date);
}
// get the return parameter values
// (it is a restriction of ADO.NET (not Massive) that the return parameters are not ready until here)
var results = db.ResultsAsExpando(command);
Console.WriteLine(results.count_rewardees);
````


## Cursor Support

Cursors are fully supported on Oracle and PostgreSQL.

Don't worry, you don't want or need cursor support on MySQL or SQL Server because you can effortlessly return result sets from stored procedures on those providers without using cursors: just put a non-bound SELECT into your procedure. This is why the underlying ADO.NET providers don't support cursors on those providers, which is why Massive doesn't! SQLite itself doesn't have cursors, or stored procedures, at all.

### Automatic Cursor Derefencing

If you use Massive to query a procedure which returns cursors, then you end up querying the result sets referred to by the cursors instead. This is on purpose, and useful!

````csharp
var db = new DynamicModel();
// Oracle function with a single cursor return value
var employees = db.QueryFromProcedure("get_dept_emps", inParams: new { p_DeptNo = 10 }, returnParams: new { v_rc = new Cursor() });
// When we execute a query, we read back the result set which the cursor points to
foreach(var employee in employees)
{
	Console.WriteLine(employee.EMPNO + " " + employee.ENAME);
}
````

If you need to get at the cursors themselves you use `.Execute` instead of `.Query`, as in the examples in the next section.

The Oracle ADO.NET provides this style of cursor dereferencing automatically.

Because we're nice, we've added full support for the same style of dereferencing on PostgreSQL to Massive itself! This used to be partially supported by version 2 of the Npgsql provider for PostgreSQL (and the Massive dereferencing support started from that removed code, but is now complete). But it currently [isn't supported at all](https://github.com/npgsql/npgsql/issues/438) in version 3 of Npgsql. Npgsql dereferencing support in Massive is completely transparent to you as an end-user (as in the code above; and except for the two configuration settings below), it just makes your life easier. We're looking at submitting this code back to the Npgsql project (where it probably really belongs).

Each instance of DynamicModel features two properties which are there to configure cursor dereferencing for PostgreSQL only:

* `NpgsqlAutoDereferenceCursors` with default value `true`
* `NpgsqlAutoDereferenceFetchSize` with default value `10000` (which is probably good for small projects accessing large cursors, but which should be reduced for large projects with multiple concurrent cursor accesses). If you're not using PostgreSQL, or if you're not using cursors on PostgreSQL, then you don't need to worry about this setting at all. If you are, and if you're writing code which is going to have lots of concurrent users, then you do!

You should be aware that cursors in PostgreSQL itself are NOT very efficient. This is a limitation of PostgreSQL, not of Massive or Npgsql. For small or medium result sets there are much faster ways you can read back your data. [[ref]](http://stackoverflow.com/a/42301318/795690) [[ref]](https://github.com/npgsql/npgsql/issues/438)

### Manual Cursor Dereferencing And Input Cursor Parameters

You can also work with cursors more explicitly.

On PostgreSQL you have to wrap explicit cursor code in a transaction:

````csharp
int FetchSize = 2000;
var db = new DynamicModel();
using(var conn = db.OpenConnection())
{
	// cursors in PostgreSQL must share a transaction (not just a connection, as in Oracle)
	using(var trans = conn.BeginTransaction())
	{
		var result = db.ExecuteAsProcedure("fn_returns_cursor", returnParams: new { cname = new Cursor() }, connection: conn);
		while(true)
		{
			var resultBatch = db.QueryWithParams($@"FETCH {FetchSize} FROM ""{result.cname}""", connection: conn);
			foreach(var item in resultBatch)
			{
				Console.WriteLine(item.id);
			}
		}
		db.Execute($@"CLOSE ""{result.cname}""", connection: conn);
		trans.Commit();
	}
}
````

All that code for batching the results, closing the cursors, etc. - potentially for multiple cursors - is what the automatic cursor dereferencing code is doing for you... but that's how you manually dereference, if you need to!

On PostgreSql (or in any other case where you need to manage your own transactions when using Massive) you can use `TransactionScope` instead `DbConnection.BeginTransaction())`, but be aware that `TransactionScope` isn't supported on .NET Core.

On Oracle to pass cursors between calls (we are not dereferencing here, just passing a cursor) you have to wrap the calls in a shared connection (unlike PostgreSQL you don't also need a shared transaction):

````csharp
var db = new DynamicModel();
// To share cursors between commands in Oracle the commands must use the same connection
using(var conn = db.OpenConnection())
{
	var res1 = db.ExecuteWithParams("begin open :p_rc for select* from emp where deptno = 10; end;", outParams: new { p_rc = new Cursor() }, connection: conn);
	db.ExecuteAsProcedure("cursor_in_out.process_cursor", inParams: new { p_cursor = res1.p_rc }, connection: conn);
}
````

## .NET Core Support

Basically, Massive now just works on .NET Core.

### Connection Configuration

The main change you will notice is around connection string and provider configuration.

On .NET Framework 4.0+ this still works from `.config` files, exactly as it did before. (So you will normally pass in a connection string *name* to Massive in .NET Framework; though in Massive 3 you can now also pass in the connection string itself with the provider name appended, as for .NET Core, if you want to...)

On .NET Core you pass in the connection string itself, but with one additional property of `ProviderName=` appended to it, and Massive does the rest (by magic...).

In both cases you could override the default behaviour (by passing in an instance of either the backwards-compatible `IConnectionStringProvider` or the new, more capable `ConnectionProvider` to your DynamicModel constructor) if you wanted to - but in normal cases you won't ever need to.

### Compilation

If you're dropping in the source files to compile Massive yourself, you'll need to define the `COREFX` compilation symbol to get the .NET Core version.

### Breaking Changes

See [BreakingChanges.md]() for the short list of breaking changes between Massive v2.0 and Massive 3.

Apart from one issue, `.AddParam()` (which you aren't using, because Massive does it for you! ;) ), there *are* no breaking changes in the .NET Framework version of Massive, and the only 'breaking' changes in the .NET Core version are to do with removing (or replacing where possible) support for features such as `ConfigurationManager` connection configuration, and `DataTable`, which simply aren't present in .NET Core.

## Supported ADO.NET Providers

|ADO.NET Provider Name|.NET Framework 4.0+|.NET Core|
|:-----|:-----|:-----|
|System.Data.SqlClient|YES|YES|
|Oracle.ManagedDataAccess.Client|YES|There is no .NET Core version of this provider yet [[ref]](www.oracle.com/technetwork/topics/dotnet/tech-info/odpnet-dotnet-core-sod-3628981.pdf)|
|Oracle.DataAccess.Client|YES|There will never be a .NET Core version of this provider [[ref]](www.oracle.com/technetwork/topics/dotnet/tech-info/odpnet-dotnet-core-sod-3628981.pdf)|
|Npgsql|YES|YES|
|MySql.Data.MySqlClient|YES|YES (driver at pre-release on NuGet, but passing all tests in Massive)|
|Devart.Data.MySql|YES|There is no .NET Core version of this provider yet|
|System.Data.SQLite|YES|N/A|
|Microsoft.Data.Sqlite|N/A|YES|

### Plugging in an ADO.NET provider which Massive doesn't already know about

By implementing the new, more extensible `ConnectionProvider` abstract class in Massive 3 (which is supported alongside the previous, less capable, `IConnectionStringProvider` interface), you can now plug in any ADO.NET driver you like, as long as it connects to a database which is (or which speaks the same variant of SQL as...) one of the databases which Massive already knows about.

You don't have to deal with any of this if you're using Massive against a database and driver which it already knows about.

## Getting Massive

#### Getting It

To get Massive, you need to download all the `.cs` files from `net.core.shared/src` and put them into a sub-folder called `Massive` in your own project.

That's it, then fire up your project and start using Massive! It compiles and works on .NET and .NET Core; define the `COREFX` compilation symbol to get the .NET Core version.

#### Customising It

You can leave out any of the five database specific files (`Massive.MySql.cs`, `Massive.Oracle.cs`, `Massive.PostgreSql.cs`, `Massive.Sqlite.cs` and `Massive.SqlServer.cs`) which you are not using, so in most cases you will only need one of these five files; but can have more than one (unlike in previous versions of Massive). You can also, optionally, leave out `Massive.Shared.Async.cs` if you don't want the async features of the Massive API. (Note that the new features in Massive 3 have not yet been ported to the async API, though they will be.) The async support in Massive requires .NET 4.5+, everything else works on .NET 4.0+.

Of course you could also be more clever and compile these files into a separate DLL in a separate project first, then link to that, if you wanted to... but soon you won't need to. Read on.

#### The Past

Massive was called Massive because the first version of the code, with most of the MicroORM features already in it, was a 'massive' (not) 400 lines of code - and because that's how @RobConery's mind works. ;)

This is relevant here, because up to and including v2.0 Massive has advertised itself as a project where you download the source code yourself, and if it doesn't do quite what you want... then you just go in and change it. That makes sense for a single file, 400 LOC project. It already arguably made less sense in Massive v2.0, where you had to download two (or three) files and 2,000+ LOC. And it makes even less sense now, where Massive 3 is somewhat larger (though still not huge; and of course even more functional!) again.

You can still download the Massive source files; and as of right now, you still have to. And of course you can change them: it's open source! But realistically, a lot of people using Massive don't want to change it, they just want it to work.

#### One Possible Future 

Downloading source files for most users needs to be a temporary state of affairs. As @FransBouma rightly pointed out a while ago [it's past time](https://github.com/FransBouma/Massive/issues/248) that Massive had automatically generated downloads, and this is next on the to-do list. To be honest, it's past time that Massive was on NuGet given that it now supports .NET Core, and I think this is the way to go. Stay posted.

## Breaking Changes

Massive 3 is compile and link compatible with the current version of Massive v2.0. For existing users of Massive, the following are the only known breaking changes from v2.0 to 3. If you find anything else please, [raise an Issue]().

* AddParam is no longer an extension method of DbParameter

	* Supporting multiple databases at once (is quite nice really and) actually made it much easier to support .NET Core. However `DbParameter.AddParam()` needs to know which database it is running on, and extension methods can only access the properties and methods of the object they are extending. The hacks required to get AddParam to do exactly what it used to do just didn't seem worth it, on what (I think) is a non-core aspect of the API.
	* The exact same functionality is still available, just call `DynamicModel.AddParam(DbParameter, value)` instead of `DbParameter.AddParam(value)`

* Two methods drop out of the Massive API if you are on .NET Core, namely the two variants of `ToDataTable`, because .NET Core does not support `DataTable`
	* Unless you know that you are using either of these then you aren't(!), and if you are then unfortunately you're going to have to be making changes to your codebase for .NET Core anyway.

* If you are moving from .NET to .NET Core then you will also have to change where you put the connection string and provider config settings for Massive: see the .NET Core section of [README.md](README.md).
	* These can no longer be stored in a `.config` file since this isn't supported by .NET Core. (Remember: nothing changes on .NET Framework and your old `.config` files still work with Massive exactly as before.) This is a feature, not a bug!
	* Instead, you just pass in your connection string itself (instead of the connection string name as you would have done before), having first added `ProviderName=<value>` to it (with `<value>` replaced by the provider name which would previously have lived in a separate attribute in your config file; see the list of supported provider names above) and ... everything works!
