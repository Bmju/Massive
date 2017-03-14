using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Dynamic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using Massive.Tests.MySql.TableClasses;
using NUnit.Framework;
using SD.Tools.OrmProfiler.Interceptor;

namespace Massive.Tests.MySql
{
	[TestFixture("MySql.Data.MySqlClient")]
	[TestFixture("Devart.Data.MySql")]
	public class SPTests
	{
		private string ProviderName;

		/// <summary>
		/// Initialise tests for given provider
		/// </summary>
		/// <param name="providerName">Provider name</param>
		public SPTests(string providerName)
		{
			ProviderName = providerName;
		}

		[TestFixtureSetUp]
		public void Setup()
		{
			// These tests are run automatically on both providers (MySql.Data.MySqlClient and Devart.Data.MySql) using
			// two separate projects, which share most files. We cannot change the value of DbProviderFactoryName between
			// the two projects but we don't need to as long as we specify the provider name in the connection strings.
			//InterceptorCore.Initialize("Massive MySql stored procedure tests .NET 4.0");
		}


		[Test]
		public void Procedure_Call()
		{
			var db = new SPTestsDatabase(ProviderName);
			var result = db.ExecuteAsProcedure("rewards_report_for_date", inParams: new { min_monthly_purchases = 3, min_dollar_amount_purchased = 20, report_date = new DateTime(2005, 1, 1) }, returnParams: new { count_rewardees = 0 });
			Assert.AreEqual(27, result.count_rewardees);
		}


		/// <remarks>
		/// Both MySQL drivers create a typed param from a bool value, but the type is not bool, and is different in each case.
		/// All the output casting and almost all the input casting of Massive works by using the default operation of
		/// the various ADO.NET drivers so (for now?) we don't try to fix this, we just document it here.
		/// </remarks>
		[Test]
		public void Function_Call_Bool()
		{
			var db = new SPTestsDatabase(ProviderName);
			if(ProviderName == "Devart.Data.MySql")
			{
				var conn = db.OpenConnection();
				var cmd = db.CreateCommandWithParams("inventory_in_stock", isProcedure: true, connection: conn);
				((dynamic)cmd).ParameterCheck = true;
				((dynamic)cmd).Prepare();
				Assert.AreEqual(DbType.Int64, cmd.Parameters["@retval"].DbType);
				db.ExecuteWithParams(string.Empty, command: cmd, connection: conn);
				var result = cmd.ResultsAsDynamic();
				Assert.AreEqual((long)1, (object)result.retval);
			}
			else
			{
				var cmd = db.CreateCommandWithParams("inventory_in_stock", inParams: new { p_inventory_id = 5 }, returnParams: new { retval = false }, isProcedure: true);
				Assert.AreEqual(DbType.SByte, cmd.Parameters["@retval"].DbType);
				db.Execute(cmd);
				var result = cmd.ResultsAsDynamic();
				Assert.AreEqual((byte)1, (object)result.retval);
			}
		}


		/// <remarks>
		/// What Devart is doing with the the type here is unexpected, but (for now?) we just document it.
		/// </remarks>
		[Test]
		public void Function_Call_Byte()
		{
			var db = new SPTestsDatabase(ProviderName);
			var cmd = db.CreateCommandWithParams("inventory_in_stock", inParams: new { p_inventory_id = 5 }, returnParams: new { retval = (byte)1 }, isProcedure: true);
			if(ProviderName == "Devart.Data.MySql")
			{
				((dynamic)cmd).ParameterCheck = true;
			}
			if(ProviderName == "Devart.Data.MySql")
			{
				Assert.AreEqual(DbType.Int16, cmd.Parameters["@retval"].DbType);
			}
			else
			{
				Assert.AreEqual(DbType.Byte, cmd.Parameters["@retval"].DbType);
			}
			db.Execute(cmd);
			var result = cmd.ResultsAsDynamic();
			if(ProviderName == "Devart.Data.MySql")
			{
				Assert.AreEqual((short)1, (object)result.retval);
			}
			else
			{
				Assert.AreEqual((byte)1, (object)result.retval);
			}
		}


		[Test]
		public void Function_Call_Simple()
		{
			var db = new SPTestsDatabase(ProviderName);
			var result = db.ExecuteAsProcedure("inventory_in_stock", inParams: new { p_inventory_id = 5 }, returnParams: new { retval = false });
			// 1 database casts it as SByte, the other as Int64 (!!)
			Assert.AreEqual((long)1, (long)result.retval);
		}


		[Test]
		public void Procedure_Call_Query_Plus_Results()
		{
			var db = new SPTestsDatabase(ProviderName);

			var inParams = new { min_monthly_purchases = 3, min_dollar_amount_purchased = 20, report_date = new DateTime(2005, 1, 1) };
			var outParams = new { count_rewardees = 0 };
			var command = db.CreateCommandWithParams("rewards_report_for_date", inParams: inParams, outParams: outParams, isProcedure: true);

			var resultset = db.Query(command);

			// read the result set
			int count = 0;
			foreach(var item in resultset)
			{
				count++;
				Assert.AreEqual(typeof(string), item.last_name.GetType());
				Assert.AreEqual(typeof(DateTime), item.create_date.GetType());
			}

			// Now we can ask Massive to read the query results AND get the param values. Cool.
			// Because of yield return execution, results are not available until at least one item has been read back
			var results = command.ResultsAsDynamic();

			Assert.Greater(results.count_rewardees, 0);
			Assert.AreEqual(count, results.count_rewardees);
		}
	}
}
