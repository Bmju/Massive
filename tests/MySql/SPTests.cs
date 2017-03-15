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
			var result = db.ExecuteAsProcedure("rewards_report_for_date", inParams: new { min_monthly_purchases = 3, min_dollar_amount_purchased = 20, report_date = new DateTime(2005, 5, 1) }, outParams: new { count_rewardees = 0 });
			Assert.AreEqual(27, result.count_rewardees);
		}


		/// <remarks>
		/// What Devart is doing with the the type here is unexpected, but for now we just document it.
		/// Although SByte looks surprising in MySQL, it maps directly to TINYINT (as opposed to TINYINT UNSIGNED) and is a plausible thing to map to.
		/// </remarks>
		[Test]
		public void Function_Call_Bool()
		{
			var db = new SPTestsDatabase(ProviderName);
			if(ProviderName == "Devart.Data.MySql")
			{
				var cmd = db.CreateCommandWithParams("inventory_in_stock", inParams: new { p_inventory_id = 5 }, returnParams: new { retval = false }, isProcedure: true);
				Assert.AreEqual(DbType.Int64, cmd.Parameters["retval"].DbType);
				db.Execute(cmd);
				var result = cmd.ResultsAsExpando();
				Assert.AreEqual((byte)1, (object)result.retval);
			}
			else
			{
				var cmd = db.CreateCommandWithParams("inventory_in_stock", inParams: new { p_inventory_id = 5 }, returnParams: new { retval = false }, isProcedure: true);
				Assert.AreEqual(DbType.SByte, cmd.Parameters["@retval"].DbType);
				db.Execute(cmd);
				var result = cmd.ResultsAsExpando();
				Assert.AreEqual((byte)1, (object)result.retval);
			}
		}


		/// <remarks>
		/// What Devart is doing with the the type here is unexpected, but for now we just document it.
		/// </remarks>
		[Test]
		public void Function_Call_Byte()
		{
			var db = new SPTestsDatabase(ProviderName);
			if(ProviderName == "Devart.Data.MySql")
			{
				var cmd = db.CreateCommandWithParams("inventory_in_stock", inParams: new { p_inventory_id = 5 }, returnParams: new { retval = (byte)1 }, isProcedure: true);
				Assert.AreEqual(DbType.Int16, cmd.Parameters["retval"].DbType);
				db.Execute(cmd);
				var result = cmd.ResultsAsExpando();
				Assert.AreEqual((short)1, (object)result.retval);
			}
			else
			{
				var cmd = db.CreateCommandWithParams("inventory_in_stock", inParams: new { p_inventory_id = 5 }, returnParams: new { retval = (byte)1 }, isProcedure: true);
				Assert.AreEqual(DbType.Byte, cmd.Parameters["@retval"].DbType);
				db.Execute(cmd);
				var result = cmd.ResultsAsExpando();
				Assert.AreEqual((byte)1, (object)result.retval);
			}
		}


		/// <remarks>
		/// What Devart is doing with the the type here is unexpected, but for now we just document it.
		/// </remarks>
		[Test]
		public void Function_Call_SByte()
		{
			var db = new SPTestsDatabase(ProviderName);
			if(ProviderName == "Devart.Data.MySql")
			{
				var cmd = db.CreateCommandWithParams("inventory_in_stock", inParams: new { p_inventory_id = 5 }, returnParams: new { retval = (sbyte)1 }, isProcedure: true);
				Assert.AreEqual(DbType.Int16, cmd.Parameters["retval"].DbType);
				db.Execute(cmd);
				var result = cmd.ResultsAsExpando();
				Assert.AreEqual((short)1, (object)result.retval);
			}
			else
			{
				var cmd = db.CreateCommandWithParams("inventory_in_stock", inParams: new { p_inventory_id = 5 }, returnParams: new { retval = (sbyte)1 }, isProcedure: true);
				Assert.AreEqual(DbType.SByte, cmd.Parameters["@retval"].DbType);
				db.Execute(cmd);
				var result = cmd.ResultsAsExpando();
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


		/// <summary>
		/// Now we can ask Massive to read the query results AND get the param values. Cool.
		/// 
		/// Because of yield return execution, results are definitely not available until at least one item has been read back.
		/// Becasue of the ADO.NET driver, results may not be available until all of the values have been read back (REF).
		/// </summary>
		[Test]
		public void Procedure_Call_Query_Plus_Results()
		{
			var db = new SPTestsDatabase(ProviderName);

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

			var resultset = db.Query(command);

			// read the result set
			int count = 0;
			foreach(var item in resultset)
			{
				count++;
				Assert.AreEqual(typeof(string), item.last_name.GetType());
				Assert.AreEqual(typeof(DateTime), item.create_date.GetType());
			}

			var results = command.ResultsAsExpando();

			Assert.Greater(results.count_rewardees, 0);
			Assert.AreEqual(count, results.count_rewardees);
		}


		// Massive style calls to some examples from https://www.devart.com/dotconnect/mysql/docs/Parameters.html#inoutparams
		#region Devart Examples
		[Test]
		public void In_Out_Params_SQL()
		{
			var _providerName = ProviderName;
			if (ProviderName == "MySql.Data.MySqlClient")
			{
				_providerName += ", AllowUserVariables=true";
			}
			var db = new SPTestsDatabase(_providerName);
			// old skool SQL
			// this approach only works on the Oracle/MySQL driver if "AllowUserVariables=true" is included in the connection string
			var result = db.Scalar("CALL testproc_in_out(10, @param2); SELECT @param2");
			Assert.AreEqual(20, result);
		}


		[Test]
		public void In_Out_Params_SP()
		{
			var db = new SPTestsDatabase(ProviderName);
			// new skool
			var result = db.ExecuteAsProcedure("testproc_in_out", inParams: new { param1 = 10 }, outParams: new { param2 = 0 });
			Assert.AreEqual(20, result.param2);
		}


		[Test]
		public void InOut_Param_SP()
		{
			var db = new SPTestsDatabase(ProviderName);
			var result = db.ExecuteAsProcedure("testproc_inout", ioParams: new { param1 = 10 });
			Assert.AreEqual(20, result.param1);
		}
		#endregion
	}
}
