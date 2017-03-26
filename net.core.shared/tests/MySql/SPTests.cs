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
	public class SPTests : IDisposable
	{
		public static IEnumerable<object[]> ProviderNames = new[] {
			new object[] { "MySql.Data.MySqlClient" }
#if !COREFX
		  , new object[] { "Devart.Data.MySql" }
#endif
		};

		private readonly string OrmProfilerApplicationName = "Massive MySql stored procedure tests";

		public SPTests()
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


		[Theory]
		[MemberData(nameof(ProviderNames))]
		public void Procedure_Call(string ProviderName)
		{
			var db = new SPTestsDatabase(ProviderName);
			var result = db.ExecuteAsProcedure("rewards_report_for_date", inParams: new { min_monthly_purchases = 3, min_dollar_amount_purchased = 20, report_date = new DateTime(2005, 5, 1) }, outParams: new { count_rewardees = 0 });
			Assert.Equal(27, result.count_rewardees);
		}


		/// <remarks>
		/// There's some non-trivial work behind the scenes in Massive.MySql.cs to make the two 
		/// providers return a bool when we expect them to.
		/// </remarks>
		[Theory]
		[MemberData(nameof(ProviderNames))]
		public void Function_Call_Bool(string ProviderName)
		{
			var db = new SPTestsDatabase(ProviderName);
			var result = db.ExecuteAsProcedure("inventory_in_stock",
											   inParams: new { p_inventory_id = 5 },
											   returnParams: new { retval = false });
			Assert.Equal(true, result.retval);
		}


		/// <remarks>
		/// Devart doesn't have an unsigned byte type, so has to put 0-255 into a short
		/// </remarks>
		[Theory]
		[MemberData(nameof(ProviderNames))]
		public void Function_Call_Byte(string ProviderName)
		{
			var db = new SPTestsDatabase(ProviderName);
			var result = db.ExecuteAsProcedure("inventory_in_stock",
											   inParams: new { p_inventory_id = 5 },
											   returnParams: new { retval = (byte)1 });
			if(ProviderName == "Devart.Data.MySql")
			{
				Assert.Equal(typeof(short), result.retval.GetType());
			}
			else
			{
				Assert.Equal(typeof(byte), result.retval.GetType());
			}
			Assert.Equal(1, result.retval);
		}


		/// <remarks>
		/// Again there's some non-trivial work behind the scenes in Massive.MySql.cs to make both 
		/// providers return a signed byte when we expect them to.
		/// </remarks>
		[Theory]
		[MemberData(nameof(ProviderNames))]
		public void Function_Call_SByte(string ProviderName)
		{
			var db = new SPTestsDatabase(ProviderName);
			var result = db.ExecuteAsProcedure("inventory_in_stock",
											   inParams: new { p_inventory_id = 5 },
											   returnParams: new { retval = (sbyte)1 });
			Assert.Equal(typeof(sbyte), result.retval.GetType());
			Assert.Equal(1, result.retval);
		}


		/// <summary>
		/// Now we can ask Massive to read the query results AND get the param values. Cool.
		/// 
		/// Because of yield return execution, results are definitely not available until at least one item has been read back.
		/// Becasue of the ADO.NET driver, results may not be available until all of the values have been read back (REF).
		/// </summary>
		[Theory]
		[MemberData(nameof(ProviderNames))]
		public void Procedure_Call_Query_Plus_Results(string ProviderName)
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
				Assert.Equal(typeof(string), item.last_name.GetType());
				Assert.Equal(typeof(DateTime), item.create_date.GetType());
			}

			var results = db.ResultsAsExpando(command);

			Assert.Equal(true, results.count_rewardees > 0);
			Assert.Equal(count, results.count_rewardees);
		}


		// Massive style calls to some examples from https://www.devart.com/dotconnect/mysql/docs/Parameters.html#inoutparams
		#region Devart Examples
		[Theory]
		[MemberData(nameof(ProviderNames))]
		public void In_Out_Params_SQL(string ProviderName)
		{
			var _providerName = ProviderName;
			if(ProviderName == "MySql.Data.MySqlClient")
			{
				// this must be added to access user variables on the Oracle/MySQL driver
				_providerName += ";AllowUserVariables=true";
			}
			var db = new SPTestsDatabase(_providerName);
			// old skool SQL
			// this approach only works on the Oracle/MySQL driver if "AllowUserVariables=true" is included in the connection string
			var result = db.Scalar("CALL testproc_in_out(10, @param2); SELECT @param2");
			Assert.Equal((long)20, result);
		}


		[Theory]
		[MemberData(nameof(ProviderNames))]
		public void In_Out_Params_SP(string ProviderName)
		{
			var db = new SPTestsDatabase(ProviderName);
			// new skool
			var result = db.ExecuteAsProcedure("testproc_in_out", inParams: new { param1 = 10 }, outParams: new { param2 = 0 });
			Assert.Equal(20, result.param2);
		}


		[Theory]
		[MemberData(nameof(ProviderNames))]
		public void InOut_Param_SP(string ProviderName)
		{
			var db = new SPTestsDatabase(ProviderName);
			var result = db.ExecuteAsProcedure("testproc_inout", ioParams: new { param1 = 10 });
			Assert.Equal(20, result.param1);
		}
		#endregion
	}
}
