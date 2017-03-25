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
	[TestFixture("Devart.Data.MySql")]
	public class DevartTests
	{
		private string ProviderName;

		/// <summary>
		/// Initialise tests for given provider
		/// </summary>
		/// <param name="providerName">Provider name</param>
		public DevartTests(string providerName)
		{
			ProviderName = providerName;
		}

		[TestFixtureSetUp]
		public void Setup()
		{
			// The SD Tools wrapper is hiding the Devart command ParameterCheck property which is used in this test
			//InterceptorCore.Initialize("Massive MySql Devart driver tests .NET 4.0");
		}


		// Massive style calls to some examples from https://www.devart.com/dotconnect/mysql/docs/Parameters.html#inoutparams
		#region Devart Examples
		/// <remarks>
		/// Demonstrates that this Devart-specific syntax is possible in Massive;
		/// although it pretty much stops looking much like Massive when used like this.
		/// </remarks>
		[Test]
		public void Devart_ParameterCheck()
		{
			var db = new SPTestsDatabase(ProviderName);
			var connection = db.OpenConnection();
			var command = db.CreateCommandWithParams("testproc_in_out", isProcedure: true, connection: connection);
			((dynamic)command).ParameterCheck = true; // dynamic trick to set the underlying property
			command.Prepare(); // makes a round-trip to the database
			command.Parameters["param1"].Value = 10;
			var result = db.ExecuteWithParams(command);
			Assert.AreEqual(20, result.param2);
		}
		#endregion
	}
}
