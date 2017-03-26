#if !COREFX
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
	public class DevartTests : IDisposable
	{
		private readonly string OrmProfilerApplicationName = "Massive MySql Devart provider tests";

		public DevartTests()
		{
			Console.WriteLine("Entering " + OrmProfilerApplicationName);
#if !COREFX
			// The SD Tools wrapper is hiding the Devart DbCommand subclass ParameterCheck property which is tested here
			//InterceptorCore.Initialize(OrmProfilerApplicationName);
#endif
		}

		public void Dispose()
		{
			Console.WriteLine("Exiting " + OrmProfilerApplicationName);
		}


		private string ProviderName = "Devart.Data.MySql";

		// Massive style calls to some examples from https://www.devart.com/dotconnect/mysql/docs/Parameters.html#inoutparams
		#region Devart Examples
		
		/// <remarks>
		/// Demonstrates that this Devart-specific syntax is possible in Massive;
		/// although it pretty much stops looking much like Massive when used like this.
		/// </remarks>
		[Fact]
		public void Devart_ParameterCheck()
		{
			Console.WriteLine("Hello, World");
			var db = new SPTestsDatabase(ProviderName);
			var connection = db.OpenConnection();
			var command = db.CreateCommandWithParams("testproc_in_out", isProcedure: true, connection: connection);
			((dynamic)command).ParameterCheck = true; // dynamic trick to set the underlying property
			command.Prepare(); // makes a round-trip to the database
			command.Parameters["param1"].Value = 10;
			var result = db.ExecuteWithParams(command);
			Assert.Equal(20, result.param2);
		}
		#endregion
	}
}
#endif
