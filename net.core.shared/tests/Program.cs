using System;
using System.Reflection;
using System.Data.Common;
using Npgsql;
using System.Runtime.Loader;
using MySql.Data.MySqlClient;
using Microsoft.Data.Sqlite;

namespace MassiveTests
{
	class Program
	{
		static void XMain(string[] args)
		{
			var s = Microsoft.Data.Sqlite.SqliteFactory.Instance;

			var w = MySql.Data.MySqlClient.MySqlClientFactory.Instance;

			string factoryClassName = "Npgsql.NpgsqlFactory";
			//Activator.CreateInstance("Npgsql", "NpgsqlFactory");
			Assembly a = Assembly.GetEntryAssembly();

			AssemblyName an = new AssemblyName("Npgsql");
			Assembly b = Assembly.Load(an);

			var foo = typeof(NpgsqlFactory).GetTypeInfo().Assembly.GetType("Npgsql.NpgsqlFactory");

			Type typea = Type.GetType(typeof(NpgsqlFactory).FullName);
			Type type = typeof(NpgsqlFactory);
			//DbProviderFactory faz = (DbProviderFactory)p.GetValue(null);
			if(type == null)
			{
				throw new InvalidOperationException("Provider factory '" + factoryClassName + "' is not installed, you must include a reference to this ADO.NET provider in your project.");
			}

			// okay this bit works now, at least
			foreach(var f in type.GetFields())
			{
				if(f.Name == "Instance")
				{
					DbProviderFactory baz = (DbProviderFactory)f.GetValue(null);
				}
			}
		}

		//public static Type GetType(string typeName)
		//{
		//	System.Reflection.AssemblyName
		//	AssemblyLoadContext.Default.LoadFromAssemblyName("Npgsql").GetType("NpgsqlFactorty");
		//	var type = Type.GetType(typeName);
		//	if(type != null) return type;
		//	foreach(var a in AppDomain.CurrentDomain.GetAssemblies())
		//	{
		//		type = a.GetType(typeName);
		//		if(type != null)
		//			return type;
		//	}
		//	return null;
		//}
	}
}