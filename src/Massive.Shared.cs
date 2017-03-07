///////////////////////////////////////////////////////////////////////////////////////////////////
// Massive v2.0. Main code.
///////////////////////////////////////////////////////////////////////////////////////////////////
// Licensed to you under the New BSD License
// http://www.opensource.org/licenses/bsd-license.php
// Massive is copyright (c) 2009-2016 various contributors.
// All rights reserved.
// See for sourcecode, full history and contributors list: https://github.com/FransBouma/Massive
//
// Redistribution and use in source and binary forms, with or without modification, are permitted 
// provided that the following conditions are met:
//
// - Redistributions of source code must retain the above copyright notice, this list of conditions and the 
//   following disclaimer.
// - Redistributions in binary form must reproduce the above copyright notice, this list of conditions and 
//   the following disclaimer in the documentation and/or other materials provided with the distribution.
// - The names of its contributors may not be used to endorse or promote products derived from this software 
//   without specific prior written permission.
// 
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS 
// OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY 
// AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR 
// CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL 
// DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, 
// DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, 
// WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY 
// WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
///////////////////////////////////////////////////////////////////////////////////////////////////
using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Configuration;
using System.Data;
using System.Data.Common;
using System.Dynamic;
using System.Linq;
using System.Reflection;
using System.Transactions;

namespace Massive
{
	/// <summary>
	/// Class which provides extension methods for various ADO.NET objects.
	/// </summary>
	public static partial class ObjectExtensions
	{
		/// <summary>
		/// Yield return the next result set of this reader
		/// </summary>
		/// <param name="rdr">The reader.</param>
		/// <returns>streaming enumerable with expandos, one for each row read</returns>
		internal static IEnumerable<dynamic> YieldResult(this IDataReader rdr)
		{
			while(rdr.Read())
			{
				yield return rdr.RecordToExpando();
			}
		}


		/// <summary>
		/// Extension for adding single parameter.
		/// </summary>
		/// <param name="cmd">The command to add the parameter to.</param>
		/// <param name="value">The value to add as a parameter to the command.</param>
		/// <remarks>
		/// This method is needed for backwards compatibility. If it was removed completely then old code could be made to
		/// compile and work just fine simply by renaming AddNamedParam() to AddParam(). But an overload of AddParam() with
		/// exactly this old method signature would /still/ be required, for the case where someone is accessing a copy of
		/// Massive compiled into a DLL, and hence the linker is still looking for exactly this old method signature.
		/// </remarks>
		public static void AddParam(this DbCommand cmd, object value)
		{
			cmd.AddNamedParam(value);
		}

		/// <summary>
		/// Extension for adding single parameter with optional support for parameter name, value and type.
		/// </summary>
		/// <param name="cmd">The command to add the parameter to.</param>
		/// <param name="value">The value to add as a parameter to the command.</param>
		/// <param name="name">The parameter name, auto-generated if omitted.</param>
		/// <param name="direction">The parameter direction, input if omitted.</param>
		/// <param name="type">Type from which to infer sql parameter type when value is null. Optional, but required if value is null and direction is not input.</param>
		public static void AddNamedParam(this DbCommand cmd, object value, string name = null, ParameterDirection direction = ParameterDirection.Input, Type type = null)
		{
			var p = cmd.CreateParameter();
			if(name == string.Empty)
			{
				if(!p.SetAnonymousParameter())
				{
					throw new InvalidOperationException("Current ADO.NET provider does not support anonymous parameters");
				}
			}
			else
			{
				// Adding the : or @ prefix to DbParameter.ParameterName works in most cases on most databases, but
				// fails to bind for procedure/function parameter names in Oracle.
				// Not prefixing here always works, on the current versions of all currently supported providers...
				p.ParameterName = name ?? cmd.Parameters.Count.ToString();
			}
			p.SetDirection(direction);
			if(value == null)
			{
				if(type != null)
				{
					p.SetValue(type.CreateInstance());
					// explicitly lock type and size to the values which ADO.NET has just implicitly assigned
					// (when only implictly assigned, setting Value to DBNull.Value later on causes these to reset, in at least the Npgsql and SQL Server providers)
					p.DbType = p.DbType;
					p.Size = p.Size;
				}
				// Some ADO.NET providers completely ignore the parameter DbType when deciding on the .NET type for return values, others do not
				else if(direction != ParameterDirection.Input && !p.IgnoresOutputTypes())
				{
					throw new InvalidOperationException("Parameter \"" + p.ParameterName + "\" - on this ADO.NET provider all output, input-output and return parameters require non-null value or fully typed property, to allow correct SQL parameter type to be inferred");
				}
				p.Value = DBNull.Value;
			}
			else
			{
				var cursor = value as Cursor;
				if(cursor != null)
				{
					// Placeholder cursor ref; we only need the value if passing in a cursor by value
					// doesn't work on Postgres.
					if (!p.SetCursor(cursor.Value))
					{
						throw new InvalidOperationException("ADO.NET provider does not support cursors");
					}
				}
				else
				{
					// Note - the passed in parameter value can be a real cursor ref, this works - at least in Oracle
					p.SetValue(value);
				}
			}
			cmd.Parameters.Add(p);
		}


		/// <summary>
		/// Extension method for adding a set of automatically named input parameters
		/// </summary>
		/// <param name="cmd">The command to add the parameters to.</param>
		/// <param name="args">The parameter values to convert to parameters.</param>
		public static void AddParams(this DbCommand cmd, params object[] args)
		{
			if(args == null)
			{
				return;
			}
			foreach(var item in args)
			{
				AddParam(cmd, item);
			}
		}


		/// <summary>
		/// Extension method for adding in a set of parameters with name, value and direction support
		/// </summary>
		/// <param name="cmd">The command to add the parameters to.</param>
		/// <param name="nameValuePairs">Parameter names, values and types; can be ExpandoObject, NameValueCollection (or subclass), anonymous type or POCO.</param>
		/// <param name="direction">The parameter direction.</param>
		/// <remarks>
		/// object[] is also accepted for nameValuePairs in this method; this produces unnamed parameters, in ADO.NET providers which supported this.
		/// NOTE: note this is not the same as passing object[] to AddParams, which it produces automatically named parameters ("@0".."@n").
		/// </remarks>
		public static void AddNamedParams(this DbCommand cmd, object nameValuePairs, ParameterDirection direction = ParameterDirection.Input)
		{
			if(nameValuePairs == null)
			{
				return;
			}

			object[] values = nameValuePairs as object[];
			if(values != null)
			{
				// anonymous parameters from array
				foreach(var value in values)
				{
					cmd.AddNamedParam(value, string.Empty);
				}
				return;
			}

			if(nameValuePairs is ExpandoObject)
			{
				foreach(var pair in (IDictionary<string, object>)nameValuePairs)
				{
					cmd.AddNamedParam(pair.Value, pair.Key, direction);
				}
				return;
			}

			if(nameValuePairs.GetType() == typeof(NameValueCollection) || nameValuePairs.GetType().IsSubclassOf(typeof(NameValueCollection)))
			{
				var argsCollection = (NameValueCollection)nameValuePairs;
				foreach(string name in argsCollection)
				{
					cmd.AddNamedParam(argsCollection[name], name);
				}
				return;
			}

			// names, values and types from properties of anonymous object or POCO
			foreach(PropertyInfo property in nameValuePairs.GetType().GetProperties())
			{
				// Extra null in GetValue() required for .NET backwards compatibility
				cmd.AddNamedParam(property.GetValue(nameValuePairs, null), property.Name, direction, property.PropertyType);
			}
		}


		/// <summary>
		/// Extension method to read back parameter return values after execution
		/// </summary>
		/// <param name="cmd">The command from which to read the parameter values.</param>
		/// <param name="args">Object specifying the parameter names.</param>
		/// <param name="results">Dictionary for the result object.</param>
		public static void AddParamValuesToResult(this DbCommand cmd, object args, IDictionary<string, object> results)
		{
			if(args == null)
			{
				return;
			}

			object[] argsArray = args as object[];
			if(argsArray != null)
			{
				throw new InvalidOperationException("object[] arguments supported for input parameters only");
			}

			if(args is ExpandoObject)
			{
				foreach(var item in (IDictionary<string, object>)args)
				{
					string name = item.Key;
					object value = cmd.Parameters[name].Value;
					results.Add(name, value == DBNull.Value ? null : value);
				}
				return;
			}

			if(args.GetType() == typeof(NameValueCollection) || args.GetType().IsSubclassOf(typeof(NameValueCollection)))
			{
				var argsCollection = (NameValueCollection)args;
				foreach(string name in argsCollection)
				{
					object value = cmd.Parameters[name].Value;
					results.Add(name, value == DBNull.Value ? null : value);
				}
			}

			// read back values specified by anonymous object or POCO
			foreach(PropertyInfo property in args.GetType().GetProperties())
			{
				string name = property.Name;
				object value = cmd.Parameters[name].Value;
				results.Add(name, value == DBNull.Value ? null : value);
			}
		}


		/// <summary>
		/// Create non-null instance of Type
		/// </summary>
		/// <param name="type">Input type.</param>
		/// <returns>Non-null instance of type.</returns>
		/// <remarks>
		/// This supports all the types listed in ADO.NET DbParameter type-inference documentation https://msdn.microsoft.com/en-us/library/yy6y35y8(v=vs.110).aspx , except for byte[] and Object.
		/// Although this method supports all these types, the various ADO.NET providers do not:
		/// None of the providers support DbType.UInt16/32/64; Oracle and Postgres do not support DbType.Guid or DbType.Boolean.
		/// Setting DbParameter DbType or Value to one of the per-provider non-supported types will produce an ArgumentException
		/// (immediately on Postgres and Oracle, at DbCommand execution time on SQL Server).
		/// The per-database method DbParameter.SetValue is the place to add code to convert these non-supported types to supported types.
		/// </remarks>
		public static object CreateInstance(this Type type)
		{
			Type underlying = Nullable.GetUnderlyingType(type);
			if(underlying != null)
			{
				return Activator.CreateInstance(underlying);
			}
			if(type.IsValueType)
			{
				return Activator.CreateInstance(type);
			}
			if(type == typeof(string))
			{
				return "";
			}
			throw new InvalidOperationException("CreateInstance does not support type " + type);
		}


		/// <summary>
		/// Turns an IDataReader to a Dynamic list of things
		/// </summary>
		/// <param name="reader">The datareader which rows to convert to a list of expandos.</param>
		/// <returns>List of expandos, one for every row read.</returns>
		public static List<dynamic> ToExpandoList(this IDataReader reader)
		{
			var result = new List<dynamic>();
			while(reader.Read())
			{
				result.Add(reader.RecordToExpando());
			}
			return result;
		}


		/// <summary>
		/// Converts the current row the datareader points to to a new Expando object.
		/// </summary>
		/// <param name="reader">The RDR.</param>
		/// <returns>expando object which contains the values of the row the reader points to</returns>
		public static dynamic RecordToExpando(this IDataReader reader)
		{
			dynamic e = new ExpandoObject();
			var d = (IDictionary<string, object>)e;
			object[] values = new object[reader.FieldCount];
			reader.GetValues(values);
			for(int i = 0; i < values.Length; i++)
			{
				var v = values[i];
				d.Add(reader.GetName(i), DBNull.Value.Equals(v) ? null : v);
			}
			return e;
		}


		/// <summary>
		/// Use reflection to set named enum property to named value
		/// </summary>
		/// <param name="o">The object.</param>
		/// <param name="enumPropertyName">The name of the public enum property to modify.</param>
		/// <param name="enumStringValue">The string name of the enum value to set.</param>
		public static void SetRuntimeEnumProperty(this object o, string enumPropertyName, string enumStringValue)
		{
			// Both these lines can be simpler in .NET 4.5
			PropertyInfo pinfoEnumProperty = o.GetType().GetProperties().Where(property => property.Name == enumPropertyName).FirstOrDefault();
			pinfoEnumProperty.SetValue(o, Enum.Parse(pinfoEnumProperty.PropertyType, enumStringValue), null);
		}


		/// <summary>
		/// Use reflection to get string value of named enum property
		/// </summary>
		/// <param name="o">The object.</param>
		/// <param name="enumPropertyName">The name of the public enum property to get.</param>
		/// <returns></returns>
		public static string GetRuntimeEnumProperty(this object o, string enumPropertyName)
		{
			// Both these lines can be simpler in .NET 4.5
			PropertyInfo pinfoEnumProperty = o.GetType().GetProperties().Where(property => property.Name == enumPropertyName).FirstOrDefault();
			return pinfoEnumProperty.GetValue(o, null).ToString();
		}


		/// <summary>
		/// Turns the object into an ExpandoObject 
		/// </summary>
		/// <param name="o">The object to convert.</param>
		/// <returns>a new expando object with the values of the passed in object</returns>
		public static dynamic ToExpando(this object o)
		{
			if(o is ExpandoObject)
			{
				return o;
			}
			var result = new ExpandoObject();
			var d = (IDictionary<string, object>)result; //work with the Expando as a Dictionary
			if(o.GetType() == typeof(NameValueCollection) || o.GetType().IsSubclassOf(typeof(NameValueCollection)))
			{
				var nv = (NameValueCollection)o;
				nv.Cast<string>().Select(key => new KeyValuePair<string, object>(key, nv[key])).ToList().ForEach(i => d.Add(i));
			}
			else
			{
				var props = o.GetType().GetProperties();
				foreach(var item in props)
				{
					d.Add(item.Name, item.GetValue(o, null));
				}
			}
			return result;
		}


		/// <summary>
		/// Turns the object into a Dictionary with for each property a name-value pair, with name as key.
		/// </summary>
		/// <param name="thingy">The object to convert to a dictionary.</param>
		/// <returns></returns>
		public static IDictionary<string, object> ToDictionary(this object thingy)
		{
			return (IDictionary<string, object>)thingy.ToExpando();
		}


		/// <summary>
		/// Extension method to convert dynamic data to a DataTable. Useful for databinding.
		/// </summary>
		/// <param name="items"></param>
		/// <returns>A DataTable with the copied dynamic data.</returns>
		/// <remarks>Credit given to Brian Vallelunga http://stackoverflow.com/a/6298704/5262210 </remarks>
		public static DataTable ToDataTable(this IEnumerable<dynamic> items)
		{
			var data = items.ToArray();
			var toReturn = new DataTable();
			if(!data.Any())
			{
				return toReturn;
			}
			foreach(var kvp in (IDictionary<string, object>)data[0])
			{
				// for now we'll fall back to string if the value is null, as we don't know any type information on null values.
				var type = kvp.Value == null ? typeof(string) : kvp.Value.GetType();
				toReturn.Columns.Add(kvp.Key, type);
			}
			return data.ToDataTable(toReturn);
		}


		/// <summary>
		/// Extension method to convert dynamic data to a DataTable. Useful for databinding.
		/// </summary>
		/// <param name="items">The items to convert to data rows.</param>
		/// <param name="toFill">The datatable to fill. It's required this datatable has the proper columns setup.</param>
		/// <returns>
		/// toFill with the data from items.
		/// </returns>
		/// <remarks>
		/// Credit given to Brian Vallelunga http://stackoverflow.com/a/6298704/5262210
		/// </remarks>
		public static DataTable ToDataTable(this IEnumerable<dynamic> items, DataTable toFill)
		{
			dynamic[] data = items is dynamic[] ? (dynamic[])items : items.ToArray();
			if(toFill == null || toFill.Columns.Count <= 0)
			{
				return toFill;
			}
			foreach(var d in data)
			{
				toFill.Rows.Add(((IDictionary<string, object>)d).Values.ToArray());
			}
			return toFill;
		}
	}


	/// <summary>
	/// Convenience class for opening/executing data
	/// </summary>
	public static class DB
	{
		public static DynamicModel Current
		{
			get
			{
				if(ConfigurationManager.ConnectionStrings.Count > 1)
				{
					return new DynamicModel(ConfigurationManager.ConnectionStrings[1].Name);
				}
				throw new InvalidOperationException("Need a connection string name - can't determine what it is");
			}
		}
	}


	/// <summary>
	/// Set this class as the value of a parameter to indicate that the underlying db cursor type should be used.
	/// </summary>
	public class Cursor
	{
		internal object Value { get; private set; }

		/// <summary>
		/// Construct an output or return direction cursor parameter
		/// </summary>
		public Cursor()
		{
		}


		/// <summary>
		/// Construct an input or input-output direction cursor parameter
		/// </summary>
		/// <param name="value">A cursor object returned previously by an output or return direction cursor parameter, or null</param>
		public Cursor(object value)
		{
			Value = value;
		}
	}


	/// <summary>
	/// A class that wraps your database table in Dynamic Funtime
	/// </summary>
	/// <seealso cref="System.Dynamic.DynamicObject" />
	public partial class DynamicModel : DynamicObject
	{
		#region Members
		private DbProviderFactory _factory;
		private string _connectionString;
		private IEnumerable<dynamic> _schema;
		private string _primaryKeyFieldSequence;
		#endregion


		/// <summary>
		/// Initializes a new instance of the <see cref="DynamicModel" /> class.
		/// </summary>
		/// <param name="connectionStringName">Name of the connection string to load from the config file.</param>
		/// <param name="tableName">Name of the table to read the meta data for. Can be left empty, in which case the name of this type is used.</param>
		/// <param name="primaryKeyField">The primary key field. Can be left empty, in which case 'ID' is used.</param>
		/// <param name="descriptorField">The descriptor field, if the table is a lookup table. Descriptor field is the field containing the textual representation of the value
		/// in primaryKeyField.</param>
		/// <param name="primaryKeyFieldSequence">The primary key sequence to use. Specify the empty string if the PK isn't sequenced/identity. Is initialized by default with
		/// the name specified in the constant DynamicModel.DefaultSequenceName.</param>
		/// <param name="connectionStringProvider">The connection string provider to use. By default this is empty and the default provider is used which will read values from 
		/// the application's config file.</param>
		public DynamicModel(string connectionStringName, string tableName = "", string primaryKeyField = "", string descriptorField = "",
							string primaryKeyFieldSequence = DynamicModel._defaultSequenceName,
							IConnectionStringProvider connectionStringProvider = null)
		{
			this.TableName = string.IsNullOrWhiteSpace(tableName) ? this.GetType().Name : tableName;
			ProcessTableName();
			this.PrimaryKeyField = string.IsNullOrWhiteSpace(primaryKeyField) ? "ID" : primaryKeyField;
			_primaryKeyFieldSequence = primaryKeyFieldSequence == "" ? ConfigurationManager.AppSettings["default_seq"] : primaryKeyFieldSequence;
			this.DescriptorField = descriptorField;
			this.Errors = new List<string>();

			if(connectionStringProvider == null)
			{
				connectionStringProvider = new ConfigurationBasedConnectionStringProvider();
			}
			var _providerName = connectionStringProvider.GetProviderName(connectionStringName);
			if(string.IsNullOrWhiteSpace(_providerName))
			{
				_providerName = this.DbProviderFactoryName;
			}
			_factory = DbProviderFactories.GetFactory(_providerName);
			_connectionString = connectionStringProvider.GetConnectionString(connectionStringName);
		}


		/// <summary>
		/// Gets a default value for the column with the name specified as defined in the schema.
		/// </summary>
		/// <param name="columnName">Name of the column.</param>
		/// <returns></returns>
		public dynamic DefaultValue(string columnName)
		{
			var column = GetColumn(columnName);
			if(column == null)
			{
				return null;
			}
			return GetDefaultValue(column);
		}


		/// <summary>
		/// Gets or creates a new, empty DynamicModel on the DB pointed to by the connectionstring stored under the name specified.
		/// </summary>
		/// <param name="connectionStringName">Name of the connection string to load from the config file.</param>
		/// <returns>ready to use, empty DynamicModel</returns>
		public static DynamicModel Open(string connectionStringName)
		{
			return new DynamicModel(connectionStringName);
		}


		/// <summary>
		/// Creates a new Expando from a Form POST - white listed against the columns in the DB, only setting values which names are in the schema.
		/// </summary>
		/// <param name="coll">The name-value collection coming from an external source, e.g. a POST.</param>
		/// <returns>new expando object with the fields as defined in the schema and with the values as specified in the collection passed in</returns>
		public dynamic CreateFrom(NameValueCollection coll)
		{
			dynamic result = new ExpandoObject();
			var dc = (IDictionary<string, object>)result;
			foreach(var item in coll.Keys)
			{
				var columnName = item.ToString();
				if(this.ColumnExists(columnName))
				{
					dc.Add(columnName, coll[columnName]);
				}
			}
			return result;
		}


		/// <summary>
		/// Enumerates the reader yielding the result
		/// </summary>
		/// <param name="sql">The SQL to execute as a command.</param>
		/// <param name="args">The parameter values.</param>
		/// <returns>streaming enumerable with expandos, one for each row read</returns>
		public virtual IEnumerable<dynamic> Query(string sql, params object[] args)
		{
			using(var conn = OpenConnection())
			{
				using(var rdr = CreateCommand(sql, conn, args).ExecuteReader())
				{
					while(rdr.Read())
					{
						yield return rdr.RecordToExpando();
					}
					rdr.Close();
				}
				conn.Close();
			}
		}


		/// <summary>
		/// Enumerates a reader for multiple result sets
		/// </summary>
		/// <param name="sql">The SQL to execute as a command.</param>
		/// <param name="args">The parameter values.</param>
		/// <returns>streaming enumerable of enumerables, outer enumerable is the result sets, objects of inner enumerable are expandos, one for each row read</returns>
		public virtual IEnumerable<IEnumerable<dynamic>> QueryMultiple(string sql, params object[] args)
		{
			using(var conn = OpenConnection())
			{
				using(var rdr = CreateCommand(sql, conn, args).ExecuteReader())
				{
					do
					{
						yield return rdr.YieldResult();
					}
					while(rdr.NextResult());
				}
				conn.Close();
			}
		}


		/// <summary>
		/// Enumerates a reader yielding the result of procedure or function call, with optional directional parameters.
		/// For each set of parameters, you can pass in an Anonymous object, an ExpandoObject, a regular old POCO, or a NameValueCollection e.g. from a Request.Form or Request.QueryString.
		/// </summary>
		/// <param name="spName">The procedure name.</param>
		/// <param name="inParams">The input parameter collection. Additionally accepts object[] for anonymous parameter support, on ADO.NET providers which support this.</param>
		/// <param name="outParams">The output parameter collection.</param>
		/// <param name="ioParams">The input-output parameter collection.</param>
		/// <param name="returnParams">The return value collection.</param>
		/// <param name="connection">The connection to use, has to be open.</param>
		/// <returns>streaming enumerable with expandos, one for each row read</returns>
		public virtual IEnumerable<dynamic> QueryFromProcedure(string spName, object inParams = null, object outParams = null, object ioParams = null, object returnParams = null, DbConnection connection = null)
		{
			return QueryWithParams(spName, inParams, outParams, ioParams, returnParams, true, connection);
		}


		/// <summary>
		/// Enumerates reader yielding multiple result sets from the result of procedure or function call, with optional directional parameters.
		/// For each set of parameters, you can pass in an Anonymous object, an ExpandoObject, a regular old POCO, or a NameValueCollection e.g. from a Request.Form or Request.QueryString.
		/// </summary>
		/// <param name="spName">The procedure name.</param>
		/// <param name="inParams">The input parameter collection. Additionally accepts object[] for anonymous parameter support, on ADO.NET providers which support this.</param>
		/// <param name="outParams">The output parameter collection.</param>
		/// <param name="ioParams">The input-output parameter collection.</param>
		/// <param name="returnParams">The return value collection.</param>
		/// <param name="connection">The connection to use, has to be open.</param>
		/// <returns>streaming enumerable with expandos, one for each row read</returns>
		public virtual IEnumerable<IEnumerable<dynamic>> QueryMultipleFromProcedure(string spName, object inParams = null, object outParams = null, object ioParams = null, object returnParams = null, DbConnection connection = null)
		{
			return QueryMultipleWithParams(spName, inParams, outParams, ioParams, returnParams, true, connection);
		}


		/// <summary>
		/// Enumerates a reader yielding the result of procedure, function or specified SQL call, with optional directional parameters.
		/// For each set of parameters, you can pass in an Anonymous object, an ExpandoObject, a regular old POCO, or a NameValueCollection e.g. from a Request.Form or Request.QueryString.
		/// </summary>
		/// <param name="sql">Stored procedure name (or general SQL if isProcedure=false)</param>
		/// <param name="inParams">Input parameters (optional). Names and values are used.</param>
		/// <param name="outParams">Output parameters (optional). Names are used. Values are used to determine parameter type.</param>
		/// <param name="ioParams">Input-output parameters (optional). Names and values are used.</param>
		/// <param name="returnParams">Return parameters (optional). Names are used. Values are used to determine parameter type.</param>
		/// <param name="isProcedure">Whether to execute the command as stored procedure or general SQL. Defaults to general SQL.</param>
		/// <param name="connection">The connection to use, has to be open.</param>
		/// <returns>streaming enumerable with expandos, one for each row read</returns>
		public IEnumerable<dynamic> QueryWithParams(string sql, object inParams = null, object outParams = null, object ioParams = null, object returnParams = null, bool isProcedure = false, DbConnection connection = null)
		{
			return QueryNWithParams<dynamic>(sql, inParams, outParams, ioParams, returnParams, isProcedure, connection);
		}


		/// <summary>
		/// Enumerates reader yielding multiple result sets from the result of procedure, function or specified SQL, with optional directional parameters.
		/// For each set of parameters, you can pass in an Anonymous object, an ExpandoObject, a regular old POCO, or a NameValueCollection e.g. from a Request.Form or Request.QueryString.
		/// </summary>
		/// <param name="sql">Stored procedure name (or general SQL if isProcedure=false)</param>
		/// <param name="inParams">Input parameters (optional). Names and values are used.</param>
		/// <param name="outParams">Output parameters (optional). Names are used. Values are used to determine parameter type.</param>
		/// <param name="ioParams">Input-output parameters (optional). Names and values are used.</param>
		/// <param name="returnParams">Return parameters (optional). Names are used. Values are used to determine parameter type.</param>
		/// <param name="isProcedure">Whether to execute the command as stored procedure or general SQL. Defaults to general SQL.</param>
		/// <param name="connection">The connection to use, has to be open.</param>
		/// <returns>streaming enumerable with expandos, one for each row read</returns>
		public IEnumerable<IEnumerable<dynamic>> QueryMultipleWithParams(string sql, object inParams = null, object outParams = null, object ioParams = null, object returnParams = null, bool isProcedure = false, DbConnection connection = null)
		{
			return QueryNWithParams<IEnumerable<dynamic>>(sql, inParams, outParams, ioParams, returnParams, isProcedure, connection);
		}


		/// <summary>
		/// Share the main logic for QueryWithParams and QueryMultipleWithParams
		/// </summary>
		/// <typeparam name="T"></typeparam>
		/// <param name="sql"></param>
		/// <param name="inParams"></param>
		/// <param name="outParams"></param>
		/// <param name="ioParams"></param>
		/// <param name="returnParams"></param>
		/// <param name="isProcedure"></param>
		/// <param name="connection"></param>
		/// <returns></returns>
		private IEnumerable<T> QueryNWithParams<T>(string sql, object inParams = null, object outParams = null, object ioParams = null, object returnParams = null, bool isProcedure = false, DbConnection connection = null)
		{
			using(var localConn = (connection == null ? OpenConnection() : null))
			{
				var cmd = CreateCommandWithNamedParams(sql, inParams, outParams, ioParams, returnParams, isProcedure, connection ?? localConn);
				// manage wrapping transaction if required, and if we have not been passed an incoming connection
				using(var trans = ((connection == null && Transaction.Current == null && cmd.RequiresWrappingTransaction(this)) ? localConn.BeginTransaction() : null))
				{
					// TO DO: Apply single result hint when appropriate (once Npgsql is dereferencing for us)
					using(var rdr = cmd.ExecuteDereferencingReader(connection ?? localConn, this))
					{
						if(typeof(T) == typeof(IEnumerable<dynamic>))
						{
							// query multiple pattern
							do
							{
								yield return (T)rdr.YieldResult();
							}
							while(rdr.NextResult());
						}
						else
						{
							// query pattern
							while(rdr.Read())
							{
								yield return rdr.RecordToExpando();
							}
						}
					}
					if(trans != null) trans.Commit();
				}
			}
		}


		/// <summary>
		/// Enumerates the reader yielding the result
		/// </summary>
		/// <param name="sql">The SQL to execute as a command.</param>
		/// <param name="connection">The connection to use with the command.</param>
		/// <param name="args">The parameter values.</param>
		/// <returns>streaming enumerable with expandos, one for each row read</returns>
		public virtual IEnumerable<dynamic> Query(string sql, DbConnection connection, params object[] args)
		{
			using(var rdr = CreateCommand(sql, connection, args).ExecuteReader())
			{
				while(rdr.Read())
				{
					yield return rdr.RecordToExpando();
				}
				rdr.Close();
			}
		}


		/// <summary>
		/// Returns a single result by executing the passed in query + parameters as a scalar query.
		/// </summary>
		/// <param name="sql">The SQL to execute as a scalar command.</param>
		/// <param name="args">The parameter values.</param>
		/// <returns>first value returned from the query executed or null of no result was returned by the database.</returns>
		public virtual object Scalar(string sql, params object[] args)
		{
			object result;
			using(var conn = OpenConnection())
			{
				result = CreateCommand(sql, conn, args).ExecuteScalar();
				conn.Close();
			}
			return result;
		}


		/// <summary>
		/// Returns an OpenConnection
		/// </summary>
		public virtual DbConnection OpenConnection()
		{
			var result = _factory.CreateConnection();
			if(result != null)
			{
				result.ConnectionString = _connectionString;
				result.Open();
			}
			return result;
		}


		/// <summary>
		/// Executes the specified command using a new connection
		/// </summary>
		/// <param name="command">The command to execute.</param>
		/// <returns>the value returned by the database after executing the command. </returns>
		public virtual int Execute(DbCommand command)
		{
			return Execute(new[] { command });
		}


		/// <summary>
		/// Executes the specified SQL as a new command using a new connection. 
		/// </summary>
		/// <param name="sql">The SQL statement to execute as a command.</param>
		/// <param name="args">The parameter values.</param>
		/// <returns>the value returned by the database after executing the command. </returns>
		public virtual int Execute(string sql, params object[] args)
		{
			return Execute(CreateCommand(sql, null, args));
		}


		/// <summary>
		/// Executes the specified SQL as a new command using a new connection. 
		/// </summary>
		/// <param name="sql">The SQL statement to execute as a command.</param>
		/// <param name="connection">The connection to use with the command.</param>
		/// <param name="args">The parameter values.</param>
		/// <returns>The value returned by the database after executing the command.</returns>
		/// <remarks>After some fairly extensive tests, I believe that adding this 'missing' overload is fully non-breaking for both compile and link against existing code.</remarks>
		public virtual int Execute(string sql, DbConnection connection, params object[] args)
		{
			return ExecuteDbCommand(CreateCommand(sql, null, args), connection, null);
		}


		/// <summary>
		/// Stored procedure and function support for named, typed, directional parameters.
		/// For each set of parameters, you can pass in an Anonymous object, an ExpandoObject, a regular old POCO, or a NameValueCollection e.g. from a Request.Form or Request.QueryString.
		/// </summary>
		/// <param name="spName">The procedure name.</param>
		/// <param name="conn">The connection to use, normally null to let Massive manage this.</param>
		/// <param name="inParams">The input parameter collection. Additionally accepts object[] for anonymous parameter support, on ADO.NET providers which support this.</param>
		/// <param name="outParams">The output parameter collection.</param>
		/// <param name="ioParams">The input-output parameter collection.</param>
		/// <param name="returnParams">The return value collection.</param>
		/// <param name="connection">The connection to use, has to be open.</param>
		/// <returns>Dynamic containing return values of all non-input parameters.</returns>
		public virtual dynamic ExecuteAsProcedure(string spName, object inParams = null, object outParams = null, object ioParams = null, object returnParams = null, DbConnection connection = null)
		{
			return ExecuteWithParams(spName, inParams, outParams, ioParams, returnParams, true, connection);
		}


		/// <summary>
		/// Execute procedure, function or specified SQL with optional directional parameters, send back the return values of all non-input parameters in a dynamic object.
		/// For each set of parameters, you can pass in an Anonymous object, an ExpandoObject, a regular old POCO, or a NameValueCollection e.g. from a Request.Form or Request.QueryString.
		/// </summary>
		/// <param name="sql">Stored procedure name (or general SQL if isProcedure=false)</param>
		/// <param name="inParams">Input parameters (optional). Names and values are used.</param>
		/// <param name="outParams">Output parameters (optional). Names are used. Values are used to determine parameter type.</param>
		/// <param name="ioParams">Input-output parameters (optional). Names and values are used.</param>
		/// <param name="returnParams">Return parameters (optional). Names are used. Values are used to determine parameter type.</param>
		/// <param name="isProcedure">Whether to execute the command as stored procedure or general SQL. Defaults to general SQL.</param>
		/// <param name="connection">The connection to use, has to be open.</param>
		/// <returns>Dynamic holding return values of any output, input-output and return parameters.</returns>
		public dynamic ExecuteWithParams(string sql, object inParams = null, object outParams = null, object ioParams = null, object returnParams = null, bool isProcedure = false, DbConnection connection = null)
		{
			dynamic result = new ExpandoObject();
			using(var localConn = (connection == null ? OpenConnection() : null))
			{
				var cmd = CreateCommandWithNamedParams(sql, inParams, outParams, ioParams, returnParams, isProcedure, connection ?? localConn);
				cmd.ExecuteNonQuery();
				var resultDictionary = (IDictionary<string, object>)result;
				cmd.AddParamValuesToResult(outParams, resultDictionary);
				cmd.AddParamValuesToResult(ioParams, resultDictionary);
				cmd.AddParamValuesToResult(returnParams, resultDictionary);
			}
			return result;
		}


		/// <summary>
		/// Executes a series of DBCommands in a new transaction using a new connection
		/// </summary>
		/// <param name="commands">The commands to execute.</param>
		/// <returns>the sum of the values returned by the database when executing each command.</returns>
		public virtual int Execute(IEnumerable<DbCommand> commands)
		{
			var result = 0;
			using(var conn = OpenConnection())
			{
				using(var tx = conn.BeginTransaction())
				{
					foreach(var cmd in commands)
					{
						result += ExecuteDbCommand(cmd, conn, tx);
					}
					tx.Commit();
				}
				conn.Close();
			}
			return result;
		}


		/// <summary>
		/// Conventionally introspects the object passed in for a field that looks like a PK. If you've named your PrimaryKeyField, this becomes easy
		/// </summary>
		public virtual bool HasPrimaryKey(object o)
		{
			return o.ToDictionary().ContainsKey(PrimaryKeyField);
		}


		/// <summary>
		/// If the object passed in has a property with the same name as your PrimaryKeyField it is returned here.
		/// </summary>
		public virtual object GetPrimaryKey(object o)
		{
			object result;
			o.ToDictionary().TryGetValue(PrimaryKeyField, out result);
			return result;
		}


		/// <summary>
		/// Returns all records complying with the passed-in WHERE clause and arguments, ordered as specified, limited by limit specified using the DB specific limit system.
		/// </summary>
		/// <param name="where">The where clause. Default is empty string. Parameters have to be numbered starting with 0, for each value in args.</param>
		/// <param name="orderBy">The order by clause. Default is empty string.</param>
		/// <param name="limit">The limit. Default is 0 (no limit).</param>
		/// <param name="columns">The columns to use in the project. Default is '*' (all columns, in table defined order).</param>
		/// <param name="args">The values to use as parameters.</param>
		/// <returns>streaming enumerable with expandos, one for each row read</returns>
		public virtual IEnumerable<dynamic> All(string where = "", string orderBy = "", int limit = 0, string columns = "*", params object[] args)
		{
			return Query(string.Format(BuildSelectQueryPattern(where, orderBy, limit), columns, TableName), args);
		}


		/// <summary>
		/// Fetches a dynamic PagedResult. 
		/// </summary>
		/// <param name="where">The where clause. Default is empty string. Parameters have to be numbered starting with 0, for each value in args.</param>
		/// <param name="orderBy">The order by clause. Default is empty string.</param>
		/// <param name="columns">The columns to use in the project. Default is '*' (all columns, in table defined order).</param>
		/// <param name="pageSize">Size of the page. Default is 20</param>
		/// <param name="currentPage">The current page. 1-based. Default is 1.</param>
		/// <param name="args">The values to use as parameters.</param>
		/// <returns>The result of the paged query. Result properties are Items, TotalPages, and TotalRecords.</returns>
		public virtual dynamic Paged(string where = "", string orderBy = "", string columns = "*", int pageSize = 20, int currentPage = 1, params object[] args)
		{
			return BuildPagedResult(whereClause: where, orderByClause: orderBy, columns: columns, pageSize: pageSize, currentPage: currentPage, args: args);
		}


		/// <summary>
		/// Fetches a dynamic PagedResult.
		/// </summary>
		/// <param name="sql">The SQL statement to use as query over which resultset is paged.</param>
		/// <param name="primaryKey">The primary key to use for ordering. Can be left empty</param>
		/// <param name="where">The where clause. Default is empty string. Parameters have to be numbered starting with 0, for each value in args.</param>
		/// <param name="orderBy">The order by clause. Default is empty string.</param>
		/// <param name="columns">The columns to use in the project. Default is '*' (all columns, in table defined order).</param>
		/// <param name="pageSize">Size of the page. Default is 20</param>
		/// <param name="currentPage">The current page. 1-based. Default is 1.</param>
		/// <param name="args">The values to use as parameters.</param>
		/// <returns>
		/// The result of the paged query. Result properties are Items, TotalPages, and TotalRecords.
		/// </returns>
		public virtual dynamic Paged(string sql, string primaryKey, string where = "", string orderBy = "", string columns = "*", int pageSize = 20, int currentPage = 1, params object[] args)
		{
			return BuildPagedResult(sql, primaryKey, where, orderBy, columns, pageSize, currentPage, args);
		}


		/// <summary>
		/// Returns a single row from the database
		/// </summary>
		/// <param name="where">The where clause.</param>
		/// <param name="args">The arguments.</param>
		/// <returns></returns>
		public virtual dynamic Single(string where, params object[] args)
		{
			return All(where, limit: 1, args: args).FirstOrDefault();
		}


		/// <summary>
		/// Returns a single row from the database
		/// </summary>
		/// <param name="key">The pk value.</param>
		/// <param name="columns">The columns to fetch.</param>
		/// <returns></returns>
		public virtual dynamic Single(object key, string columns = "*")
		{
			return All(this.GetPkComparisonPredicateQueryFragment(), limit: 1, columns: columns, args: new[] { key }).FirstOrDefault();
		}


		/// <summary>
		/// This will return a string/object dictionary for dropdowns etc
		/// </summary>
		public virtual IDictionary<string, object> KeyValues(string orderBy = "")
		{
			if(string.IsNullOrEmpty(DescriptorField))
			{
				throw new InvalidOperationException("There's no DescriptorField set - do this in your constructor to describe the text value you want to see");
			}
			var results = All(orderBy: orderBy, columns: string.Format("{0}, {1}", this.PrimaryKeyField, this.DescriptorField)).ToList().Cast<IDictionary<string, object>>();
			return results.ToDictionary(key => key[PrimaryKeyField].ToString(), value => value[DescriptorField]);
		}


		/// <summary>
		/// This will return an Expando as a Dictionary. This method does a cast to an interface through a method call, which means it's ... rather useless.
		/// </summary>
		/// <param name="item">The item to convert</param>
		/// <returns></returns>
		public virtual IDictionary<string, object> ItemAsDictionary(ExpandoObject item)
		{
			return item;
		}


		/// <summary>
		/// Checks to see if a key is present based on the passed-in value
		/// </summary>
		/// <param name="key">The key to search for.</param>
		/// <param name="item">The expando object to search for the key.</param>
		/// <returns>true if the passed in expando object contains key, false otherwise</returns>
		public virtual bool ItemContainsKey(string key, ExpandoObject item)
		{
			return ((IDictionary<string, object>)item).ContainsKey(key);
		}


		/// <summary>
		/// Executes a set of objects as Insert or Update commands based on their property settings, within a transaction. These objects can be POCOs, Anonymous, NameValueCollections, 
		/// or Expandos. Objects with a PK property (whatever PrimaryKeyField is set to) will be created at UPDATEs
		/// </summary>
		/// <param name="things">The objects to save within a single transaction.</param>
		/// <returns>the sum of the values returned by the database when executing each command.</returns>
		public virtual int Save(params object[] things)
		{
			if(things == null)
			{
				throw new ArgumentNullException("things");
			}
			if(things.Any(item => !IsValid(item)))
			{
				throw new InvalidOperationException("Can't save this item: " + string.Join("; ", this.Errors.ToArray()));
			}
			return PerformSave(false, things);
		}


		/// <summary>
		/// Executes a set of objects as Insert commands, within a transaction. These objects can be POCOs, Anonymous, NameValueCollections, or Expandos. 
		/// </summary>
		/// <param name="things">The objects to save within a single transaction.</param>
		/// <returns>the sum of the values returned by the database when executing each command.</returns>
		public virtual int SaveAsNew(params object[] things)
		{
			if(things == null)
			{
				throw new ArgumentNullException("things");
			}
			if(things.Any(item => !IsValid(item)))
			{
				throw new InvalidOperationException("Can't save this item: " + string.Join("; ", this.Errors.ToArray()));
			}
			return PerformSave(true, things);
		}


		/// <summary>
		/// Adds a record to the database. You can pass in an Anonymous object, an ExpandoObject, a regular old POCO, or a NameValueCollection from a Request.Form or Request.QueryString
		/// </summary>
		/// <param name="o">The object to insert.</param>
		/// <returns>the object inserted as expando. If the PrimaryKeyField is an identity field, it's set in the returned object to the value it received at insert.</returns>
		public virtual dynamic Insert(object o)
		{
			var oAsExpando = o.ToExpando();
			if(!IsValid(oAsExpando))
			{
				throw new InvalidOperationException("Can't insert: " + string.Join("; ", Errors.ToArray()));
			}
			if(BeforeSave(oAsExpando))
			{
				using(var conn = OpenConnection())
				{
					PerformInsert(conn, null, oAsExpando);
					Inserted(oAsExpando);
					conn.Close();
				}
				return oAsExpando;
			}
			return null;
		}


		/// <summary>
		/// Updates a record in the database. You can pass in an Anonymous object, an ExpandoObject, a regular old POCO, or a NameValueCollection from a Request.Form or Request.QueryString
		/// </summary>
		/// <param name="o">The object to update</param>
		/// <param name="key">The key value to compare against PrimaryKeyField.</param>
		/// <returns>the number returned by the database after executing the update command </returns>
		public virtual int Update(object o, object key)
		{
			var oAsExpando = o.ToExpando();
			if(!IsValid(oAsExpando))
			{
				throw new InvalidOperationException("Can't Update: " + string.Join("; ", Errors.ToArray()));
			}
			var result = 0;
			if(BeforeSave(oAsExpando))
			{
				result = Execute(CreateUpdateCommand(oAsExpando, key));
				Updated(oAsExpando);
			}
			return result;
		}


		/// <summary>
		/// Updates a all records in the database that match where clause. You can pass in an Anonymous object, an ExpandoObject,
		/// A regular old POCO, or a NameValueCollection from a Request.Form or Request.QueryString. Where works same same as in All().
		/// </summary>
		/// <param name="o">The object to update</param>
		/// <param name="where">The where clause. Default is empty string. Parameters have to be numbered starting with 0, for each value in args.</param>
		/// <param name="args">The parameters used in the where clause.</param>
		/// <returns>the number returned by the database after executing the update command </returns>
		public virtual int Update(object o, string where = "1=1", params object[] args)
		{
			if(string.IsNullOrWhiteSpace(where))
			{
				return 0;
			}
			var oAsExpando = o.ToExpando();
			if(!IsValid(oAsExpando))
			{
				throw new InvalidOperationException("Can't Update: " + string.Join("; ", Errors.ToArray()));
			}
			var result = 0;
			if(BeforeSave(oAsExpando))
			{
				result = Execute(CreateUpdateWhereCommand(oAsExpando, where, args));
				Updated(oAsExpando);
			}
			return result;
		}


		/// <summary>
		/// Deletes one or more records from the DB according to the passed-in where clause/key value. 
		/// </summary>
		/// <param name="key">The key. Value to compare with the PrimaryKeyField. If null, <see cref="where"/> is used as the where clause.</param>
		/// <param name="where">The where clause. Can be empty. Ignored if key is set.</param>
		/// <param name="args">The parameter values.</param>
		/// <returns></returns>
		public virtual int Delete(object key = null, string where = "", params object[] args)
		{
			if(key == null)
			{
				// directly delete on the DB, no fetch of individual element
				return Execute(CreateDeleteCommand(where, null, args));
			}
			var deleted = Single(key);
			var result = 0;
			if(BeforeDelete(deleted))
			{
				result = Execute(CreateDeleteCommand(where, key, args));
				Deleted(deleted);
			}
			return result;
		}


		/// <summary>
		/// Adds the value to item with the name stored in key, if item doesn't already contains a field with the name in key
		/// </summary>
		/// <param name="key">The key.</param>
		/// <param name="value">The value.</param>
		/// <param name="item">The item.</param>
		public void DefaultTo(string key, object value, dynamic item)
		{
			if(!ItemContainsKey(key, item))
			{
				var dc = (IDictionary<string, object>)item;
				dc[key] = value;
			}
		}


		/// <summary>
		/// Determines whether the specified item is valid. Errors are stored in the Errors property
		/// </summary>
		/// <param name="item">The item to validate.</param>
		/// <returns>true if valid (0 errors), false otherwise</returns>
		public bool IsValid(dynamic item)
		{
			Errors.Clear();
			Validate(item);
			return Errors.Count == 0;
		}


		/// <summary>
		/// Validates if the value specified is null or not. If it is null, the message is added to Errors.
		/// </summary>
		/// <param name="value">The value to check.</param>
		/// <param name="message">The message to log as Error. Default is 'Required'.</param>
		public virtual void ValidatesPresenceOf(object value, string message = "Required")
		{
			if((value == null) || string.IsNullOrEmpty(value.ToString()))
			{
				Errors.Add(message);
			}
		}


		/// <summary>
		/// Validates whether the value specified is numeric (short, int, long, double, single and float). If not, message is logged in Errors.
		/// </summary>
		/// <param name="value">The value.</param>
		/// <param name="message">The message to log as Error. Default is 'Required'.</param>
		public virtual void ValidatesNumericalityOf(object value, string message = "Should be a number")
		{
			var numerics = new[] { "Int32", "Int16", "Int64", "Decimal", "Double", "Single", "Float" };
			if((value == null) || !numerics.Contains(value.GetType().Name))
			{
				Errors.Add(message);
			}
		}


		/// <summary>
		/// Executes a Count(*) query on the Table
		/// </summary>
		/// <returns>number of rows returned after executing the count query</returns>
		public int Count()
		{
			return Count(TableName);
		}


		/// <summary>
		/// Executes a Count(*) query on the Tablename specified using the where clause specified
		/// </summary>
		/// <param name="tableName">Name of the table to execute the count query on. By default it's this table's name</param>
		/// <param name="where">The where clause. Default is empty string. Parameters have to be numbered starting with 0, for each value in args.</param>
		/// <param name="args">The parameters used in the where clause.</param>
		/// <returns>number of rows returned after executing the count query</returns>
		/// <remarks>
		/// In order to retain cross-DB compatibility we are coercing long values (e.g. MySql) to int values, note that a simple cast would always exception regardless of the value.
		/// </remarks>
		public int Count(string tableName = "", string where = "", params object[] args)
		{
			var scalarQueryPattern = this.GetCountRowQueryPattern();
			scalarQueryPattern += ReadifyWhereClause(where);
			return Convert.ToInt32(Scalar(string.Format(scalarQueryPattern, string.IsNullOrEmpty(tableName) ? this.TableName : tableName), args));
		}


		/// <summary>
		/// Provides the implementation for operations that invoke a member. This method implementation tries to create queries from the methods being invoked based on the name
		/// of the invoked method.
		/// </summary>
		/// <param name="binder">Provides information about the dynamic operation. The binder.Name property provides the name of the member on which the dynamic operation is performed. 
		/// For example, for the statement sampleObject.SampleMethod(100), where sampleObject is an instance of the class derived from the <see cref="T:System.Dynamic.DynamicObject" /> class, 
		/// binder.Name returns "SampleMethod". The binder.IgnoreCase property specifies whether the member name is case-sensitive.</param>
		/// <param name="args">The arguments that are passed to the object member during the invoke operation. For example, for the statement sampleObject.SampleMethod(100), where sampleObject is 
		/// derived from the <see cref="T:System.Dynamic.DynamicObject" /> class, <paramref name="args[0]" /> is equal to 100.</param>
		/// <param name="result">The result of the member invocation.</param>
		/// <returns>
		/// true if the operation is successful; otherwise, false. If this method returns false, the run-time binder of the language determines the behavior. (In most cases, a language-specific 
		/// run-time exception is thrown.)
		/// </returns>
		public override bool TryInvokeMember(InvokeMemberBinder binder, object[] args, out object result)
		{
			result = null;
			var info = binder.CallInfo;
			if(info.ArgumentNames.Count != args.Length)
			{
				throw new InvalidOperationException("Please use named arguments for this type of query - the column name, orderby, columns, etc");
			}

			var columns = " * ";
			var orderByClauseFragment = string.Format(" ORDER BY {0}", PrimaryKeyField);
			var whereClauseFragment = string.Empty;
			var whereArguments = new List<object>();
			var wherePredicates = new List<string>();
			var counter = 0;
			if(info.ArgumentNames.Count > 0)
			{
				for(int i = 0; i < args.Length; i++)
				{
					var name = info.ArgumentNames[i].ToLowerInvariant();
					switch(name)
					{
						case "orderby":
							orderByClauseFragment = " ORDER BY " + args[i];
							break;
						case "columns":
							columns = args[i].ToString();
							break;
						case "where":
							// add it as-is.
							wherePredicates.Add(args[i].ToString());
							break;
						default:
							wherePredicates.Add(string.Format(" {0} = {1}", name, this.PrefixParameterName(counter.ToString())));
							whereArguments.Add(args[i]);
							counter++;
							break;
					}
				}
			}
			if(wherePredicates.Count > 0)
			{
				whereClauseFragment = " WHERE " + string.Join(" AND ", wherePredicates.ToArray());
			}

			var op = binder.Name;
			var oplowercase = op.ToLowerInvariant();
			switch(oplowercase)
			{
				case "count":
					result = Count(TableName, whereClauseFragment, whereArguments.ToArray());
					break;
				case "sum":
				case "max":
				case "min":
				case "avg":
					var aggregate = this.GetAggregateFunction(oplowercase);
					if(!string.IsNullOrWhiteSpace(aggregate))
					{
						result = Scalar(string.Format("SELECT {0}({1}) FROM {2} {3}", aggregate, columns, this.TableName, whereClauseFragment), whereArguments.ToArray());
					}
					break;
				default:
					var justOne = op.StartsWith("First") || op.StartsWith("Last") || op.StartsWith("Get") || op.StartsWith("Find") || op.StartsWith("Single");
					//Be sure to sort by DESC on the PK (PK Sort is the default)
					if(op.StartsWith("Last"))
					{
						orderByClauseFragment = orderByClauseFragment + " DESC ";
					}
					result = justOne ? All(whereClauseFragment, orderByClauseFragment, 1, columns, whereArguments.ToArray()).FirstOrDefault() : All(whereClauseFragment, orderByClauseFragment, 0, columns, whereArguments.ToArray());
					break;
			}
			return true;
		}


		/// <summary>
		/// Creates a DbCommand with an insert statement to insert a new row in the table, using the values in the passed in expando.
		/// </summary>
		/// <param name="expando">The expando object which contains per field the value to insert.</param>
		/// <returns>ready to use DbCommand</returns>
		/// <exception cref="System.InvalidOperationException">Can't parse this object to the database - there are no properties set</exception>
		public virtual DbCommand CreateInsertCommand(dynamic expando)
		{
			var fieldNames = new List<string>();
			var valueParameters = new List<string>();
			var insertQueryPattern = this.GetInsertQueryPattern();
			var result = CreateCommand(insertQueryPattern, null);
			int counter = 0;
			foreach(var item in (IDictionary<string, object>)expando)
			{
				fieldNames.Add(item.Key);
				valueParameters.Add(this.PrefixParameterName(counter.ToString()));
				result.AddParam(item.Value);
				counter++;
			}
			if(counter > 0)
			{
				result.CommandText = string.Format(insertQueryPattern, TableName, string.Join(", ", fieldNames.ToArray()), string.Join(", ", valueParameters.ToArray()));
			}
			else
			{
				throw new InvalidOperationException("Can't parse this object to the database - there are no properties set");
			}
			return result;
		}


		/// <summary>
		/// Creates a DbCommand with an update command to update an existing row in the table, using the values in the specified expando.
		/// </summary>
		/// <param name="expando">The expando with the fields to update.</param>
		/// <param name="key">The key value to use for PrimarykeyField comparison.</param>
		/// <returns>ready to use DbCommand</returns>
		public virtual DbCommand CreateUpdateCommand(dynamic expando, object key)
		{
			return CreateUpdateWhereCommand(expando, string.Format("{0} = {1}", this.PrimaryKeyField, this.PrefixParameterName("0")), key);
		}


		/// <summary>
		/// Creates a DbCommand with an update command to update an existing row in the table, using the values in the specified expando.
		/// </summary>
		/// <param name="expando">The expando with the fields to update.</param>
		/// <param name="where">The where clause. Default is empty string. Parameters have to be numbered starting with 0, for each value in args.</param>
		/// <param name="args">The parameter values to use.</param>
		/// <returns>
		/// ready to use DbCommand
		/// </returns>
		/// <exception cref="System.InvalidOperationException">No parsable object was sent in - could not define any name/value pairs</exception>
		public virtual DbCommand CreateUpdateWhereCommand(dynamic expando, string where = "", params object[] args)
		{
			var fieldSetFragments = new List<string>();
			var updateQueryPattern = this.GetUpdateQueryPattern();
			updateQueryPattern += ReadifyWhereClause(where);
			var result = CreateCommand(updateQueryPattern, null, args);
			int counter = args.Length > 0 ? args.Length : 0;
			foreach(var item in (IDictionary<string, object>)expando)
			{
				var val = item.Value;
				if(!item.Key.Equals(PrimaryKeyField, StringComparison.OrdinalIgnoreCase))
				{
					if(item.Value == null)
					{
						fieldSetFragments.Add(string.Format("{0} = NULL", item.Key));
					}
					else
					{
						result.AddParam(val);
						fieldSetFragments.Add(string.Format("{0} = {1}", item.Key, this.PrefixParameterName(counter.ToString())));
						counter++;
					}
				}
			}
			if(fieldSetFragments.Count > 0)
			{
				result.CommandText = string.Format(updateQueryPattern, TableName, string.Join(", ", fieldSetFragments.ToArray()));
			}
			else
			{
				throw new InvalidOperationException("No parsable object was sent in - could not define any name/value pairs");
			}
			return result;
		}


		/// <summary>
		/// Creates a DbCommand with a delete statement to delete one or more records from the DB according to the passed-in where clause/key value. 
		/// </summary>
		/// <param name="where">The where clause. Can be empty. Ignored if key is set.</param>
		/// <param name="key">The key. Value to compare with the PrimaryKeyField. If null, <see cref="where"/> is used as the where clause.</param>
		/// <param name="args">The parameter values.</param>
		/// <returns>ready to use DbCommand</returns>
		public virtual DbCommand CreateDeleteCommand(string where = "", object key = null, params object[] args)
		{
			var sql = string.Format(this.GetDeleteQueryPattern(), TableName);
			if(key == null)
			{
				sql += ReadifyWhereClause(where);
			}
			else
			{
				sql += string.Format("WHERE {0}={1}", this.PrimaryKeyField, this.PrefixParameterName("0"));
				args = new[] { key };
			}
			return CreateCommand(sql, null, args);
		}


		/// <summary>
		/// Hook, called when IsValid is called
		/// </summary>
		/// <param name="item">The item to validate.</param>
		public virtual void Validate(dynamic item) { }
		/// <summary>
		/// Hook, called after item has been inserted.
		/// </summary>
		/// <param name="item">The item inserted.</param>
		public virtual void Inserted(dynamic item) { }
		/// <summary>
		/// Hook, called after item has been updated
		/// </summary>
		/// <param name="item">The item updated.</param>
		public virtual void Updated(dynamic item) { }
		/// <summary>
		/// Hook, called after item has been deleted.
		/// </summary>
		/// <param name="item">The item deleted.</param>
		public virtual void Deleted(dynamic item) { }
		/// <summary>
		/// Hook, called before item will be deleted.
		/// </summary>
		/// <param name="item">The item to be deleted.</param>
		/// <returns>true if delete can proceed, false if it can't</returns>
		public virtual bool BeforeDelete(dynamic item) { return true; }
		/// <summary>
		/// Hook, called before item will be saved
		/// </summary>
		/// <param name="item">The item to save.</param>
		/// <returns>true if save can proceed, false if it can't</returns>
		public virtual bool BeforeSave(dynamic item) { return true; }
		/// <summary>
		/// Partial method which, when implemented offers ways to set DbCommand specific properties, which are specific for a given ADO.NET provider. 
		/// </summary>
		/// <param name="toAlter">the command object to alter the properties of</param>
		partial void SetCommandSpecificProperties(DbCommand toAlter);


		/// <summary>
		/// Creates a new DbCommand from the sql statement specified and assigns it to the connection specified. 
		/// </summary>
		/// <param name="sql">The SQL statement to create the command for.</param>
		/// <param name="conn">The connection to assign the command to.</param>
		/// <param name="args">The parameter values.</param>
		/// <returns>new DbCommand, ready to rock</returns>
		public DbCommand CreateCommand(string sql, DbConnection conn, params object[] args)
		{
			var result = _factory.CreateCommand();
			if(result != null)
			{
				SetCommandSpecificProperties(result);
				result.Connection = conn;
				result.CommandText = sql;
				result.AddParams(args);
			}
			return result;
		}


		/// <summary>
		/// Creates a new DbCommand from the sql statement specified, with support for parameter names and directions
		/// </summary>
		/// <param name="sql">sql to execute (typically just the Procedure or Function name, when isProcedure=true)</param>
		/// <param name="conn">The connection to assign the command to.</param>
		/// <param name="inParams">Object containing input parameter name:value pairs</param>
		/// <param name="outParams">Object containing output parameter name:value pairs</param>
		/// <param name="ioParams">Object containing input-output parameter name:value pairs</param>
		/// <param name="returnParams">Object containing return parameter name:value pairs</param>
		/// <param name="isProcedure">Whether to execute the command as stored procedure or general SQL.</param>
		/// <param name="connection">The connection to use, has to be open.</param>
		/// <returns>Ready to use DbCommand</returns>
		public DbCommand CreateCommandWithNamedParams(string sql, object inParams, object outParams, object ioParams, object returnParams, bool isProcedure, DbConnection connection)
		{
			DbCommand cmd = CreateCommand(sql, connection);
			if(isProcedure) cmd.CommandType = CommandType.StoredProcedure;
			cmd.AddNamedParams(inParams, ParameterDirection.Input);
			cmd.AddNamedParams(outParams, ParameterDirection.Output);
			cmd.AddNamedParams(ioParams, ParameterDirection.InputOutput);
			cmd.AddNamedParams(returnParams, ParameterDirection.ReturnValue);
			return cmd;
		}


		/// <summary>
		/// Performs the insert action of the dynamic specified using the connection specified. Expects the connection to be open.
		/// </summary>
		/// <param name="connection">The connection to use, has to be open.</param>
		/// <param name="transactionToUse">The transaction to use, can be null.</param>
		/// <param name="toInsert">The dynamic to insert. Is used to create the sql queries</param>
		private void PerformInsert(DbConnection connection, DbTransaction transactionToUse, dynamic toInsert)
		{
			if(_sequenceValueCallsBeforeMainInsert && !string.IsNullOrEmpty(_primaryKeyFieldSequence))
			{
				var sequenceCmd = CreateCommand(this.GetIdentityRetrievalScalarStatement(), connection);
				sequenceCmd.Transaction = transactionToUse;
				((IDictionary<string, object>)toInsert)[this.PrimaryKeyField] = Convert.ToInt32(sequenceCmd.ExecuteScalar());
			}
			DbCommand cmd = CreateInsertCommand(toInsert);
			cmd.Connection = connection;
			cmd.Transaction = transactionToUse;
			if(_sequenceValueCallsBeforeMainInsert || string.IsNullOrEmpty(_primaryKeyFieldSequence))
			{
				cmd.ExecuteNonQuery();
			}
			else
			{
				// simply batch the identity scalar query to the main insert query and execute them as one scalar query. This will both execute the statement and return the sequence value
				cmd.CommandText += ";" + this.GetIdentityRetrievalScalarStatement();
				((IDictionary<string, object>)toInsert)[this.PrimaryKeyField] = Convert.ToInt32(cmd.ExecuteScalar());
			}
		}


		/// <summary>
		/// Performs the save of the elements in toSave for the Save() and SaveAsNew() methods. 
		/// </summary>
		/// <param name="allSavesAreInserts">if set to <c>true</c> it will simply save all elements in toSave using insert queries</param>
		/// <param name="toSave">The elements to save.</param>
		/// <returns>the sum of the values returned by the database when executing each command.</returns>
		private int PerformSave(bool allSavesAreInserts, params object[] toSave)
		{
			var result = 0;
			using(var connection = OpenConnection())
			{
				using(var transactionToUse = connection.BeginTransaction())
				{
					foreach(var o in toSave)
					{
						var oAsExpando = o.ToExpando();
						if(BeforeSave(oAsExpando))
						{
							if(!allSavesAreInserts && HasPrimaryKey(o))
							{
								// update
								result += ExecuteDbCommand(CreateUpdateCommand(oAsExpando, GetPrimaryKey(o)), connection, transactionToUse);
								Updated(oAsExpando);
							}
							else
							{
								// insert
								PerformInsert(connection, transactionToUse, oAsExpando);
								Inserted(oAsExpando);
								result++;
							}
						}
					}
					transactionToUse.Commit();
				}
				connection.Close();
			}
			return result;
		}


		/// <summary>
		/// Executes the database command specified
		/// </summary>
		/// <param name="cmd">The command.</param>
		/// <param name="connection">The connection to use, has to be open.</param>
		/// <param name="transactionToUse">The transaction to use, can be null.</param>
		/// <returns></returns>
		private int ExecuteDbCommand(DbCommand cmd, DbConnection connection, DbTransaction transactionToUse)
		{
			cmd.Connection = connection;
			cmd.Transaction = transactionToUse;
			return cmd.ExecuteNonQuery();
		}


		/// <summary>
		/// Checks whether there's a column in the table schema of this dynamic model which has the same name as the columnname specified, using a culture invariant, case insensitive
		/// comparison
		/// </summary>
		/// <param name="columnName">Name of the column.</param>
		/// <returns></returns>
		private bool ColumnExists(string columnName)
		{
			return this.Schema.Any(c => string.Compare(this.GetColumnName(c), columnName, StringComparison.InvariantCultureIgnoreCase) == 0);
		}


		/// <summary>
		/// Builds the paged result.
		/// </summary>
		/// <param name="sql">The SQL statement to build the query pair for. Can be left empty, in which case the table name from the schema is used</param>
		/// <param name="primaryKeyField">The primary key field. Used for ordering. If left empty the defined PK field is used</param>
		/// <param name="whereClause">The where clause. Default is empty string.</param>
		/// <param name="orderByClause">The order by clause. Default is empty string.</param>
		/// <param name="columns">The columns to use in the project. Default is '*' (all columns, in table defined order).</param>
		/// <param name="pageSize">Size of the page. Default is 20</param>
		/// <param name="currentPage">The current page. 1-based. Default is 1.</param>
		/// <param name="args">The values to use as parameters.</param>
		/// <returns>The result of the paged query. Result properties are Items, TotalPages, and TotalRecords.</returns>
		private dynamic BuildPagedResult(string sql = "", string primaryKeyField = "", string whereClause = "", string orderByClause = "", string columns = "*", int pageSize = 20,
										 int currentPage = 1, params object[] args)
		{
			var queryPair = this.BuildPagingQueryPair(sql, primaryKeyField, whereClause, orderByClause, columns, pageSize, currentPage);
			dynamic result = new ExpandoObject();
			result.TotalRecords = Scalar(queryPair.CountQuery, args);
			result.TotalPages = result.TotalRecords / pageSize;
			if(result.TotalRecords % pageSize > 0)
			{
				result.TotalPages += 1;
			}
			result.Items = Query(string.Format(queryPair.MainQuery, columns, TableName), args);
			return result;
		}


		/// <summary>
		/// Builds the select query pattern using the where, orderby and limit specified. 
		/// </summary>
		/// <param name="whereClause">The where.</param>
		/// <param name="orderByClause">The order by.</param>
		/// <param name="limit">The limit.</param>
		/// <returns>Select statement pattern with {0} and {1} ready to be filled with projection list and source.</returns>
		private string BuildSelectQueryPattern(string whereClause, string orderByClause, int limit)
		{
			return this.GetSelectQueryPattern(limit, ReadifyWhereClause(whereClause), ReadifyOrderByClause(orderByClause));
		}


		/// <summary>
		/// Gets the pk comparison predicate query fragment, which is PrimaryKeyField = [parameter]
		/// </summary>
		/// <returns>ready to use predicate which assumes parameter to use for value is the first parameter</returns>
		private string GetPkComparisonPredicateQueryFragment()
		{
			return string.Format("{0} = {1}", this.PrimaryKeyField, this.PrefixParameterName("0"));
		}



		/// <summary>
		/// Gets the column definition of the column specified. This is a dynamic which contains all the fields of the schema row obtained for this table. 
		/// </summary>
		/// <param name="columnName">Name of the column.</param>
		/// <returns></returns>
		private dynamic GetColumn(string columnName)
		{
			return this.Schema.FirstOrDefault(c => string.Compare(this.GetColumnName(c), columnName, StringComparison.InvariantCultureIgnoreCase) == 0);
		}


		/// <summary>
		/// Readifies the where clause specified. If a non-empty/whitespace string is specified, it will make sure it's prefixed with " WHERE" including a prefix space.
		/// </summary>
		/// <param name="rawWhereClause">The raw where clause.</param>
		/// <returns>processed rawWhereClause which will guaranteed contain " WHERE" including prefix space.</returns>
		private string ReadifyWhereClause(string rawWhereClause)
		{
			return ReadifyClause(rawWhereClause, "WHERE");
		}


		/// <summary>
		/// Readifies the orderby clause specified. If a non-empty/whitespace string is specified, it will make sure it's prefixed with " ORDER BY" including a prefix space.
		/// </summary>
		/// <param name="rawOrderByClause">The raw order by clause.</param>
		/// <returns>
		/// processed rawOrderByClause which will guaranteed contain " ORDER BY" including prefix space.
		/// </returns>
		private string ReadifyOrderByClause(string rawOrderByClause)
		{
			return ReadifyClause(rawOrderByClause, "ORDER BY");
		}


		/// <summary>
		/// Readifies the where clause specified. If a non-empty/whitespace string is specified, it will make sure it's prefixed with the specified operator including a prefix space.
		/// </summary>
		/// <param name="rawClause">The raw clause.</param>
		/// <param name="op">The operator, e.g. "WHERE" or "ORDER BY".</param>
		/// <returns>
		/// processed rawClause which will guaranteed start with op including prefix space.
		/// </returns>
		private string ReadifyClause(string rawClause, string op)
		{
			var toReturn = string.Empty;
			if(rawClause == null)
			{
				return toReturn;
			}
			toReturn = rawClause.Trim();
			if(!string.IsNullOrWhiteSpace(toReturn))
			{
				if(toReturn.StartsWith(op, StringComparison.OrdinalIgnoreCase))
				{
					toReturn = " " + toReturn;
				}
				else
				{
					toReturn = string.Format(" {0} {1}", op, toReturn);
				}
			}
			return toReturn;
		}


		/// <summary>
		/// Processes the name of the table specified in the CTor into multiple elements, if applicable. 
		/// </summary>
		private void ProcessTableName()
		{
			this.TableNameWithoutSchema = this.TableName;
			this.SchemaName = string.Empty;
			if(string.IsNullOrWhiteSpace(this.TableName))
			{
				return;
			}
			var fragments = this.TableName.Split('.');
			if(fragments.Length == 1)
			{
				this.TableNameWithoutSchema = fragments[0];
			}
			else
			{
				this.SchemaName = fragments[fragments.Length - 2];
				this.TableNameWithoutSchema = fragments[fragments.Length - 1];
			}
		}

		#region Obsolete methods. Do not use
		/// <summary>
		/// Builds a set of Insert and Update commands based on the passed-on objects. These objects can be POCOs, Anonymous, NameValueCollections, or Expandos. Objects
		/// With a PK property (whatever PrimaryKeyField is set to) will be created at UPDATEs
		/// </summary>
		[Obsolete("Starting with the version released on 23-jan-2016, this method is obsolete, as it doesn't create an insert statement with a sequence.", true)]
		public virtual List<DbCommand> BuildCommands(params object[] things)
		{
			var commands = new List<DbCommand>();
			foreach(var item in things)
			{
				commands.Add(HasPrimaryKey(item) ? CreateUpdateCommand(item.ToExpando(), GetPrimaryKey(item)) : CreateInsertCommand(item.ToExpando()));
			}
			return commands;
		}
		#endregion


		#region Properties
		/// <summary>
		/// List out all the schema bits for use with ... whatever
		/// </summary>
		public IEnumerable<dynamic> Schema
		{
			get
			{
				if(_schema == null)
				{
					_schema = PostProcessSchemaQuery(string.IsNullOrWhiteSpace(this.SchemaName) ? Query(this.TableWithoutSchemaQuery, this.TableName)
																		: Query(this.TableWithSchemaQuery, this.TableNameWithoutSchema, this.SchemaName));
				}
				return _schema;
			}
		}


		/// <summary>
		/// Creates an empty Expando set with defaults from the DB. The default values are in string format.
		/// </summary>
		public dynamic Prototype
		{
			get
			{
				dynamic result = new ExpandoObject();
				var dc = (IDictionary<string, object>)result;
				var schema = this.Schema;
				foreach(dynamic column in schema)
				{
					var columnName = this.GetColumnName(column);
					dc.Add(columnName, this.DefaultValue(columnName));
				}
				result._Table = this;
				return result;
			}
		}


		/// <summary>
		/// Gets the name of the schema. This name is obtained from <see cref="TableName"/>. If TableName doesn't contain
		/// a schema, this property is the empty string.
		/// </summary>
		public virtual string SchemaName { get; private set; }
		/// <summary>
		/// Gets or sets the table name without schema. This name is identical to <see cref="TableName"/> if that name doesn't contain any schema information. By default
		/// Massive splits the TableName on '.'.
		/// </summary>
		public virtual string TableNameWithoutSchema { get; private set; }
		/// <summary>
		/// Gets or sets the name of the table this dynamicmodel is represented by.
		/// </summary>
		public virtual string TableName { get; set; }
		/// <summary>
		/// Gets or sets the primary key field. If empty, "ID" is used.
		/// </summary>
		public virtual string PrimaryKeyField { get; set; }
		/// <summary>
		/// Gets or sets the descriptor field name, which is useful if the table is a lookup table. Descriptor field is the field containing the textual representation of the value
		/// in PrimaryKeyField.
		/// </summary>
		public string DescriptorField { get; protected set; }
		/// <summary>
		/// Contains the error messages collected since the last Validate.
		/// </summary>
		public IList<string> Errors { get; protected set; }
		/// <summary>
		/// Gets or sets the connection string used. By default, it's read from the config file. If no config file can be used, set the connection string using this property prior to 
		/// executing any query.
		/// </summary>
		public string ConnectionString
		{
			get { return _connectionString; }
			set { _connectionString = value; }
		}
		#endregion
	}


	/// <summary>
	/// Interface for specifying ado.net provider name and connection string. Used to create custom connection string/ado.net factory name providers 
	/// for sources other than .config files, e.g. with usage in ASPNET5
	/// </summary>
	public interface IConnectionStringProvider
	{
		/// <summary>
		/// Gets the name of the provider which is the name of the DbProviderFactory specified in the connection string stored under the name specified.
		/// </summary>
		/// <param name="connectionStringName">Name of the connection string.</param>
		/// <returns></returns>
		string GetProviderName(string connectionStringName);
		/// <summary>
		/// Gets the connection string stored under the name specified
		/// </summary>
		/// <param name="connectionStringName">Name of the connection string.</param>
		/// <returns></returns>
		string GetConnectionString(string connectionStringName);
	}


	/// <summary>
	/// Default implementation of IConnectionStringProvider which uses config files for its source.
	/// </summary>
	/// <seealso cref="Massive.IConnectionStringProvider" />
	public class ConfigurationBasedConnectionStringProvider : IConnectionStringProvider
	{
		/// <summary>
		/// Gets the name of the provider which is the name of the DbProviderFactory specified in the connection string stored under the name specified.
		/// </summary>
		/// <param name="connectionStringName">Name of the connection string.</param>
		/// <returns></returns>
		public string GetProviderName(string connectionStringName)
		{
			var providerName = ConfigurationManager.ConnectionStrings[connectionStringName].ProviderName;
			return !string.IsNullOrWhiteSpace(providerName) ? providerName : null;
		}

		/// <summary>
		/// Gets the connection string stored under the name specified
		/// </summary>
		/// <param name="connectionStringName">Name of the connection string.</param>
		/// <returns></returns>
		public string GetConnectionString(string connectionStringName)
		{
			return ConfigurationManager.ConnectionStrings[connectionStringName].ConnectionString;
		}
	}
}
