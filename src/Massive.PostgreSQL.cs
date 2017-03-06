///////////////////////////////////////////////////////////////////////////////////////////////////
// Massive v2.0. PostgreSql specific code. 
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
using System.Data;
using System.Data.Common;
using System.Dynamic;
using System.Linq;
using System.Reflection;
using System.Text;

namespace Massive
{
	// Cursor dereferencing data reader, which may go back into Npgsql at some point
	public class NpgsqlDereferencingReader : IDataReader
	{
		// TO DO: FetchCount to int property on NpgsqlCommand - and check (and probably use) the equivalent property name in JDBC
		private readonly int FetchCount = 10000;

		private DbDataReader Reader = null; // reader for current FETCH
		private List<string> Cursors = new List<string>();
		private int Index = 0;
		private string Cursor = null;
		private int Count; // # read so far for current FETCH
		private DbConnection Connection;
		private DynamicModel Db;

		/// <summary>
		/// Create a safe, sensible dereferencing reader; we have already checked that there are at least some cursors to dereference at this point.
		/// </summary>
		/// <param name="reader">The reader for the undereferenced query.</param>
		/// <param name="connection">The connection to use.</param>
		/// <param name="db">We need this just for the DbCommand factory.</param>
		internal NpgsqlDereferencingReader(DbDataReader reader, DbConnection connection, DynamicModel db)
		{
			Connection = connection;
			Db = db;

			// Supports 1x1 1xN Nx1 and NXM patterns of cursor data.
			// If just some are cursors we follow a pre-existing pattern set by the Oracle drivers, and dereference what we can.
			while (reader.Read())
			{
				for (int i = 0; i < reader.FieldCount; i++)
				{
					if (reader.GetDataTypeName(i) == "refcursor")
					{
						// cursor name can potentially contain " so stop that breaking us
						Cursors.Add(reader.GetString(i).Replace(@"""", @""""""));
					}
				}
			}
			reader.Close();
			reader.Dispose();

			NextResult();
		}

		/// <summary>
		/// Close current FETCH cursor
		/// </summary>
		/// <param name="ExecuteNow">Iff false then return the SQL but don't execute the command</param>
		/// <returns>The SQL to close the cursor, if there is one and this has not already been executed.</returns>
		private string CloseCursor(bool ExecuteNow = true)
		{
			// close and dispose current fetch reader for this cursor
			if (Reader != null)
			{
				Reader.Close();
				Reader.Dispose();
				// not nulling Reader so that it still exists to pass on all the other override calls;
				// seems okay to close/dispose multiple times, if not we could not null Reader but add code to avoid this
			}
			// close cursor itself
			if (!string.IsNullOrEmpty(Cursor))
			{
				var closeSql = string.Format(@"CLOSE ""{0}"";", Cursor);
				if (!ExecuteNow) return closeSql;
				var closeCmd = Db.CreateCommand(closeSql, Connection); // new NpgsqlCommand(..., Connection);
				closeCmd.ExecuteNonQuery();
				closeCmd.Dispose();
				Cursor = null;
			}
			return "";
		}

		/// <summary>
		/// Fetch next N rows from current cursor
		/// </summary>
		/// <param name="closeSql">SQL to prepend, to close the previous cursor in a single round trip (optional)</param>
		private void FetchNextNFromCursor(string closeSql = "")
		{
			// close and dispose previous fetch reader for this cursor
			if (Reader != null)
			{
				Reader.Close();
				Reader.Dispose();
			}
			// fetch next n from cursor (optionally close previous cursor first)
			var fetchCmd = Db.CreateCommand(closeSql + string.Format(@"FETCH {0} FROM ""{1}"";", FetchCount, Cursor), Connection); // new NpgsqlCommand(..., Connection);
			Reader = fetchCmd.ExecuteReader(CommandBehavior.SingleResult);
			Count = 0;
		}

#region IDataReader interface
		public object this[string name] { get { return Reader[name]; } }
		public object this[int i] { get { return Reader[i]; } }
		public int Depth { get { return Reader.Depth; } }
		public int FieldCount { get { return Reader.FieldCount; } }
		public bool IsClosed { get { return Reader.IsClosed; } }
		public int RecordsAffected { get { return Reader.RecordsAffected; } }

		public void Close()
		{
			CloseCursor();
		}

		public void Dispose()
		{
			CloseCursor();
		}

		public bool GetBoolean(int i) { return Reader.GetBoolean(i); }
		public byte GetByte(int i) { return Reader.GetByte(i); }
		public long GetBytes(int i, long fieldOffset, byte[] buffer, int bufferoffset, int length) { return Reader.GetBytes(i, fieldOffset, buffer, bufferoffset, length); }
		public char GetChar(int i) { return Reader.GetChar(i); }
		public long GetChars(int i, long fieldoffset, char[] buffer, int bufferoffset, int length) { return Reader.GetChars(i, fieldoffset, buffer, bufferoffset, length); }
		public IDataReader GetData(int i) { return Reader.GetData(i); }
		public string GetDataTypeName(int i) { return Reader.GetDataTypeName(i); }
		public DateTime GetDateTime(int i) { return Reader.GetDateTime(i); }
		public decimal GetDecimal(int i) { return Reader.GetDecimal(i); }
		public double GetDouble(int i) { return Reader.GetDouble(i); }
		public Type GetFieldType(int i) { return Reader.GetFieldType(i); }
		public float GetFloat(int i) { return Reader.GetFloat(i); }
		public Guid GetGuid(int i) { return Reader.GetGuid(i); }
		public short GetInt16(int i) { return Reader.GetInt16(i); }
		public int GetInt32(int i) { return Reader.GetInt32(i); }
		public long GetInt64(int i) { return Reader.GetInt64(i); }
		public string GetName(int i) { return Reader.GetName(i); }
		public int GetOrdinal(string name) { return Reader.GetOrdinal(name); }
		public DataTable GetSchemaTable() { return Reader.GetSchemaTable(); }
		public string GetString(int i) { return Reader.GetString(i); }
		public object GetValue(int i) { return Reader.GetValue(i); }
		public int GetValues(object[] values) { return Reader.GetValues(values); }
		public bool IsDBNull(int i) { return Reader.IsDBNull(i); }

		public bool NextResult()
		{
			if (Index >= Cursors.Count)
			{
				CloseCursor();
				return false;
			}
			var closeSql = CloseCursor(false);
			Cursor = Cursors[Index++];
			FetchNextNFromCursor(closeSql);
			return true;
		}

		public bool Read()
		{
			if (Reader != null)
			{
				bool cursorHasNextRow = Reader.Read();
				if (cursorHasNextRow)
				{
					Count++;
					return true;
				}
				// if rows expired before count we asked for, there is nothing more to fetch on this cursor
				if (Count < FetchCount) return false;
			}
			// if rows expired at count we asked for, there may or may not be more rows
			FetchNextNFromCursor();
			// recursive self-call
			return Read();
		}
#endregion
	}

	/// <summary>
	/// Class which provides extension methods for various ADO.NET objects.
	/// </summary>
	public static partial class ObjectExtensions
	{
		private static bool CanDereference(this DbDataReader reader)
		{
			bool hasCursors = false;
			for (int i = 0; i < reader.FieldCount; i++)
			{
				if (reader.GetDataTypeName(i) == "refcursor")
				{
					hasCursors = true;
					break;
				}
			}
			return hasCursors;
		}

		/// <summary>
		/// Dereference cursors in exactly the way which used to be supported within Npgsql itself, but no longer is (see https://github.com/npgsql/npgsql/issues/438 )
		/// </summary>
		/// <param name="cmd">The command.</param>
		/// <param name="Connection">The connection - required for deferencing.</param>
		/// <param name="db">The parent DynamicModel (or subclass) - required to get at the factory for deferencing.</param>
		/// <returns>The reader, dereferenced if needed.</returns>
		/// <remarks>
		/// This is now an improvement of the previously removed Npgsql code - although it is the same basic idea.
		/// 
		/// After extensive discussion, it should be noted that this is a fully correct way to dereference cursors - even for extrememly large datasets.
		/// http://stackoverflow.com/questions/42292341/
		/// 
		/// Npgsql are also willing to see it committed back to their project.
		/// https://github.com/npgsql/npgsql/issues/438
		/// 
		///	If/when Npqsql permanently reintroduces cursor derefencing then calls to this method can be changed back to plain ExecuteReader() calls (but this will be harmless if it remains).
		///	
		/// `FETCH ALL FROM cursor` works correctly, but it causes PostgreSQL to pre-buffer all the data requested server-side.
		/// (as do *all* PostgreSQL cursor FETCH requests: REF).
		/// We recommend using this automatic dereferencing support for small cursor accesses
		/// (why? mainly because Oracle and SQL Server support exactly this dereferencing pattern)
		/// and coding your own `FETCH n FROM cursor` calls if you are fetching large data.
		/// `ExecuteNonQuery() is the pattern used in both the other databases to obtain the cursor refs
		/// themselves, rather than the data which they refer to.
		/// </remarks>
		public static IDataReader ExecuteDereferencingReader(this DbCommand cmd, DbConnection Connection, DynamicModel db, bool DereferenceCursors = true)
		{
#if false
			//// ORIGINAL CODE
			var reader = cmd.ExecuteReader(); // var reader = Execute(behavior);

			// Transparently dereference cursors returned from functions		
			//////if (cmd.CommandType == CommandType.StoredProcedure && // if (CommandType == CommandType.StoredProcedure &&
			if (reader.FieldCount == 1 &&
				reader.GetDataTypeName(0) == "refcursor")
			{
				var sb = new StringBuilder();
				while (reader.Read())
				{
					/////sb.AppendFormat(@"FETCH ALL FROM ""{0}"";", reader.GetString(0));
					sb.AppendFormat(@"FETCH ALL FROM ""{0}"";", reader.GetString(0).Replace(@"""", @""""""));
				}
				reader.Dispose();

				var dereferenceCmd = db.CreateCommand(sb.ToString(), Connection); // var dereferenceCmd = new NpgsqlCommand(sb.ToString(), Connection);
				return dereferenceCmd.ExecuteReader(); // return dereferenceCmd.ExecuteReader(behavior);
			}

			return reader;
#else
			var reader = cmd.ExecuteReader(); // var reader = Execute(behavior);

			// TO DO: DereferenceCursors as bool property on NpgsqlCommand?
			// Remarks: Don't consider dereferencing if no returned columns are cursors, but if just some are cursors then follow the pre-existing convention set by
			// the Oracle drivers and dereference what we can. The rest of the pattern is that we only ever try to dereference on Query and Scalar, never on Execute.
			if (DereferenceCursors && reader.CanDereference())
			{
				return new NpgsqlDereferencingReader(reader, Connection, db);
			}

			return reader;
#endif
		}


		/// <summary>
		/// Extension to set the parameter to the DB specific cursor type.
		/// </summary>
		/// <param name="p">The parameter.</param>
		/// <param name="value">Object reference to an existing cursor from a previous output or return direction cursor parameter, or null.</param>
		/// <returns>Returns false if not supported on this provider.</returns>
		public static bool SetCursor(this DbParameter p, object value)
		{
			p.SetRuntimeEnumProperty("NpgsqlDbType", "Refcursor");
			p.Value = value;
			return true;
		}


		/// <summary>
		/// Check whether the parameter is of the DB specific cursor type
		/// </summary>
		/// <param name="p">The parameter.</param>
		/// <returns>true if this is a cursor parameter.</returns>
		public static bool IsCursor(this DbParameter p)
		{
			return p.GetRuntimeEnumProperty("NpgsqlDbType") == "Refcursor";
		}


		/// <summary>
		/// Returns true if this command requires a wrapping transaction.
		/// </summary>
		/// <param name="cmd">The command.</param>
		/// <returns>true if it requires a wrapping transaction</returns>
		/// <remarks>
		/// Only relevant to Postgres cursor commands and in this case we do some relevant pre-processing of the command too.
		/// </remarks>
		public static bool RequiresWrappingTransaction(this DbCommand cmd)
		{
			// If we've got cursor parameters these are actually just placeholders to kick off cursor support (i.e. the wrapping transaction); we need to remove them before executing the command.
			bool isCursorCommand = false;
			cmd.Parameters.Cast<DbParameter>().Where(p => p.IsCursor()).ToList().ForEach(p => { isCursorCommand = true; cmd.Parameters.Remove(p); });
			return isCursorCommand;
		}


		/// <summary>
		/// Extension to set anonymous DbParameter
		/// </summary>
		/// <param name="p">The parameter.</param>
		/// <returns>Returns false if not supported on this provider.</returns>
		private static bool SetAnonymousParameter(this DbParameter p)
		{
			// pretty simple! but assume in principle more could be needed in some other provider
			p.ParameterName = "";
			return true;
		}


		/// <summary>
		/// Extension to check whether ADO.NET provider ignores output parameter types (no point requiring user to provide them if it does)
		/// </summary>
		/// <param name="p">The parameter.</param>
		/// <returns>True if output parameter type is ignored when generating output data types.</returns>
		private static bool IgnoresOutputTypes(this DbParameter p)
		{
			return true;
		}


		/// <summary>
		/// Extension to set ParameterDirection for single parameter, correcting for unexpected handling in specific ADO.NET providers.
		/// </summary>
		/// <param name="p">The parameter.</param>
		/// <param name="direction">The direction to set.</param>
		private static void SetDirection(this DbParameter p, ParameterDirection direction)
		{
			// Postgres/Npgsql specific fix: return params are always returned unchanged, return values are accessed using output params
			p.Direction = (direction == ParameterDirection.ReturnValue) ? ParameterDirection.Output : direction;
		}


		/// <summary>
		/// Extension to set Value (and implicitly DbType) for single parameter, adding support for provider unsupported types, etc.
		/// </summary>
		/// <param name="p">The parameter.</param>
		/// <param name="value">The non-null value to set. Nulls are handled in shared code.</param>
		private static void SetValue(this DbParameter p, object value)
		{
			if(value is Guid)
			{
				p.Value = value.ToString();
				p.Size = 36;
			}
			else
			{
				p.Value = value;
				var valueAsString = value as string;
				if(valueAsString != null)
				{
					p.Size = valueAsString.Length > 4000 ? -1 : 4000;
				}
			}
		}
	}

    /// <summary>
    /// A class that wraps your database table in Dynamic Funtime
    /// </summary>
    public partial class DynamicModel
    {
		#region Constants
		// Mandatory constants/variables every DB has to define. 
		/// <summary>
		/// The default sequence name for initializing the pk sequence name value in the ctor. 
		/// </summary>
		internal const string _defaultSequenceName = "";
		/// <summary>
		/// Flag to signal whether the sequence retrieval call (if any) is executed before the insert query (true) or after (false). Not a const, to avoid warnings. 
		/// </summary>
		private bool _sequenceValueCallsBeforeMainInsert = true;
		#endregion

		/// <summary>
		/// Gets a default value for the column as defined in the schema.
		/// </summary>
		/// <param name="column">The column.</param>
		/// <returns></returns>
		private dynamic GetDefaultValue(dynamic column)
		{
			string defaultValue = column.COLUMN_DEFAULT;
			if(string.IsNullOrEmpty(defaultValue))
			{
				return null;
			}
			dynamic result;
			switch(defaultValue)
			{
				case "current_date":
				case "(current_date)":
					result = DateTime.Now.Date;
					break;
				case "current_time":
				case "(current_time)":
					result = DateTime.Now.TimeOfDay;
					break;
				default:
					result = defaultValue.Replace("(", "").Replace(")", "");
					break;
			}
			return result;
		}

		
		/// <summary>
		/// Gets the aggregate function to use in a scalar query for the fragment specified
		/// </summary>
		/// <param name="aggregateCalled">The aggregate called on the dynamicmodel, which should be converted to a DB function. Expected to be lower case</param>
		/// <returns>the aggregate function to use, or null if no aggregate function is supported for aggregateCalled</returns>
		protected virtual string GetAggregateFunction(string aggregateCalled)
		{
			switch(aggregateCalled)
			{
				case "sum":
					return "SUM";
				case "max":
					return "MAX";
				case "min":
					return "MIN";
				case "avg":
					return "AVG";
				default:
					return null;
			}
		}
		

		/// <summary>
		/// Gets the sql statement to use for obtaining the identity/sequenced value of the last insert.
		/// </summary>
		/// <returns></returns>
		protected virtual string GetIdentityRetrievalScalarStatement()
		{
			return string.IsNullOrEmpty(_primaryKeyFieldSequence) ? string.Empty : string.Format("SELECT nextval('{0}')", _primaryKeyFieldSequence);
		}
		

		/// <summary>
		/// Gets the sql statement pattern for a count row query (count(*)). The pattern should include as place holders: {0} for source (FROM clause).
		/// </summary>
		/// <returns></returns>
		protected virtual string GetCountRowQueryPattern()
		{
			return "SELECT COUNT(*) FROM {0} ";
		}
		

		/// <summary>
		/// Gets the name of the parameter with the prefix to use in a query, e.g. @rawName or :rawName
		/// </summary>
		/// <param name="rawName">raw name of the parameter, without parameter prefix</param>
		/// <returns>rawName prefixed with the db specific prefix (if any)</returns>
		protected virtual string PrefixParameterName(string rawName)
		{
			return ":" + rawName;
		}


		/// <summary>
		/// Gets the select query pattern, to use for building select queries. The pattern should include as place holders: {0} for project list, {1} for the source (FROM clause).
		/// </summary>
		/// <param name="limit">The limit for the resultset. 0 means no limit.</param>
		/// <param name="whereClause">The where clause. Expected to have a prefix space if not empty</param>
		/// <param name="orderByClause">The order by clause. Expected to have a prefix space if not empty</param>
		/// <returns>
		/// string pattern which is usable to build select queries.
		/// </returns>
		protected virtual string GetSelectQueryPattern(int limit, string whereClause, string orderByClause)
		{
			return string.Format("SELECT {{0}} FROM {{1}}{0}{1}{2}", whereClause, orderByClause, limit > 0 ? " LIMIT " + limit : string.Empty);
		}


		/// <summary>
		/// Gets the insert query pattern, to use for building insert queries. The pattern should include as place holders: {0} for target, {1} for field list, {2} for parameter list
		/// </summary>
		/// <returns></returns>
		protected virtual string GetInsertQueryPattern()
		{
			return "INSERT INTO {0} ({1}) VALUES ({2})";
		}


		/// <summary>
		/// Gets the update query pattern, to use for building update queries. The pattern should include as placeholders: {0} for target, {1} for field list with sets. Has to have
		/// trailing space
		/// </summary>
		/// <returns></returns>
		protected virtual string GetUpdateQueryPattern()
		{
			return "UPDATE {0} SET {1} ";
		}


		/// <summary>
		/// Gets the delete query pattern, to use for building delete queries. The pattern should include as placeholders: {0} for the target. Has to have trailing space
		/// </summary>
		/// <returns></returns>
		protected virtual string GetDeleteQueryPattern()
		{
			return "DELETE FROM {0} ";
		}


		/// <summary>
		/// Gets the name of the column using the expando object representing the column from the schema
		/// </summary>
		/// <param name="columnFromSchema">The column from schema in the form of an expando.</param>
		/// <returns>the name of the column as defined in the schema</returns>
		protected virtual string GetColumnName(dynamic columnFromSchema)
		{
			return columnFromSchema.COLUMN_NAME;
		}


		/// <summary>
		/// Post-processes the query used to obtain the meta-data for the schema. If no post-processing is required, simply return a toList 
		/// </summary>
		/// <param name="toPostProcess">To post process.</param>
		/// <returns></returns>
		private IEnumerable<dynamic> PostProcessSchemaQuery(IEnumerable<dynamic> toPostProcess)
		{
			return toPostProcess == null ? new List<dynamic>() : toPostProcess.ToList();
		}


		/// <summary>
		/// Builds a paging query and count query pair. 
		/// </summary>
		/// <param name="sql">The SQL statement to build the query pair for. Can be left empty, in which case the table name from the schema is used</param>
		/// <param name="primaryKeyField">The primary key field. Used for ordering. If left empty the defined PK field is used</param>
		/// <param name="whereClause">The where clause. Default is empty string.</param>
		/// <param name="orderByClause">The order by clause. Default is empty string.</param>
		/// <param name="columns">The columns to use in the project. Default is '*' (all columns, in table defined order).</param>
		/// <param name="pageSize">Size of the page. Default is 20</param>
		/// <param name="currentPage">The current page. 1-based. Default is 1.</param>
		/// <returns>ExpandoObject with two properties: MainQuery for fetching the specified page and CountQuery for determining the total number of rows in the resultset</returns>
		private dynamic BuildPagingQueryPair(string sql = "", string primaryKeyField = "", string whereClause = "", string orderByClause = "", string columns = "*", int pageSize = 20,
											 int currentPage = 1)
		{
			var orderByClauseFragment = string.IsNullOrEmpty(orderByClause) ? string.Format(" ORDER BY {0}", string.IsNullOrEmpty(primaryKeyField) ? PrimaryKeyField : primaryKeyField)
																			: ReadifyOrderByClause(orderByClause);
			var coreQuery = string.Format(this.GetSelectQueryPattern(0, ReadifyWhereClause(whereClause), orderByClauseFragment), columns, string.IsNullOrEmpty(sql) ? this.TableName : sql);
			dynamic toReturn = new ExpandoObject();
			toReturn.CountQuery = string.Format("SELECT COUNT(*) FROM ({0}) q", coreQuery);
			var pageStart = (currentPage - 1) * pageSize;
			toReturn.MainQuery = string.Format("{0} LIMIT {1} OFFSET {2}", coreQuery, pageSize, (pageStart + pageSize));
			return toReturn;
		}


		#region Properties
		/// <summary>
		/// Provides the default DbProviderFactoryName to the core to create a factory on the fly in generic code.
		/// </summary>
		/// <remarks>If you're using an older version of Npgsql, be sure to manually register the DbProviderFactory in either the machine.config file or in your
		/// application's app/web.config file.</remarks>
		protected virtual string DbProviderFactoryName
		{
			get { return "Npgsql"; }
		}


		/// <summary>
		/// Gets the table schema query to use to obtain meta-data for a given table and schema
		/// </summary>
		protected virtual string TableWithSchemaQuery
		{
			get { return "SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = :0 AND TABLE_SCHEMA = :1"; }
		}

		/// <summary>
		/// Gets the table schema query to use to obtain meta-data for a given table which is specified as the single parameter.
		/// </summary>
		protected virtual string TableWithoutSchemaQuery
		{
			get { return "SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = :0"; }
		}
		#endregion
    }
}