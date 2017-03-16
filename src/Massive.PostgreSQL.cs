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
		private DbConnection Connection;
		private DynamicModel Db;
		private int FetchSize;

		private DbDataReader Reader = null; // current FETCH reader
		private List<string> Cursors = new List<string>();
		private int Index = 0;
		private string Cursor = null;
		private int Count; // # read on current FETCH

		/// <summary>
		/// Create a safe, sensible dereferencing reader; we have already checked that there are at least some cursors to dereference at this point.
		/// </summary>
		/// <param name="reader">The original reader for the undereferenced query.</param>
		/// <param name="connection">The connection to use.</param>
		/// <param name="db">We need this for the DbCommand factory.</param>
		/// <param name="fetchSize">Batch fetch size; zero or negative value will FETCH ALL from each cursor.</param>
		/// <remarks>
		/// FETCH ALL is genuinely useful in some situations (e.g. if using (abusing?) cursors to return small or medium sized multiple result
		/// sets then we can and do save one round trip to the database overall: n cursors round trips, rather than n cursors plus one), but since
		/// it is badly problematic in the case of large cursors we force the user to request it explicitly.
		/// https://github.com/npgsql/npgsql/issues/438
		/// http://stackoverflow.com/questions/42292341/
		/// </remarks>
		internal NpgsqlDereferencingReader(DbDataReader reader, DbConnection connection, DynamicModel db, int fetchSize)
		{
			FetchSize = fetchSize;
			Connection = connection;
			Db = db;

			// Supports 1x1 1xN Nx1 and NXM patterns of cursor data.
			// If just some values are cursors we follow the pre-existing pattern set by the Oracle drivers, and dereference what we can.
			while(reader.Read())
			{
				for(int i = 0; i < reader.FieldCount; i++)
				{
					if(reader.GetDataTypeName(i) == "refcursor")
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
		/// SQL to fetch required count from current cursor
		/// </summary>
		/// <returns>SQL</returns>
		private string FetchSQL()
		{
			return string.Format(@"FETCH {0} FROM ""{1}"";", (FetchSize <= 0 ? "ALL" : FetchSize.ToString()), Cursor);
		}

		/// <summary>
		/// SQL to close current cursor
		/// </summary>
		/// <returns>SQL</returns>
		private string CloseSQL()
		{
			return string.Format(@"CLOSE ""{0}"";", Cursor);
		}

		/// <summary>
		/// Close current FETCH cursor on the database
		/// </summary>
		/// <param name="ExecuteNow">Iff false then return the SQL but don't execute the command</param>
		/// <returns>The SQL to close the cursor, if there is one and this has not already been executed.</returns>
		private string CloseCursor(bool ExecuteNow = true)
		{
			// close and dispose current fetch reader for this cursor
			if(Reader != null && !Reader.IsClosed)
			{
				Reader.Close();
				Reader.Dispose();
			}
			// close cursor itself
			if(FetchSize > 0 && !string.IsNullOrEmpty(Cursor))
			{
				var closeSql = CloseSQL();
				if(!ExecuteNow)
				{
					return closeSql;
				}
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
		/// <param name="closePreviousSQL">SQL to prepend, to close the previous cursor in a single round trip (optional)</param>
		private void FetchNextNFromCursor(string closePreviousSQL = "")
		{
			// close and dispose previous fetch reader for this cursor
			if(Reader != null && !Reader.IsClosed)
			{
				Reader.Close();
				Reader.Dispose();
			}
			// fetch next n from cursor;
			// optionally close previous cursor;
			// iff we're fetching all, we can close this cursor in this command
			var fetchCmd = Db.CreateCommand(closePreviousSQL + FetchSQL() + (FetchSize <= 0 ? CloseSQL() : ""), Connection); // new NpgsqlCommand(..., Connection);
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
			var closeSql = CloseCursor(Index >= Cursors.Count);
			if(Index >= Cursors.Count)
			{
				return false;
			}
			Cursor = Cursors[Index++];
			FetchNextNFromCursor(closeSql);
			return true;
		}

		public bool Read()
		{
			if(Reader != null)
			{
				bool cursorHasNextRow = Reader.Read();
				if(cursorHasNextRow)
				{
					Count++;
					return true;
				}
				// if we did FETCH ALL or if rows expired before requested count, there is nothing more to fetch on this cursor
				if(FetchSize <= 0 || Count < FetchSize)
				{
					return false;
				}
			}
			// if rows expired at requested count, there may or may not be more rows
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
		/// <summary>
		/// True iff current reader has cursors in its output types.
		/// </summary>
		/// <param name="reader">The reader to check</param>
		/// <returns>Are there cursors?</returns>
		/// <remarks>Part of NpgsqlDereferencingReader</remarks>
		private static bool CanDereference(this IDataReader reader)
		{
			bool hasCursors = false;
			for(int i = 0; i < reader.FieldCount; i++)
			{
				if(reader.GetDataTypeName(i) == "refcursor")
				{
					hasCursors = true;
					break;
				}
			}
			return hasCursors;
		}


		/// <summary>
		/// Dereference cursors in more or less the way which used to be supported within Npgsql itself, only now considerably improved from that removed, partial support.
		/// </summary>
		/// <param name="cmd">The command.</param>
		/// <param name="Connection">The connection - required for deferencing.</param>
		/// <param name="db">The parent DynamicModel (or subclass) - required to get at the factory for deferencing and config vaules.</param>
		/// <returns>The reader, dereferenced if needed.</returns>
		/// <remarks>
		/// https://github.com/npgsql/npgsql/issues/438
		/// http://stackoverflow.com/questions/42292341/
		/// </remarks>
		internal static IDataReader ExecuteDereferencingReader(this DbCommand cmd, DbConnection Connection, DynamicModel db)
		{
			var reader = cmd.ExecuteReader(); // var reader = Execute(behavior);

			// Remarks: Do not consider dereferencing if no returned columns are cursors, but if just some are cursors then follow the pre-existing convention set by
			// the Oracle drivers and dereference what we can. The rest of the pattern is that we only ever try to dereference on Query and Scalar, never on Execute.
			if(db.AutoDereferenceCursors && reader.CanDereference())
			{
				return new NpgsqlDereferencingReader(reader, Connection, db, db.AutoDereferenceFetchSize);
			}

			return reader;
		}


		/// <summary>
		/// Returns true if this command requires a wrapping transaction.
		/// </summary>
		/// <param name="cmd">The command.</param>
		/// <param name="db">The dynamic model, to access config params.</param>
		/// <returns>true if it requires a wrapping transaction</returns>
		/// <remarks>
		/// Only relevant to Postgres cursor dereferencing and in this case we also do some relevant pre-processing of the command.
		/// </remarks>
		internal static bool RequiresWrappingTransaction(this DbCommand cmd, DynamicModel db)
		{
			if (!db.AutoDereferenceCursors)
			{
				// Do not request wrapping transaction if auto-dereferencing is off
				return false;
			}
			// If we've got cursor parameters these are actually just placeholders to kick off cursor support (i.e. the wrapping transaction); we need to remove them before executing the command.
			bool isCursorCommand = false;
			cmd.Parameters.Cast<DbParameter>().Where(p => p.IsCursor()).ToList().ForEach(p => { isCursorCommand = true; cmd.Parameters.Remove(p); });
			return isCursorCommand;
		}


		/// <summary>
		/// Extension to set the parameter to the DB specific cursor type.
		/// </summary>
		/// <param name="p">The parameter.</param>
		/// <param name="value">Object reference to an existing cursor from a previous output or return direction cursor parameter, or null.</param>
		/// <returns>Returns false if not supported on this provider.</returns>
		private static bool SetCursor(this DbParameter p, object value)
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
		private static bool IsCursor(this DbParameter p)
		{
			return p.GetRuntimeEnumProperty("NpgsqlDbType") == "Refcursor";
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
			p.Value = value;
			var valueAsString = value as string;
			if(valueAsString != null)
			{
				p.Size = valueAsString.Length > 4000 ? -1 : 4000;
			}
		}


		/// <summary>
		/// Extension to get the output Value from single parameter, adding support for provider unsupported types, etc.
		/// </summary>
		/// <param name="p">The parameter.</param>
		private static object GetValue(this DbParameter p)
		{
			return p.Value;
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
			return "SELECT COUNT(*) FROM {0}";
		}


		/// <summary>
		/// Gets the name of the parameter with the prefix to use in a query, e.g. @rawName or :rawName
		/// </summary>
		/// <param name="rawName">raw name of the parameter, without parameter prefix</param>
		/// <returns>rawName prefixed with the db specific prefix (if any)</returns>
		internal static string PrefixParameterName(string rawName, DbCommand cmd = null)
		{
			return (cmd != null) ? rawName : (":" + rawName);
		}


		/// <summary>
		/// Gets the name of the parameter without the prefix, to use in results
		/// </summary>
		/// <param name="rawName">The name of the parameter, prefixed if we prefixed it above</param>
		/// <returns>raw name</returns>
		internal static string DeprefixParameterName(string dbParamName, DbCommand cmd)
		{
			return dbParamName;
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

		/// <summary>
		/// Whether to dereference cursors.
		/// </summary>
		public bool AutoDereferenceCursors { get; set; } = true;

		/// <summary>
		/// The number of rows to fetch at once when dereferencing cursors.
		/// Set to zero or negative for FETCH ALL, but use with care this will cause huge PostgreSQL server-side buffering on large cursors.
		/// </summary>
		/// <remarks>
		/// This is large enough to get the data back reasonably quickly for most users, but might be too large for an application
		/// which requires multiple large cursors open at once.
		/// </remarks>
		public int AutoDereferenceFetchSize { get; set; } = 10000;
		#endregion
	}
}