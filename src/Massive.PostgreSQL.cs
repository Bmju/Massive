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
	/// <summary>
	/// Class which provides extension methods for various ADO.NET objects.
	/// </summary>
    public static partial class ObjectExtensions
	{
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
		/// </remarks>
		public static DbDataReader ExecuteDereferencingReader(this DbCommand cmd, DbConnection Connection, DynamicModel db, bool DereferenceCursors = true)
		{
			var reader = cmd.ExecuteReader(); // Execute(behavior);

			// Perhaps a bool property on NpgsqlCommand?
			if (DereferenceCursors)
			{
				// Transparently dereference returned cursors, where possible
				bool cursors = false;
				bool noncursors = false;
				for (int i = 0; i < reader.FieldCount; i++)
				{
					if (reader.GetDataTypeName(i) == "refcursor") cursors = true;
					else noncursors = true;
				}

				// Don't consider dereferencing if no returned columns are cursors
				if (cursors)
				{
					// Iff dereferencing was turned on, this will stop and complain if some but not all columns are cursors
					if (noncursors)
					{
						throw new InvalidOperationException("Command returns mixed cursor and non-cursor results. To read this data you must disable Npgsql automatic cursor dereferencing and write your own cursor FETCH commands.");
					}

					// Supports 1x1 1xN Nx1 (and NXM!) patterns of cursor data
					var sb = new StringBuilder();
					while (reader.Read())
					{
						for (int i = 0; i < reader.FieldCount; i++)
						{
							// Note that FETCH ALL FROM cursor correctly streams cursored data without any pathological server or client side buffering, even for huge datasets.
							// http://stackoverflow.com/a/42297234/795690
							// Closing cursors as we go to save server side resources.
							// TO DO: This *will* break if the cursor name contains ", which it can - the cursor references should be arguments.
							// Have applied working (but less good) .Replace() fix here for use in Massive.
							sb.AppendFormat(@"FETCH ALL FROM ""{0}"";CLOSE ""{0}"";", reader.GetString(i).Replace(@"""", @""""""));
						}
					}
					reader.Dispose();

					var dereferenceCmd = db.CreateCommand(sb.ToString(), Connection); // new NpgsqlCommand(sb.ToString(), Connection);
					try
					{
						return dereferenceCmd.ExecuteReader(); // .ExecuteReader(behavior);
					}					
					catch (DbException ex) //catch (PostgresException ex)
					{
						if ((string)((PropertyInfo)ex.GetType().GetProperties().Where(property => property.Name == "SqlState").FirstOrDefault()).GetValue(ex, null) == "34000") // if (ex.SqlState == "34000")
						{
							throw new InvalidOperationException("Cursor dereferencing requires a containing transaction. Please add one, or consider using TABLE return values instead: these are more efficient than cursors for small and medium sized data sets.");
						}
						throw;
					}
				}
			}

			return reader;
		}


		/// <summary>
		/// Extension to set the parameter to the DB specific cursor type.
		/// </summary>
		/// <param name="p">The parameter.</param>
		/// <returns>Returns false if not supported on this provider.</returns>
		public static bool SetCursor(this DbParameter p)
		{
			// If we were explicitly linking to Npgsql.dll then this would just be ((NpgsqlParameter)p).NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Refcursor;
			p.SetRuntimeEnumProperty("NpgsqlDbType", "Refcursor");
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
		/// Returns true if this command is a cursor command. Does any additional pre-processsing necessary if so.
		/// </summary>
		/// <param name="cmd">The command.</param>
		/// <returns>true if it's a cursor command</returns>
		public static bool IsCursorCommand(this DbCommand cmd)
		{
			// If we've got cursor parameters these are actually just placeholders to kick off cursor support (i.e. the wrapping transaction); we need to remove them before executing the command.
			bool IsCursorCommand = false;
			cmd.Parameters.Cast<DbParameter>().Where(p => p.IsCursor()).ToList().ForEach(p => { IsCursorCommand = true; cmd.Parameters.Remove(p); });
			return IsCursorCommand;
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
		/// Extension to check whether ADO.NET provider notices output parameter types (no point requiring user to provide them when it doesn't)
		/// </summary>
		/// <param name="p">The parameter.</param>
		/// <returns>Return true if output types should be enforced.</returns>
		private static bool EnforceOutputTypes(this DbParameter p)
		{
			return false;
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
		/// Does cursor access on this provider require a wrapping transaction?
		/// </summary>
		/// <returns>true if wrapping transaction required.</returns>
		protected virtual bool CursorsRequireTransaction()
		{
			return true;
		}


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