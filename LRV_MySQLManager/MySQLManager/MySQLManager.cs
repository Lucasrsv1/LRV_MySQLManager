/// CREATED BY LUCAS RASSILAN VILANOVA;

using MySql.Data.MySqlClient;
using System;
using System.Collections.Generic;
using System.Data;

namespace LRV_Utilities.DBMS {
	/// <summary>
	/// The Data Manipulation Language queries available.
	/// </summary>
	public enum DML { DELETE, INSERT_INTO, SELECT, UPDATE }

	/// <summary>
	/// The exception thrown during connection or queries.
	/// </summary>
	[Serializable()]
	public class MySQLManagerException : Exception { //NEXT EXCEPTION NUMBER: 13
		/// <summary>
		/// MySQLManagerException error code.
		/// </summary>
		public int Number { get; private set; }

		/// <summary>
		/// MySqlException error code.
		/// </summary>
		public int MySqlExceptionNumber { get; private set; }

		/// <summary>
		/// Initializes a new instance of the MySQLManagerException class.
		/// </summary>
		public MySQLManagerException () : base() { Number = -1; MySqlExceptionNumber = -1; }

		/// <summary>
		/// Initializes a new instance of the MySQLManagerException class with a error message.
		/// </summary>
		/// <param name="message">The error message. What happened?</param>
		public MySQLManagerException (string message) : base(message) { Number = -1; MySqlExceptionNumber = -1; }

		/// <summary>
		/// Initializes a new instance of the MySQLManagerException class with a error message and error code.
		/// </summary>
		/// <param name="message">The error message. What happened?</param>
		/// <param name="number">Error code for that message.</param>
		public MySQLManagerException (string message, int number) : base(message) { Number = number; MySqlExceptionNumber = -1; }

		/// <summary>
		/// Initializes a new instance of the MySQLManagerException class with a error message and error code.
		/// </summary>
		/// <param name="message">The error message. What happened?</param>
		/// <param name="number">Error code for that message.</param>
		/// <param name="mysqlExceptionNumber">MySqlException error code that causes the problem.</param>
		public MySQLManagerException (string message, int number, int mysqlExceptionNumber) : base(message) { Number = number; MySqlExceptionNumber = mysqlExceptionNumber; }

		/// <summary>
		/// Initializes a new instance of the MySQLManagerException class with a error message and error code.
		/// </summary>
		/// <param name="message">The error message. What happened?</param>
		/// <param name="inner">The instance of System.Exception that causes the problem.</param>
		public MySQLManagerException (string message, Exception inner) : base(message, inner) { Number = -1; MySqlExceptionNumber = -1; }

		/// <summary>
		/// Initializes a new instance of the MySQLManagerException class with a error message and error code.
		/// </summary>
		/// <param name="message">The error message. What happened?</param>
		/// <param name="inner">The instance of System.Exception that causes the problem.</param>
		/// <param name="number">Error code for that message.</param>
		public MySQLManagerException (string message, Exception inner, int number) : base(message, inner) { Number = number; MySqlExceptionNumber = -1; }

		/// <summary>
		/// Initializes a new instance of the MySQLManagerException class with a error message and error code.
		/// </summary>
		/// <param name="message">The error message. What happened?</param>
		/// <param name="inner">The instance of System.Exception that causes the problem.</param>
		/// <param name="number">Error code for that message.</param>
		/// <param name="mysqlExceptionNumber">MySqlException error code that causes the problem.</param>
		public MySQLManagerException (string message, Exception inner, int number, int mysqlExceptionNumber) : base(message, inner) { Number = number; MySqlExceptionNumber = mysqlExceptionNumber; }
		
		protected MySQLManagerException (System.Runtime.Serialization.SerializationInfo info, System.Runtime.Serialization.StreamingContext context) : base(info, context) { }
	}

	/// <summary>
	/// A Manager to construct and perform DML queries in an open connection MySQL Server database.
	/// </summary>
	public class MySQLManager {
		/// <summary>
		/// Initializes a new instance of the MySQLManager class.
		/// </summary>
		public MySQLManager () {
			Error = null;
			Connection = null;
			SelectResult = null;
			NumRows = AffectedRows = 0;
			ConnectionString = Password = User = Database = Server = Query = TableName ="";
		}

		/// <summary>
		/// Initializes a new instance of the MySQLManager class and connect to a database.
		/// </summary>
		/// <param name="database">Name of the database to connect.</param>
		/// <param name="user">Database username.</param>
		/// <param name="password">Database user password.</param>
		/// <param name="server">IP or DNS of the server you would like to connect.</param>
		public MySQLManager (string database, string user = "root", string password = "", string server = "localhost") {
			TableName = Query = "";
			NumRows = AffectedRows = 0;
			SelectResult = null;
			Connect(database, user, password, server);
		}

		/// <summary>
		/// Used to represent NULL in the queries.
		/// </summary>
		public const string NULL = "%NULL%";

		/// <summary>
		/// Constructor of connections strings to use with the MySqlConnection contructor.
		/// </summary>
		/// <param name="database">Name of the database to connect.</param>
		/// <param name="user">Database username.</param>
		/// <param name="password">Database user password.</param>
		/// <param name="server">IP or DNS of the server you would like to connect.</param>
		/// <returns>The string connection needed to create a MySqlConnection.</returns>
		public static string CreateConnectionString (string database, string user = "root", string password = "", string server = "localhost") {
			return "SERVER=" + server + ";" + "DATABASE=" + database + ";" + "UID=" + user + ";" + "PASSWORD=" + password + ";";
		}

		/// <summary>
		/// Constructor of INSERT queries.
		/// </summary>
		/// <param name="tableName">The table you would like to insert rows.</param>
		/// <param name="columns">The list of column names you will specify values.</param>
		/// <param name="values">The list of values for each column.</param>
		/// <param name="index">Number of skipped values.</param>
		/// <returns>A executable INSERT instruction.</returns>
		public static string CreateInsertIntoQuery (string tableName, string[] columns, string[][] values, int index = 0) {
			if (tableName == null) {
				Exception error = new ArgumentNullException("tableName");
				throw new MySQLManagerException("tableName cannot be null.", error, 2);
			} else if (columns == null) {
				Exception error = new ArgumentNullException("columns");
				throw new MySQLManagerException("columns cannot be null.", error, 3);
			} else if (values == null) {
				Exception error = new ArgumentNullException("values");
				throw new MySQLManagerException("values cannot be null.", error, 4);
			} else {
				foreach (string[] val in values) {
					if (val == null) {
						Exception error = new ArgumentNullException("values");
						throw new MySQLManagerException("values cannot have null rows.", error, 5);
					}
				}
			}

			if (tableName == "") {
				throw new MySQLManagerException("Cannot insert. Table not specified.", 6);
			} else if (columns.Length == 0) {
				throw new MySQLManagerException("Cannot insert into table with no columns specified.", 7);
			} else if (values.Length == 0) {
				throw new MySQLManagerException("Cannot insert into table with no values to insert.", 8);
			}

			if (index < 0)
				index = 0;
			else if (index >= values.Length)
				index = values.Length - 1;

			string valuesJoint = "";
			string columnsJoint = string.Join(", ", columns);

			for (int i = index; i < values.Length; i++) {
				if (values[i].Length > 0)
					valuesJoint += "('" + string.Join("', '", values[i]) + "'";
				else
					valuesJoint += "(";

				valuesJoint = valuesJoint.Replace("'" + NULL + "'", "NULL");

				for (int c = 0; values[i].Length + c < columns.Length; c++) {
					if (values[i].Length + c > 0)
						valuesJoint += ", ";

					valuesJoint += "NULL";
				}

				valuesJoint += ")";
				if (i + 1 != values.Length)
					valuesJoint += ", ";
			}

			return "INSERT INTO " + tableName + "(" + columnsJoint + ") VALUES " + valuesJoint;
		}

		/// <summary>
		/// Constructor of DELETE queries.
		/// </summary>
		/// <param name="tableName">The table you would like to delete from.</param>
		/// <param name="condition">The WHERE clause. What is the condition to delete a row?</param>
		/// <returns>A executable DELETE instruction.</returns>
		public static string CreateDeleteFromQuery (string tableName, string condition = "") {
			if (tableName == null) {
				Exception error = new ArgumentNullException("tableName");
				throw new MySQLManagerException("tableName cannot be null.", error, 2);
			}

			if (tableName == "")
				throw new MySQLManagerException("Cannot delete. Table not specified.", 6);

			condition = ClearCondition(condition);

			return "DELETE FROM " + tableName + " WHERE " + condition;
		}

		/// <summary>
		/// Constructor of UPDATE queries.
		/// </summary>
		/// <param name="tableName">The table you would like to update.</param>
		/// <param name="columns">The list of columns names to update.</param>
		/// <param name="values">The list of values to update the columns.</param>
		/// <param name="condition">The WHERE clause. What is the condition to update a row?</param>
		/// <returns>A executable UPDATE instruction.</returns>
		public static string CreateUpdateQuery (string tableName, string[] columns, string[] values, string condition = "") {
			if (tableName == null) {
				Exception error = new ArgumentNullException("tableName");
				throw new MySQLManagerException("tableName cannot be null.", error, 2);
			} else if (columns == null) {
				Exception error = new ArgumentNullException("columns");
				throw new MySQLManagerException("columns cannot be null.", error, 3);
			} else if (values == null) {
				Exception error = new ArgumentNullException("values");
				throw new MySQLManagerException("values cannot be null.", error, 4);
			}

			if (tableName == "") {
				throw new MySQLManagerException("Cannot update. Table not specified.", 6);
			} else if (columns.Length == 0) {
				throw new MySQLManagerException("Cannot update table with no columns specified.", 7);
			} else if (values.Length == 0) {
				throw new MySQLManagerException("Cannot update table with no values to set.", 8);
			} else if (condition == "" || condition == null) {
				condition = "TRUE";
			}

			string setJoint = "";
			condition = ClearCondition(condition);

			for (int i = 0; i < columns.Length; i++) {
				if (i < values.Length)
					setJoint += columns[i] + " = '" + values[i] + "'";
				else
					setJoint += columns[i] + " = '" + NULL + "'";

				setJoint = setJoint.Replace("'" + NULL + "'", "NULL");

				if (i + 1 != columns.Length)
					setJoint += ", ";
			}

			return "UPDATE " + tableName + " SET " + setJoint + " WHERE " + condition;
		}

		/// <summary>
		/// Constructor of SELECT queries.
		/// </summary>
		/// <param name="tableName">The table you would like to select from.</param>
		/// <param name="columns">The list of columns names to select.</param>
		/// <param name="condition">The WHERE clause. What is the condition to select a row?</param>
		/// <param name="complement">The last part of the query, that can be a GROUP BY or LIMIT statements, for example.</param>
		/// <param name="inner">The inner part of the query, such as INNER JOIN or INNER LEFT.</param>
		/// <returns>A executable SELECT instruction.</returns>
		public static string CreateSelectQuery (string tableName, string[] columns, string condition = "", string complement = "", string inner = "") {
			if (tableName == null) {
				Exception error = new ArgumentNullException("tableName");
				throw new MySQLManagerException("tableName cannot be null.", error, 2);
			} else if (columns == null) {
				Exception error = new ArgumentNullException("columns");
				throw new MySQLManagerException("columns cannot be null.", error, 3);
			}

			if (tableName == "") {
				throw new MySQLManagerException("Cannot select. Table not specified.", 6);
			} else if (columns.Length == 0) {
				throw new MySQLManagerException("Cannot select from table with no columns specified.", 7);
			} else if (condition == "" || condition == null) {
				condition = "TRUE";
			}

			if (inner == null)
				inner = "";
			else if (inner.Length > 0)
				inner = inner.Replace("'" + NULL + "'", "NULL") + " ";

			if (complement == null)
				complement = "";
			else
				complement = complement.Replace("'" + NULL + "'", "NULL");

			string columnsJoint = string.Join(", ", columns);
			condition = ClearCondition(condition);

			return ("SELECT " + columnsJoint + " FROM " + tableName + " " + inner + "WHERE " + condition + " " + complement).Trim();
		}

		private static string ClearCondition (string condition) {
			if (condition == "" || condition == null)
				condition = "TRUE";

			condition = condition.Trim();
			condition = condition.Replace("'" + NULL + "'", "NULL");

			if (condition.ToUpper().StartsWith("WHERE "))
				condition = condition.Substring(6);

			return condition;
		}

		/// <summary>
		/// Is the MySQLManager connected to a server and database?
		/// </summary>
		public bool Connected {
			get {
				return Connection.State == ConnectionState.Open || Connection.State == ConnectionState.Fetching || Connection.State == ConnectionState.Executing;
			}
		}

		/// <summary>
		/// Total number of rows returned in the last SELECT.
		/// </summary>
		public int NumRows { get; private set; }

		/// <summary>
		/// Number of rows inserted in the last INSERT or number of rows changed in the last UPDATE
		/// or number of rows deleted in the last DELETE.
		/// </summary>
		public int AffectedRows { get; private set; }

		/// <summary>
		/// Last query executed.
		/// </summary>
		public string Query { get; private set; }

		/// <summary>
		/// The last Data Manipulation Language query executed.
		/// </summary>
		public DML CurrentDML { get; private set; }

		/// <summary>
		/// Database username.
		/// </summary>
		public string User { get; private set; }

		/// <summary>
		/// Database password.
		/// </summary>
		public string Password { get; private set; }

		/// <summary>
		/// IP or DNS of the connected server.
		/// </summary>
		public string Server { get; private set; }

		/// <summary>
		/// Name of the connected database.
		/// </summary>
		public string Database { get; private set; }

		/// <summary>
		/// The string used to create the MySqlConnection.
		/// </summary>
		public string ConnectionString { get; private set; }

		/// <summary>
		/// Name of the last table accessed.
		/// </summary>
		public string TableName { get; private set; }

		/// <summary>
		/// Result of the last SELECT. The first row (SelectResult[0]) has the names of the selected columns.
		/// Also, SelectResult.Length is equal to NumRows + 1.
		/// </summary>
		public string[][] SelectResult { get; private set; }

		/// <summary>
		/// The instance of MySqlConnection used by this library to perform queries.
		/// </summary>
		public MySqlConnection Connection { get; private set; }

		/// <summary>
		/// The last exception thrown.
		/// </summary>
		public MySQLManagerException Error { get; private set; }

		public bool Connect (string database, string user = "root", string password = "", string server = "localhost") {
			Server = server;
			Database = database;
			User = user;
			Password = password;
			ConnectionString = CreateConnectionString(Database, User, Password, Server);

			Connection = new MySqlConnection(ConnectionString);

			return OpenConnection();
		}

		public bool OpenConnection () {
			Error = null;
			try {
				if (Connection.State == ConnectionState.Broken)
					Connection.Close();

				if (Connection.State != ConnectionState.Open)
					Connection.Open();

				return true;
			} catch (MySqlException error) {
				switch (error.Number) {
					case 0:
						Error = new MySQLManagerException("Cannot connect to server. Check string connection.", error, 0, 0);
						break;
					case 1045:
						Error = new MySQLManagerException("Invalid username/password, please try again.", error, 1, 1045);
						break;
					default:
						Error = new MySQLManagerException(error.Message, error, -1, error.Number);
						break;
				}
			} catch (Exception error) {
				Error = new MySQLManagerException(error.Message, error, -1);
			}

			return false;
		}

		public bool CloseConnection () {
			Error = null;
			try {
				if (Connection.State != ConnectionState.Closed)
					Connection.Close();

				return true;
			} catch (Exception error) {
				Error = new MySQLManagerException(error.Message, error, -1);
			}

			return false;
		}

		private bool ValidateQuery (string query) {
			Query = "";
			if (query == null) {
				Exception error = new ArgumentNullException("query");
				Error = new MySQLManagerException("query cannot be null.", error, 9);
				return false;
			}

			query = query.Trim();
			query = query.Replace("'" + NULL + "'", "NULL");

			switch (CurrentDML) {
				case DML.DELETE:
					if (!query.ToUpper().StartsWith("DELETE FROM") || query.Length <= 11) {
						Error = new MySQLManagerException("Cannot delete. Invalid query.", 10);
						return false;
					}
					break;
				case DML.INSERT_INTO:
					if (!query.ToUpper().StartsWith("INSERT INTO") || !query.ToUpper().Contains("VALUES") || query.IndexOf("(") == -1 || query.IndexOf(")") == -1) {
						Error = new MySQLManagerException("Cannot insert. Invalid query.", 10);
						return false;
					}
					break;
				case DML.SELECT:
					if (!query.ToUpper().StartsWith("SELECT") || !query.ToUpper().Contains("FROM")) {
						Error = new MySQLManagerException("Cannot select. Invalid query.", 10);
						return false;
					}
					break;
				case DML.UPDATE:
					if (!query.ToUpper().StartsWith("UPDATE") || !query.ToUpper().Contains("SET") || query.IndexOf("=") == -1) {
						Error = new MySQLManagerException("Cannot update. Invalid query.", 10);
						return false;
					}
					break;
			}

			Query = query;
			return true;
		}

		private string[][] SelectToInsertValues (List<string>[] select) {
			if (select == null) {
				Exception error = new ArgumentNullException("select");
				Error = new MySQLManagerException("select cannot be null.", error, 11);
				return null;
			} else {
				foreach (List<string> s in select) {
					if (s == null) {
						Exception error = new ArgumentNullException("select");
						Error = new MySQLManagerException("select cannot have null columns.", error, 12);
						return null;
					}
				}
			}

			int numRows = select[0].Count;
			string[][] selectValues = new string[numRows][];

			for (int r = 0; r < numRows; r++) {
				selectValues[r] = new string[select.Length];
				for (int c = 0; c < select.Length; c++) {
					selectValues[r][c] = select[c][r];
				}
			}

			return selectValues;
		}

		private int ExecuteNonQuery () {
			Error = null;
			AffectedRows = -1;
			using (MySqlConnection localConnection = new MySqlConnection(ConnectionString)) {
				try {
					localConnection.Open();
					MySqlCommand command = new MySqlCommand(Query, localConnection);
					AffectedRows = command.ExecuteNonQuery();

					command.Dispose();
					localConnection.Close();
				} catch (MySqlException error) {
					Console.WriteLine("ME: " + error.Message);
					Error = new MySQLManagerException(error.Message, error, -1, error.Number);
				} catch (Exception error) {
					Console.WriteLine("E: " + error.Message);
					Error = new MySQLManagerException(error.Message, error, -1);
				}
			}

			return AffectedRows;
		}

		//Data Manipulation Language:
		public int DeleteFrom (string tableName, string condition = "") {
			AffectedRows = -1;

			string query = "";
			try {
				query = CreateDeleteFromQuery(tableName, condition);
			} catch (MySQLManagerException error) {
				Error = error;
				return -1;
			}

			return DeleteFrom(query);
		}

		public int DeleteFrom (string query) {
			AffectedRows = -1;
			CurrentDML = DML.DELETE;

			if (!ValidateQuery(query))
				return -1;

			return ExecuteNonQuery();
		}

		public int InsertInto (string tableName, string[] columns, string[][] values, int index = 0) {
			AffectedRows = -1;

			string query = "";
			try {
				query = CreateInsertIntoQuery(tableName, columns, values, index);
			} catch (MySQLManagerException error) {
				Error = error;
				return -1;
			}

			return InsertInto(query);
		}

		public int InsertInto (string query) {
			AffectedRows = -1;
			CurrentDML = DML.INSERT_INTO;

			if (!ValidateQuery(query))
				return -1;

			return ExecuteNonQuery();
		}

		public string[][] Select (string tableName, string[] columns, string condition = "", string complement = "", string inner = "") {
			string query = TableName = "";
			SelectResult = null;
			NumRows = 0;

			try {
				query = CreateSelectQuery(tableName, columns, condition, complement, inner);
			} catch (MySQLManagerException error) {
				Error = error;
				return null;
			}

			return Select(query);
		}

		public string[][] Select (string query) {
			Error = null;
			NumRows = 0;
			TableName = "";
			SelectResult = null;
			CurrentDML = DML.SELECT;
			if (!ValidateQuery(query))
				return null;

			bool close = false;
			List<string>[] result = null;

			if (!Connected) {
				if (!OpenConnection()) {
					return null;
				} else {
					close = true;
				}
			}

			try {
				MySqlCommand command = new MySqlCommand(Query, Connection);
				MySqlDataReader reader = command.ExecuteReader(CommandBehavior.KeyInfo);
				
				while (reader.Read()) {
					if (NumRows == 0) {
						result = new List<string>[reader.FieldCount];
						for (int i = 0; i < result.Length; i++)
							(result[i] = new List<string>()).Add(reader.GetName(i));
					}

					NumRows++;
					for (int i = 0; i < reader.FieldCount; i++) {
						if (reader[i] == DBNull.Value)
							result[i].Add(NULL);
						else
							result[i].Add(reader[i].ToString());
					}
				}

				reader.Close();
				command.Dispose();
				reader.Dispose();
			} catch (MySqlException error) {
				Error = new MySQLManagerException(error.Message, error, -1, error.Number);
				result = null;
			} catch (Exception error) {
				Error = new MySQLManagerException(error.Message, error, -1);
				result = null;
			}

			if (close)
				CloseConnection();

			if (result != null) {
				string tableName = query.Replace("`", " ");
				tableName = tableName.Substring(tableName.ToUpper().IndexOf("FROM ") + 5).Trim();
				if (tableName.IndexOf(" ") != -1)
					tableName = tableName.Substring(0, tableName.IndexOf(" "));

				TableName = tableName;
				SelectResult = SelectToInsertValues(result);
			}

			return SelectResult;
		}

		public int Update (string tableName, string[] columns, string[] values, string condition = "") {
			AffectedRows = -1;

			string query = "";
			try {
				query = CreateUpdateQuery(tableName, columns, values, condition);
			} catch (MySQLManagerException error) {
				Error = error;
				return -1;
			}

			return Update(query);
		}

		public int Update (string query) {
			AffectedRows = -1;
			CurrentDML = DML.UPDATE;

			if (!ValidateQuery(query))
				return -1;

			return ExecuteNonQuery();
		}
	}
}