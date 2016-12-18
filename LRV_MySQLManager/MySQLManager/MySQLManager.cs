/// CREATED BY LRV;

using MySql.Data.MySqlClient;
using System;
using System.Collections.Generic;
using System.Data;

namespace DBMS {
	public enum DML { DELETE, INSERT_INTO, SELECT, UPDATE }

	[Serializable()]
	public class MySQLManagerException : Exception { //NEXT EXCEPTION NUMBER: 13
		public int Number { get; private set; }
		public int MySqlExceptionNumber { get; private set; }

		public MySQLManagerException () : base() { Number = -1; MySqlExceptionNumber = -1; }
		public MySQLManagerException (string message) : base(message) { Number = -1; MySqlExceptionNumber = -1; }
		public MySQLManagerException (string message, int number) : base(message) { Number = number; MySqlExceptionNumber = -1; }
		public MySQLManagerException (string message, int number, int mysqlExceptionNumber) : base(message) { Number = number; MySqlExceptionNumber = mysqlExceptionNumber; }
		public MySQLManagerException (string message, Exception inner) : base(message, inner) { Number = -1; MySqlExceptionNumber = -1; }
		public MySQLManagerException (string message, Exception inner, int number) : base(message, inner) { Number = number; MySqlExceptionNumber = -1; }
		public MySQLManagerException (string message, Exception inner, int number, int mysqlExceptionNumber) : base(message, inner) { Number = number; MySqlExceptionNumber = mysqlExceptionNumber; }

		protected MySQLManagerException (System.Runtime.Serialization.SerializationInfo info, System.Runtime.Serialization.StreamingContext context) : base(info, context) { }
	}

	public class MySQLManager {
		public MySQLManager () {
			Error = null;
			Connection = null;
			SelectResult = null;
			NumRows = AffectedRows = 0;
			ConnectionString = Password = User = Database = Server = Query = TableName ="";
		}

		public MySQLManager (string database, string user = "root", string password = "", string server = "localhost") {
			TableName = Query = "";
			NumRows = AffectedRows = 0;
			SelectResult = null;
			Connect(database, user, password, server);
		}

		public const string NULL = "%NULL%";

		public static string CreateConnectionString (string database, string user = "root", string password = "", string server = "localhost") {
			return "SERVER=" + server + ";" + "DATABASE=" + database + ";" + "UID=" + user + ";" + "PASSWORD=" + password + ";";
		}

		public static string CreateInsertIntoQuery (string tableName, string[] columns, string[][] values) {
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

			string valuesJoint = "";
			string columnsJoint = string.Join(", ", columns);

			for (int i = 0; i < values.Length; i++) {
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

		public static string[][] SelectToInsertValues (List<string>[] select) {
			if (select == null) {
				Exception error = new ArgumentNullException("select");
				throw new MySQLManagerException("select cannot be null.", error, 11);
			} else {
				foreach (List<string> s in select) {
					if (s == null) {
						Exception error = new ArgumentNullException("select");
						throw new MySQLManagerException("select cannot have null columns.", error, 12);
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

		private static string ClearCondition (string condition) {
			if (condition == "" || condition == null)
				condition = "TRUE";

			condition = condition.Trim();
			condition = condition.Replace("'" + NULL + "'", "NULL");

			if (condition.ToUpper().StartsWith("WHERE "))
				condition = condition.Substring(6);

			return condition;
		}

		public bool Connected {
			get {
				return Connection.State == ConnectionState.Open || Connection.State == ConnectionState.Fetching || Connection.State == ConnectionState.Executing;
			}
		}

		public int NumRows { get; private set; }
		public int AffectedRows { get; private set; }
		public string Query { get; private set; }
		public DML CurrentDML { get; private set; }

		public string User { get; private set; }
		public string Password { get; private set; }
		public string Server { get; private set; }
		public string Database { get; private set; }
		public string ConnectionString { get; private set; }

		public string TableName { get; private set; }
		public List<string>[] SelectResult { get; private set; }
		public MySqlConnection Connection { get; private set; }
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
			try {
				if (Connection.State == ConnectionState.Broken)
					Connection.Close();

				if (Connection.State != ConnectionState.Open)
					Connection.Open();

				Error = null;
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

		private int ExecuteNonQuery () {
			bool close = false;
			if (!Connected) {
				if (!OpenConnection()) {
					return -1;
				} else {
					close = true;
				}
			}

			try {
				MySqlCommand command = new MySqlCommand(Query, Connection);
				AffectedRows = command.ExecuteNonQuery();
			} catch (MySqlException error) {
				Error = new MySQLManagerException(error.Message, error, -1, error.Number);
			} catch (Exception error) {
				Error = new MySQLManagerException(error.Message, error, -1);
			}

			if (close)
				CloseConnection();

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

		public int InsertInto (string tableName, string[] columns, string[][] values) {
			AffectedRows = -1;

			string query = "";
			try {
				query = CreateInsertIntoQuery(tableName, columns, values);
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

		public List<string>[] Select (string tableName, string[] columns, string condition = "", string complement = "", string inner = "") {
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

		public List<string>[] Select (string query) {
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
			}

			SelectResult = result;
			return result;
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