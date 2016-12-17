/// CREATED BY LRV;

/// This software is for test the MySQLManager library;
/// To use it, please create the next users in your DBMS or change the connection configuration below:
/// 'Manager'@'%' with password "12345678" and all DML privilegies;
/// 'Manager'@'localhost' with password "12345678" and all DML privilegies;
///
/// And then, run the next SQL script:
/// CREATE DATABASE IF NOT EXISTS test;
/// USE test;
/// CREATE TABLE IF NOT EXISTS teste (
///		id int PRIMARY KEY AUTO_INCREMENT,
///		string VARCHAR(100) NULL
/// );

using DBMS;
using System;
using System.Collections.Generic;

namespace MySQLManager_Tests {
	class Program {
		const string NULL = MySQLManager.NULL;
		static MySQLManager dbms;

		static void ShowNonQueryResult () {
			Console.WriteLine("Query Type: " + dbms.CurrentDML + " [OK]");
			Console.WriteLine("Query: " + dbms.Query + " [OK]");

			if (dbms.AffectedRows == -1) {
				if (dbms.Error.Number != -1)
					Console.WriteLine("Error " + dbms.Error.Number + ": " + dbms.Error.Message + " [!]");

				if (dbms.Error.MySqlExceptionNumber != -1)
					Console.WriteLine("MySql error " + dbms.Error.MySqlExceptionNumber + ": " + dbms.Error.InnerException.Message + " [!]\n");
				else
					Console.WriteLine("Exception message: " + dbms.Error.InnerException.Message + " [!]\n");
			} else {
				Console.WriteLine("Affected Rows: " + dbms.AffectedRows + " [OK]\n");
			}
		}

		static void ShowQueryResult (List<string>[] select, string tableName, string[] columns) {
			Console.WriteLine("Query Type: " + dbms.CurrentDML + " [OK]");
			Console.WriteLine("Query: " + dbms.Query + " [OK]");

			if (select != null) {
				Console.WriteLine("Table Name: " + tableName + " [OK]");
				Console.WriteLine("Num rows selected: " + dbms.NumRows + " [OK]");

				Console.WriteLine("Select result:");
				Console.WriteLine("| " + string.Join(" | ", columns) + " |");
				for (int row = 0; row < dbms.NumRows; row++) {
					Console.Write("| ");
					for (int column = 0; column < select.Length; column++)
						Console.Write(select[column][row] + " | ");

					Console.Write("\n");
				}
				Console.WriteLine("Select completed [OK]\n");
			} else {
				if (dbms.Error.Number != -1)
					Console.WriteLine("Error " + dbms.Error.Number + ": " + dbms.Error.Message + " [!]");

				if (dbms.Error.MySqlExceptionNumber != -1)
					Console.WriteLine("MySql error " + dbms.Error.MySqlExceptionNumber + ": " + dbms.Error.InnerException.Message + " [!]\n");
				else
					Console.WriteLine("Exception message: " + dbms.Error.InnerException.Message + " [!]\n");
			}
		}

		static void Main (string[] args) {
			Console.WriteLine("Starting [...]");

			dbms = new MySQLManager("test", "Manager", "12345678"); //Connection
			Console.WriteLine("Connection String: " + dbms.ConnectionString + " [OK]");
			Console.WriteLine("Connection Status: " + ((dbms.Connected) ? "CONNECTED [OK]\n" : "DISCONNECTED [!]\n"));
			if (dbms.Error != null) {
				if (dbms.Error.Number != -1)
					Console.WriteLine("Error " + dbms.Error.Number + ": " + dbms.Error.Message + " [!]");

				if (dbms.Error.MySqlExceptionNumber != -1)
					Console.WriteLine("MySql error " + dbms.Error.MySqlExceptionNumber + ": " + dbms.Error.InnerException.Message + " [!]\n");
				else
					Console.WriteLine("Exception message: " + dbms.Error.InnerException.Message + " [!]\n");

				Console.ReadKey();
				return;
			}

			string tableName = "teste";
			string[] columns = new string[] { "string", "id" };
			string[][] values = new string[][] { new string[] { "Hello" }, new string[] { "World" } };

			dbms.InsertInto(tableName, columns, values);
			ShowNonQueryResult();

			List<string>[] select = dbms.Select("SELECT COUNT(*) FROM teste", 1);
			ShowQueryResult(select, tableName, new string[] { "COUNT(*)" });

			dbms.DeleteFrom("DELETE FROM teste;");
			ShowNonQueryResult();

			dbms.InsertInto("INSERT INTO teste(id) VALUES (75), (76), (77), (78), (79), (80);");
			ShowNonQueryResult();

			dbms.Update("UPDATE teste SET id = 73 WHERE id = 77;");
			ShowNonQueryResult();

			dbms.Update(tableName, new string[] { "string" }, new string[] { "TEST" }, "id = 73");
			ShowNonQueryResult();

			select = dbms.Select(tableName, new string[] { "string" }, "id < 77", "ORDER BY id DESC");
			ShowQueryResult(select, tableName, new string[] { "string" });

			dbms.InsertInto(tableName, new string[] { "string" }, MySQLManager.SelectToInsertValues(select));
			ShowNonQueryResult();

			select = dbms.Select("SELECT id AS Um, string FROM teste ORDER BY id DESC LIMIT 0, 1", 2);
			ShowQueryResult(select, tableName, new string[] { "Um", "String" });

			dbms.DeleteFrom(tableName, "id = " + select[0][0]);
			ShowNonQueryResult();

			select = dbms.Select("SELECT COUNT(*) FROM teste", 1);
			ShowQueryResult(select, tableName, new string[] { "COUNT(*)" });

			Console.WriteLine("Finished [OK]");
			Console.ReadKey();
		}
	}
}