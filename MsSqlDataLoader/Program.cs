using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using NDesk.Options;

namespace MsSqlDataLoader
{
	class Program
	{
		static int Main(string[] args)
		{
			bool showHelp = false;

			string serverName = null;
			string dbName = null;
			string user = null;
			string pass = null;

			string tableListFile = null;

			bool loadData = false;
			int block = 100;

			var p = new OptionSet()
			{
				{ "s=|server=", "server name or IP address", v => serverName = v },
				{ "d=|database=", "name of database", v => dbName = v },
				{ "u=|user=", "user name", v => user = v },
				{ "p=|password=", "password", v => pass = v },
				{ "t=|tableList=", "file containing a list of tables", v => tableListFile = v },
				{ "l|loadData", "load data from tables", v => loadData = v != null },
				{ "b|block", "insert block (default - 100)", v => block = (v != null)?int.Parse(v):100 },
				{ "h|help", "show this message and exit", v => showHelp = v != null }
			};

			try
			{
				p.Parse(args);
				if (block <= 0)
					block = 100;
			}
			catch (OptionException e)
			{
				Console.WriteLine("MsSqlDataLoader: {0}", e.Message);
				Console.WriteLine("Try `MsSqlDataLoader --help' for more information.");
				return 1;
			}

			if (showHelp)
			{
				ShowHelp(p);
				return 0;
			}

			var connString = string.Format("Server={0};Database={1};User Id={2};Password={3};", serverName, dbName, user, pass);

			try
			{
				List<string> tableList;

				var conn = new SqlConnection(connString);
				conn.Open();

				if(string.IsNullOrEmpty(tableListFile))
				{
					tableList = GetTableNames(conn);
				}
				else
				{
					tableList = File.ReadAllLines(tableListFile)
						.Where(s => !string.IsNullOrWhiteSpace(s))
						.ToList();
				}

				if(loadData)
				{
					foreach (var tabName in tableList)
						LoadData(conn, tabName);
				}
				else
				{
					foreach (var tabName in tableList)
						Console.WriteLine(tabName);
				}

				return 0;
			}
			catch (Exception ex)
			{
				Console.WriteLine();
				Console.WriteLine("Error: {0}", ex);
				return 1;
			}
		}

		private static void LoadData(SqlConnection conn, string tabName)
		{
			Console.Write("Load table {0} - ", tabName);
			int consoleCol = Console.CursorLeft;
			Console.Write("0");

			var file = tabName + ".sql";
			bool isAutoIncrement = CheckIdentity(conn, tabName);

			string query = string.Format("select * from {0}", tabName);

			var cmd = new SqlCommand(query, conn);

			var sb = new StringBuilder();

			int total = 0;

			using (var reader = cmd.ExecuteReader())
			{
				var cols = reader.FieldCount;
				var colNames = Enumerable.Range(0, cols).Select(i => "[" + reader.GetName(i) + "]");
				var insertHeader = string.Format("INSERT {0} ({1}) VALUES", tabName, string.Join(", ", colNames));

				var textRows = new List<string>();

				while (reader.Read())
				{
					textRows.Add(RowToValues(Enumerable.Range(0, cols).Select(reader.GetValue)));

					if (textRows.Count >= 100)
					{
						total += textRows.Count;
						Write(sb, insertHeader, textRows);
						textRows.Clear();

						Console.CursorLeft = consoleCol;
						Console.Write(total);
					}
				}

				total += textRows.Count;
				Write(sb, insertHeader, textRows);
				Console.CursorLeft = consoleCol;
				Console.Write(total);
			}

			if (sb.Length != 0)
			{
				if (isAutoIncrement)
				{
					sb.Insert(0, string.Format("SET IDENTITY_INSERT {0} ON\r\n\r\n", tabName));
					sb.Append(string.Format("SET IDENTITY_INSERT {0} OFF\r\n", tabName));
				}
				File.WriteAllText(file, sb.ToString());

				Console.WriteLine(" saved.");
			}
			else
			{
				Console.WriteLine();
			}
		}

		static void ShowHelp(OptionSet p)
		{
			Console.WriteLine("Usage: MsSqlDataLoader [OPTIONS]");
			Console.WriteLine("Options:");
			p.WriteOptionDescriptions(Console.Out);
			Console.WriteLine();
			Console.WriteLine("Example:");
			Console.WriteLine("  * Show all table names");
			Console.WriteLine("  MsSqlDataLoader.exe -s=localhost -d=Northwind -u=sa -p=sa ");
			Console.WriteLine("  * Load tables from list");
			Console.WriteLine("  MsSqlDataLoader.exe -s=localhost -d=Northwind -u=sa -p=sa -t=tables.txt -l");
		}

		public static List<string> GetTableNames(SqlConnection conn)
		{
			DataTable schema = conn.GetSchema("Tables");
			var result = new List<string>();
			foreach (DataRow row in schema.Rows)
			{
				if (string.Equals("BASE TABLE", row[3].ToString(), StringComparison.InvariantCultureIgnoreCase))
					result.Add(string.Format("[{0}].[{1}]", row[1], row[2]));
			}
			return result;
		}

		private static bool CheckIdentity(SqlConnection conn, string tabName)
		{
			string cmdText = string.Format("SELECT TOP(1) * FROM {0}", tabName);

			var dataTable = new DataTable();
			new SqlDataAdapter(cmdText, conn).FillSchema(dataTable, SchemaType.Source);

			return dataTable.Columns.Cast<DataColumn>().Any(col => col.AutoIncrement);
		}

		private static void Write(StringBuilder sb, string insertHeader, List<string> textRows)
		{
			if(textRows.Count == 0)
				return;

			sb.Append(insertHeader);
			sb.AppendLine();
			sb.Append(textRows[0]);
			foreach(var line in textRows.Skip(1))
			{
				sb.Append(",\r\n");
				sb.Append(line);
			}
			sb.AppendLine();
			sb.Append("GO");
			sb.AppendLine();
		}

		private static string RowToValues(IEnumerable<object> values)
		{
			return "(" + string.Join(", ", values.Select(CellToSQL)) + ")";
		}

		private static string CellToSQL(object val)
		{
			Type t = val.GetType();
			if (t == typeof(DBNull))
				return "NULL";
			if (t == typeof(bool))
				return (bool)val?"1":"0";

			if (t == typeof(string))
			{
				var text = (string) val;
				text = text.Replace("'", "''");

				if (IsASCII(text))
					return "'" + text + "'";
				else
					return "N'" + text + "'";
			}

			if (t == typeof(DateTime))
				return "'" + ((DateTime)val).ToString("yyyy.MM.dd HH:mm:ss.fff") + "'";

			if (t == typeof(Int16))
				return val.ToString();

			if (t == typeof(Int32))
				return val.ToString();

			if (t == typeof(Int64))
				return val.ToString();

			if (t == typeof(byte))
				return val.ToString();

			if (t == typeof(decimal))
				return ((decimal)val).ToString(CultureInfo.InvariantCulture);

			if (t == typeof(double))
				return ((double)val).ToString(CultureInfo.InvariantCulture);

			if (t == typeof(float))
				return ((float)val).ToString(CultureInfo.InvariantCulture);

			if (t == typeof(Guid))
				return "'" + ((Guid)val).ToString() + "'";

			if (t == typeof(Byte[]))
				return ByteArrayToString((Byte[])val);

			throw new NotImplementedException("unexpected type: " + t.FullName);
		}

		public static string ByteArrayToString(byte[] ba)
		{
			var hex = new StringBuilder(ba.Length * 2);
			hex.Append("0x");
			foreach (byte b in ba)
				hex.AppendFormat("{0:x2}", b);
			return hex.ToString();
		}

		private static bool IsASCII(string text)
		{
			return text.All(ch => (int) ch <= 255);
		}
	}
}
