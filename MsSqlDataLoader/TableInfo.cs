using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace MsSqlDataLoader
{
	public class TableInfo
	{
		public string Schema { get; set; }
		public string Name { get; set; }

		public override string ToString()
		{
			return string.Format("[{0}].[{1}]", Schema, Name);
		}
	}
}
