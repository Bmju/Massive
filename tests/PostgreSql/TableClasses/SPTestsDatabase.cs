using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Massive;
using Massive.Tests;

namespace PostgreSql.TableClasses
{
	public class SPTestsDatabase : DynamicModel
	{
		public SPTestsDatabase() : base(TestConstants.ReadWriteTestConnectionStringName)
		{
		}
	}
}
