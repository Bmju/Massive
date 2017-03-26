using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Massive.Tests.SqlServer.TableClasses
{
	public class SPTestsDatabase : DynamicModel
	{
		public SPTestsDatabase() : base(TestConstants.ReadTestConnection)
		{
		}
	}
}
