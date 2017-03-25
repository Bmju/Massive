using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Massive.Tests.Oracle.TableClasses
{
	public class SPTestsDatabase : DynamicModel
	{
		public SPTestsDatabase(string providerName) : base(string.Format(TestConstants.ReadWriteTestConnectionStringName, providerName))
		{
		}
	}
}
