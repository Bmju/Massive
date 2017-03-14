using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Massive.Tests.PostgreSql.TableClasses
{
	public class Product : DynamicModel
	{
		public Product()
			: this(includeSchema: true)
		{
		}


		public Product(bool includeSchema) :
			base(TestConstants.ReadWriteTestConnectionStringName, includeSchema ? "public.products" : "products", "productid", string.Empty, "products_productid_seq")
		{
		}
	}
}
