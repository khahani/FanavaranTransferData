using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FanavaranTransformation
{
    public static class FarsiExtension
    {
        public static string FixYEH(this String instance)
        {
            return instance.Replace('ي', 'ی');
        }
    }
}
