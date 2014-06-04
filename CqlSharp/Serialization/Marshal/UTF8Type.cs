using System;
using System.Data;
using System.Text;

namespace CqlSharp.Serialization.Marshal
{
    public class UTF8Type : CqlType<string>
    {
        public static readonly UTF8Type Instance = new UTF8Type();

        private static readonly Type AType = typeof(string);

        public override CqlTypeCode CqlTypeCode
        {
            get { return CqlTypeCode.Varchar; }
        }

        public override string TypeName
        {
            get { return "org.apache.cassandra.db.marshal.UTF8Type"; }
        }

        public override Type Type
        {
            get { return AType; }
        }

        public override DbType ToDbType()
        {
            return DbType.String;
        }

        public override byte[] Serialize(string value)
        {
            return Encoding.UTF8.GetBytes(value);
        }

        public override string Deserialize(byte[] data)
        {
            return Encoding.UTF8.GetString(data);
        }
    }
}