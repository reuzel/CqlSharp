using System;
using System.Data;
using System.Text;

namespace CqlSharp.Serialization.Marshal
{
    public class AsciiType : CqlType<string>
    {
        public static readonly AsciiType Instance = new AsciiType();

        private static readonly Type AType = typeof(string);

        public override CqlTypeCode CqlTypeCode
        {
            get { return CqlTypeCode.Ascii; }
        }

        public override string TypeName
        {
            get { return "org.apache.cassandra.db.marshal.AsciiType"; }
        }

        public override Type Type
        {
            get { return AType; }
        }

        public override DbType ToDbType()
        {
            return DbType.AnsiString;
        }

        public override byte[] Serialize(string value)
        {
            return Encoding.ASCII.GetBytes(value);
        }

        public override string Deserialize(byte[] data)
        {
            return Encoding.ASCII.GetString(data);
        }
    }
}