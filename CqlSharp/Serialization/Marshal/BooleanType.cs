using System.Data;

namespace CqlSharp.Serialization.Marshal
{
    public class BooleanType : CqlType<bool>
    {
        public static readonly BooleanType Instance = new BooleanType();

        public override CqlTypeCode CqlTypeCode
        {
            get { return CqlTypeCode.Boolean; }
        }

        public override string TypeName
        {
            get { return "org.apache.cassandra.db.marshal.BooleanType"; }
        }

        public override DbType ToDbType()
        {
            return DbType.Boolean;
        }

        public override byte[] Serialize(bool value)
        {

            return new[] { value ? (byte)1 : (byte)0 };
        }

        public override bool Deserialize(byte[] data)
        {
            return data[0] > 0;
        }
    }
}