using System.Data;
using System.Numerics;

namespace CqlSharp.Serialization.Marshal
{
    public class DecimalType : CqlType<decimal>
    {
        public static readonly DecimalType Instance = new DecimalType();

        public override CqlTypeCode CqlTypeCode
        {
            get { return CqlTypeCode.Decimal; }
        }

        public override string TypeName
        {
            get { return "org.apache.cassandra.db.marshal.DecimalType"; }
        }

        public override DbType ToDbType()
        {
            return DbType.Decimal;
        }

        public override byte[] Serialize(decimal value)
        {
            //get binary representation of the decimal
            int[] bits = decimal.GetBits(value);

            //extract the sign
            bool sign = (bits[3] & 0x80000000) != 0;

            //construct the (signed) unscaled value
            BigInteger unscaled = (uint)bits[2];
            unscaled = (unscaled << 32) + (uint)bits[1];
            unscaled = (unscaled << 32) + (uint)bits[0];
            if (sign) unscaled *= -1;

            //get the unscaled value binary representation (Little Endian)
            var unscaledData = unscaled.ToByteArray();

            //construct the result array
            var rawData = new byte[4 + unscaledData.Length];

            //copy the scale into the rawData
            int scale = (bits[3] >> 16) & 0x7F;
            rawData[0] = (byte)(scale >> 24);
            rawData[1] = (byte)(scale >> 16);
            rawData[2] = (byte)(scale >> 8);
            rawData[3] = (byte)(scale);

            //copy the unscaled value (Big Endian)
            for (int i = 0; i < unscaledData.Length; i++)
                rawData[i + 4] = unscaledData[unscaledData.Length - 1 - i];

            return rawData;
        }

        public override decimal Deserialize(byte[] data)
        {
            //extract scale
            int scale = data.ToInt();

            //check the scale if it ain't too large (or small)
            if (scale < 0 || scale > 28)
                throw new CqlException("Received decimal is too large to fit in a System.Decimal");

            //copy the unscaled big integer data (and reverse to Little Endian)
            var unscaledData = new byte[data.Length - 4];
            for (int i = 0; i < unscaledData.Length; i++)
                unscaledData[i] = data[data.Length - 1 - i];

            //get the unscaled value
            var unscaled = new BigInteger(unscaledData);

            //get the sign, and make sure unscaled data is positive
            bool sign = unscaled < 0;
            if (sign) unscaled *= -1;

            //check unscaled size (Java BigDecimal can be larger the System.Decimal)
            if ((unscaled >> 96) != 0)
                throw new CqlException("Received decimal is too large to fit in a System.Decimal");

            //get the decimal int values
            var low = (uint)(unscaled & 0xFFFFFFFF);
            var mid = (uint)((unscaled >> 32) & 0xFFFFFFFF);
            var high = (uint)((unscaled >> 64) & 0xFFFFFFFF);

            //construct the decimal
            return new decimal((int)low, (int)mid, (int)high, sign, (byte)scale);
        }
    }
}