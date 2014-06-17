using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using System.IO;
using CqlSharp.Protocol;

namespace CqlSharp.Serialization.Marshal
{
    /// <summary>
    /// The definition of a User Defined Type
    /// </summary>
    public class UserDefinedType : CqlType<UserDefined>
    {
        private string _keyspace;
        private string _name;
        List<KeyValuePair<string, CqlType>> _fieldList;
        Dictionary<string, int> _fieldIndexes;

        /// <summary>
        /// Initializes a new instance of the <see cref="UserDefinedType"/> class.
        /// </summary>
        /// <param name="keyspace">The keyspace.</param>
        /// <param name="name">The name.</param>
        /// <param name="fieldNames">The field names.</param>
        /// <param name="types">The field types.</param>
        public UserDefinedType(string keyspace, string name, IEnumerable<string> fieldNames, IEnumerable<CqlType> types)
        {
            _keyspace = keyspace;
            _name = name;

            _fieldList = fieldNames.Zip(types, (n, t) => new KeyValuePair<string, CqlType>(n, t)).ToList();
            
            _fieldIndexes = new Dictionary<string,int>();
            int i = 0;
            foreach (var n in fieldNames)
                _fieldIndexes.Add(n, i++);

        }

        /// <summary>
        /// Gets the keyspace.
        /// </summary>
        /// <value>
        /// The keyspace.
        /// </value>
        public string Keyspace { get { return _keyspace; } }

        /// <summary>
        /// Gets the name.
        /// </summary>
        /// <value>
        /// The name.
        /// </value>
        public string Name { get { return _name; } }

        /// <summary>
        /// Gets the field names.
        /// </summary>
        /// <returns></returns>
        public IEnumerable<string> GetFieldNames()
        {
            return _fieldList.Select(kvp => kvp.Key);
        }

        /// <summary>
        /// Gets the name of the field.
        /// </summary>
        /// <param name="i">The index of the field.</param>
        /// <returns></returns>
        public string GetFieldName(int i)
        {
            return _fieldList[i].Key;
        }

        /// <summary>
        /// Gets the type of the field.
        /// </summary>
        /// <param name="i">The index of the field.</param>
        /// <returns></returns>
        public CqlType GetFieldType(int i)
        {
            return _fieldList[i].Value;
        }

        /// <summary>
        /// Gets the index of the field with the given name.
        /// </summary>
        /// <param name="name">The name.</param>
        /// <returns>zero based index if the field was found, or -1 otherwise</returns>
        public int GetFieldIndex(string name)
        {
            int result;
            if(!_fieldIndexes.TryGetValue(name, out result))
                result = -1;

            return result;
        }

        /// <summary>
        /// Gets the number of fields in this User Defined Type
        /// </summary>
        /// <returns></returns>
        public int GetFieldCount()
        {
            return _fieldList.Count;
        }
        
        public override byte[] Serialize(UserDefined value)
        {
            //calculate array size
            int size = 0;
            for(int i=0; i<_fieldList.Count; i++)
                size+=  4 + (value.RawValues[i]==null? 0 : value.RawValues[i].Length);

            //write all the components
            using(var stream = new MemoryStream(size))
            {
                for(int i=0;i<_fieldList.Count;i++)
                {
                    stream.WriteByteArray(value.RawValues[i]);
                }

                return stream.ToArray();
            }
        }

        public override UserDefined Deserialize(byte[] data)
        {
            var rawValues = new byte[_fieldList.Count][];

            using (var stream = new MemoryStream(data))
            {
                for (int i = 0; i < _fieldList.Count; i++)
                {
                    rawValues[i] = stream.ReadByteArray();
                }
            }

            return new UserDefined(this, rawValues);
        }

        public override CqlTypeCode CqlTypeCode
        {
            get { return CqlTypeCode.Custom; }
        }

        public override string TypeName
        {
            get 
            {
                StringBuilder sb = new StringBuilder("org.apache.cassandra.db.marshal.UserType(");
                sb.Append(_keyspace);
                sb.Append(',');
                sb.Append(_name.EncodeAsHex());
                foreach(var field in _fieldList)
                {
                    sb.Append(',');
                    sb.Append(field.Key.EncodeAsHex());
                    sb.Append(':');
                    sb.Append(field.Value.TypeName);
                }
                sb.Append(')');

                return sb.ToString();
            }
        }
              
        public override DbType ToDbType()
        {
            return DbType.Object;
        }
    }
}
