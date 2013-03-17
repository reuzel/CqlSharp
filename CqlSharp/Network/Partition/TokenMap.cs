using System;
using System.Collections.Generic;

namespace CqlSharp.Network.Partition
{
    /// <summary>
    /// Map enabling token to node searches
    /// </summary>
    class TokenMap
    {
        private Dictionary<IToken, List<Node>> _tokenMap;
        private readonly List<Node> _nodes;
        private readonly List<IToken> _tokens;
        private readonly string _partitioner;

        /// <summary>
        /// Initializes a new instance of the <see cref="TokenMap" /> class.
        /// </summary>
        /// <param name="nodes">The nodes.</param>
        /// <param name="partitioner"> </param>
        public TokenMap(List<Node> nodes, string partitioner)
        {
            _nodes = nodes;
            _tokens = new List<IToken>();
            _partitioner = partitioner;

            RebuildMap();
        }

        /// <summary>
        /// Creates a token
        /// </summary>
        /// <returns></returns>
        private IToken CreateToken()
        {
            IToken token;
            if (_partitioner.EndsWith("Murmur3Partitioner", StringComparison.InvariantCultureIgnoreCase))
                token = new MurmurToken();

            else if (_partitioner.EndsWith("RandomPartitioner", StringComparison.InvariantCultureIgnoreCase))
                token = new MD5Token();

            else if (_partitioner.EndsWith("OrderedPartitioner", StringComparison.InvariantCultureIgnoreCase))
                token = new ByteArrayToken();

            else
                return null;

            return token;
        }

        /// <summary>
        /// Rebuilds the map.
        /// </summary>
        public void RebuildMap()
        {
            _tokens.Clear();
            _tokenMap = new Dictionary<IToken, List<Node>>();

            foreach (var node in _nodes)
            {
                foreach (var tokenString in node.Tokens)
                {
                    IToken token = CreateToken();

                    //unknown partitioner
                    if (token == null)
                        return;

                    token.Parse(tokenString);
                    _tokens.Add(token);

                    if (!_tokenMap.ContainsKey(token))
                    {
                        _tokenMap.Add(token, new List<Node>());
                    }

                    _tokenMap[token].Add(node);
                }
            }

            _tokens.Sort();
        }

        /// <summary>
        /// Gets the responsible nodes.
        /// </summary>
        /// <param name="key">The partition key</param>
        /// <returns></returns>
        public List<Node> GetResponsibleNodes(PartitionKey key)
        {
            //get a token
            IToken token = CreateToken();

            //unknown partitioner
            if (token == null)
                return null;

            //parse the key into a token value
            token.Parse(key.Key);

            // Find the primary replica
            int i = _tokens.BinarySearch(token);
            if (i < 0)
            {
                //not found, searh resulted in -(first index larger than token)
                //get first smaller than token
                i = ~i - 1;

                //correct any boundary mistakes
                if (i >= _tokens.Count || i < 0)
                    i = 0;
            }

            //return the list of nodes assigned to this token (or best guess anyway)
            return _tokenMap[_tokens[i]];
        }
    }
}