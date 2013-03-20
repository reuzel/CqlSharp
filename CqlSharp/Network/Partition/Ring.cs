using CqlSharp.Network.Partition;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Threading;

namespace CqlSharp.Network
{
    /// <summary>
    /// Ring of nodes, along with their token values
    /// </summary>
    class Ring : IList<Node>
    {
        private readonly List<Node> _nodes;
        private readonly List<IToken> _tokens;
        private Dictionary<IToken, List<Node>> _tokenMap;
        private readonly string _partitioner;
        private readonly ReaderWriterLockSlim _nodeLock;

        /// <summary>
        /// Initializes a new instance of the <see cref="Ring" /> class.
        /// </summary>
        /// <param name="nodes">The nodes.</param>
        /// <param name="partitioner"> </param>
        public Ring(List<Node> nodes, string partitioner)
        {
            _nodes = nodes;
            _tokens = new List<IToken>();
            _partitioner = partitioner;
            _nodeLock = new ReaderWriterLockSlim();

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
        /// Rebuilds the token to node map
        /// </summary>
        private void RebuildMap()
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
            _nodeLock.EnterReadLock();

            try
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
            finally
            {
                _nodeLock.ExitReadLock();
            }
        }

        /// <summary>
        /// Returns an enumerator that iterates through the collection.
        /// </summary>
        /// <returns>
        /// A <see cref="T:System.Collections.Generic.IEnumerator`1"/> that can be used to iterate through the collection.
        /// </returns>
        /// <filterpriority>1</filterpriority>
        public IEnumerator<Node> GetEnumerator()
        {
            int count = _nodes.Count;

            for (int i = 0; i < count; i++)
            {
                Node node = null;
                _nodeLock.EnterReadLock();
                if (i < _nodes.Count)
                    node = _nodes[i];
                _nodeLock.ExitReadLock();

                if (node != null)
                    yield return node;
                else
                    yield break;
            }
        }

        /// <summary>
        /// Returns an enumerator that iterates through a collection.
        /// </summary>
        /// <returns>
        /// An <see cref="T:System.Collections.IEnumerator"/> object that can be used to iterate through the collection.
        /// </returns>
        /// <filterpriority>2</filterpriority>
        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        /// <summary>
        /// Adds an item to the <see cref="T:System.Collections.Generic.ICollection`1"/>.
        /// </summary>
        /// <param name="item">The object to add to the <see cref="T:System.Collections.Generic.ICollection`1"/>.</param><exception cref="T:System.NotSupportedException">The <see cref="T:System.Collections.Generic.ICollection`1"/> is read-only.</exception>
        public void Add(Node item)
        {
            _nodeLock.EnterWriteLock();

            try
            {
                if (!_nodes.Contains(item))
                {
                    _nodes.Add(item);
                    RebuildMap();
                }
            }
            finally
            {
                _nodeLock.ExitWriteLock();
            }
        }

        /// <summary>
        /// Removes all items from the <see cref="T:System.Collections.Generic.ICollection`1"/>.
        /// </summary>
        /// <exception cref="T:System.NotSupportedException">The <see cref="T:System.Collections.Generic.ICollection`1"/> is read-only. </exception>
        public void Clear()
        {
            _nodeLock.EnterWriteLock();

            try
            {
                _nodes.Clear();
                _tokenMap.Clear();
                _tokens.Clear();
            }
            finally
            {
                _nodeLock.ExitWriteLock();
            }
        }

        /// <summary>
        /// Determines whether the <see cref="T:System.Collections.Generic.ICollection`1"/> contains a specific value.
        /// </summary>
        /// <returns>
        /// true if <paramref name="item"/> is found in the <see cref="T:System.Collections.Generic.ICollection`1"/>; otherwise, false.
        /// </returns>
        /// <param name="item">The object to locate in the <see cref="T:System.Collections.Generic.ICollection`1"/>.</param>
        public bool Contains(Node item)
        {
            _nodeLock.EnterReadLock();

            try
            {
                return _nodes.Contains(item);
            }
            finally
            {
                _nodeLock.ExitReadLock();
            }
        }

        /// <summary>
        /// Copies the elements of the <see cref="T:System.Collections.Generic.ICollection`1"/> to an <see cref="T:System.Array"/>, starting at a particular <see cref="T:System.Array"/> index.
        /// </summary>
        /// <param name="array">The one-dimensional <see cref="T:System.Array"/> that is the destination of the elements copied from <see cref="T:System.Collections.Generic.ICollection`1"/>. The <see cref="T:System.Array"/> must have zero-based indexing.</param><param name="arrayIndex">The zero-based index in <paramref name="array"/> at which copying begins.</param><exception cref="T:System.ArgumentNullException"><paramref name="array"/> is null.</exception><exception cref="T:System.ArgumentOutOfRangeException"><paramref name="arrayIndex"/> is less than 0.</exception><exception cref="T:System.ArgumentException">The number of elements in the source <see cref="T:System.Collections.Generic.ICollection`1"/> is greater than the available space from <paramref name="arrayIndex"/> to the end of the destination <paramref name="array"/>.</exception>
        public void CopyTo(Node[] array, int arrayIndex)
        {
            _nodeLock.EnterReadLock();

            try
            {
                _nodes.CopyTo(array, arrayIndex);
            }
            finally
            {
                _nodeLock.ExitReadLock();
            }
        }

        /// <summary>
        /// Removes the first occurrence of a specific object from the <see cref="T:System.Collections.Generic.ICollection`1"/>.
        /// </summary>
        /// <returns>
        /// true if <paramref name="item"/> was successfully removed from the <see cref="T:System.Collections.Generic.ICollection`1"/>; otherwise, false. This method also returns false if <paramref name="item"/> is not found in the original <see cref="T:System.Collections.Generic.ICollection`1"/>.
        /// </returns>
        /// <param name="item">The object to remove from the <see cref="T:System.Collections.Generic.ICollection`1"/>.</param><exception cref="T:System.NotSupportedException">The <see cref="T:System.Collections.Generic.ICollection`1"/> is read-only.</exception>
        public bool Remove(Node item)
        {
            _nodeLock.EnterWriteLock();

            try
            {
                if (_nodes.Remove(item))
                {
                    RebuildMap();
                    return true;
                }

                return false;
            }
            finally
            {
                _nodeLock.ExitWriteLock();
            }
        }

        /// <summary>
        /// Gets the number of elements contained in the <see cref="T:System.Collections.Generic.ICollection`1"/>.
        /// </summary>
        /// <returns>
        /// The number of elements contained in the <see cref="T:System.Collections.Generic.ICollection`1"/>.
        /// </returns>
        public int Count
        {
            get
            {
                _nodeLock.EnterReadLock();

                try
                {
                    return _nodes.Count;
                }
                finally
                {
                    _nodeLock.ExitReadLock();
                }
            }
        }

        /// <summary>
        /// Gets a value indicating whether the <see cref="T:System.Collections.Generic.ICollection`1"/> is read-only.
        /// </summary>
        /// <returns>
        /// true if the <see cref="T:System.Collections.Generic.ICollection`1"/> is read-only; otherwise, false.
        /// </returns>
        public bool IsReadOnly
        {
            get { return false; }
        }

        /// <summary>
        /// Determines the index of a specific item in the <see cref="T:System.Collections.Generic.IList`1"/>.
        /// </summary>
        /// <returns>
        /// The index of <paramref name="item"/> if found in the list; otherwise, -1.
        /// </returns>
        /// <param name="item">The object to locate in the <see cref="T:System.Collections.Generic.IList`1"/>.</param>
        public int IndexOf(Node item)
        {
            _nodeLock.EnterReadLock();

            try
            {
                return _nodes.IndexOf(item);
            }
            finally
            {
                _nodeLock.ExitReadLock();
            }
        }

        /// <summary>
        /// Inserts an item to the <see cref="T:System.Collections.Generic.IList`1"/> at the specified index.
        /// </summary>
        /// <param name="index">The zero-based index at which <paramref name="item"/> should be inserted.</param><param name="item">The object to insert into the <see cref="T:System.Collections.Generic.IList`1"/>.</param><exception cref="T:System.ArgumentOutOfRangeException"><paramref name="index"/> is not a valid index in the <see cref="T:System.Collections.Generic.IList`1"/>.</exception><exception cref="T:System.NotSupportedException">The <see cref="T:System.Collections.Generic.IList`1"/> is read-only.</exception>
        public void Insert(int index, Node item)
        {
            _nodeLock.EnterWriteLock();

            try
            {
                _nodes.Insert(index, item);
                RebuildMap();
            }
            finally
            {
                _nodeLock.ExitWriteLock();
            }
        }

        /// <summary>
        /// Removes the <see cref="T:System.Collections.Generic.IList`1"/> item at the specified index.
        /// </summary>
        /// <param name="index">The zero-based index of the item to remove.</param><exception cref="T:System.ArgumentOutOfRangeException"><paramref name="index"/> is not a valid index in the <see cref="T:System.Collections.Generic.IList`1"/>.</exception><exception cref="T:System.NotSupportedException">The <see cref="T:System.Collections.Generic.IList`1"/> is read-only.</exception>
        public void RemoveAt(int index)
        {
            _nodeLock.EnterWriteLock();

            try
            {
                _nodes.RemoveAt(index);
                RebuildMap();
            }
            finally
            {
                _nodeLock.ExitWriteLock();
            }
        }

        /// <summary>
        /// Gets or sets the element at the specified index.
        /// </summary>
        /// <returns>
        /// The element at the specified index.
        /// </returns>
        /// <param name="index">The zero-based index of the element to get or set.</param><exception cref="T:System.ArgumentOutOfRangeException"><paramref name="index"/> is not a valid index in the <see cref="T:System.Collections.Generic.IList`1"/>.</exception><exception cref="T:System.NotSupportedException">The property is set and the <see cref="T:System.Collections.Generic.IList`1"/> is read-only.</exception>
        public Node this[int index]
        {
            get
            {
                _nodeLock.EnterReadLock();

                try
                {
                    return _nodes[index];
                }
                finally
                {
                    _nodeLock.ExitReadLock();
                }
            }
            set
            {
                _nodeLock.EnterWriteLock();

                try
                {
                    _nodes[index] = value;
                    RebuildMap();
                }
                finally
                {
                    _nodeLock.ExitWriteLock();
                }
            }
        }
    }
}