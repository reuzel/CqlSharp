
namespace CqlSharp.Linq
{
    /// <summary>
    /// CqlBatchTransaction that will remove itself from the context after disposal
    /// </summary>
    class CqlContextBatchTransaction : CqlBatchTransaction
    {
        private readonly CqlDatabase _database;
        private bool _disposed;

        public CqlContextBatchTransaction(CqlDatabase database, CqlConnection connection)
            : base(connection)
        {
            _database = database;
            _disposed = false;
        }

        protected override void Dispose(bool disposing)
        {
            if (!_disposed)
            {
                _disposed = true;

                base.Dispose(disposing);

                if (_database.CurrentTransaction == this)
                    _database.UseTransaction(null);

            }
        }
    }
}
