using System;

namespace CqlSharp.Network
{
    public class LoadChangeEvent : EventArgs
    {
        public int LoadDelta { get; set; }
    }
}