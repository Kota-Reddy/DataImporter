using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Business.Model;

namespace Business.Interfaces
{
    public interface Iconnect
    {
        public Task<bool> ConnectDb(Connection connection);
    }
}
