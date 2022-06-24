using Business.Model;

namespace Business.Interfaces
{
    public interface ITableDetailsService
    {
        public Task<bool> ConnectDb(DataBaseSchema dataBaseSchema);

    }
}
