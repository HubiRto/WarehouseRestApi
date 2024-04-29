using System.Data;
using MySqlConnector;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

var app = builder.Build();

if (app.Environment.IsDevelopment())
{
    app.UseSwagger();
    app.UseSwaggerUI();
}

app.UseHttpsRedirection();

var connectionString = builder.Configuration.GetConnectionString("DefaultConnection");

app.MapPost("/addProductToWarehouse", async (InventoryRequest request) =>
{
    await using var conn = new MySqlConnection(connectionString);
    await conn.OpenAsync();
    await using var transaction = await conn.BeginTransactionAsync();

    try
    {
        if (request.Amount <= 0)
        {
            return Results.BadRequest("Ilość musi być większa niż 0.");
        }

        if (!await CheckIfProductExists(request.IdProduct, conn) ||
            !await CheckIfWarehouseExists(request.IdWarehouse, conn))
        {
            return Results.NotFound("Produkt lub magazyn nie istnieje.");
        }

        var orderId = await CheckOrderExists(request, conn);
        if (orderId == -1)
        {
            return Results.BadRequest("Nie istnieje odpowiednie zamówienie lub zamówienie już zrealizowane.");
        }

        await UpdateOrderFulfilledAt(orderId, conn);
        var productWarehouseId = await InsertProductWarehouse(request, orderId, conn);

        await transaction.CommitAsync();
        return Results.Ok(new { ProductWarehouseId = productWarehouseId });
    }
    catch (Exception ex)
    {
        await transaction.RollbackAsync();
        return Results.Problem(ex.Message);
    }
});

app.MapPost("/addProductToWarehouseWithProc", async (InventoryRequest request) =>
{
    await using var conn = new MySqlConnection(connectionString);
    await conn.OpenAsync();
    await using var cmd = new MySqlCommand("AddProductToWarehouse", conn);
    cmd.CommandType = CommandType.StoredProcedure;
    cmd.Parameters.AddWithValue("IdProduct", request.IdProduct);
    cmd.Parameters.AddWithValue("IdWarehouse", request.IdWarehouse);
    cmd.Parameters.AddWithValue("Amount", request.Amount);
    cmd.Parameters.AddWithValue("CreatedAt", request.CreatedAt);

    try
    {
        var result = await cmd.ExecuteScalarAsync();
        return Results.Ok(new { ProductWarehouseId = result });
    }
    catch (MySqlException ex)
    {
        return Results.Problem(detail: ex.Message, statusCode: 500);
    }
});

app.Run();
return;

async Task<bool> CheckIfProductExists(int productId, MySqlConnection conn)
{
    await using var cmd = new MySqlCommand("SELECT COUNT(*) FROM Product WHERE IdProduct = @id", conn);
    cmd.Parameters.AddWithValue("@id", productId);
    var result = (long)(await cmd.ExecuteScalarAsync())!;
    return result > 0;
}

async Task<bool> CheckIfWarehouseExists(int warehouseId, MySqlConnection conn)
{
    await using var cmd = new MySqlCommand("SELECT COUNT(*) FROM Warehouse WHERE IdWarehouse = @id", conn);
    cmd.Parameters.AddWithValue("@id", warehouseId);
    var result = (long)(await cmd.ExecuteScalarAsync())!;
    return result > 0;
}

async Task<int> CheckOrderExists(InventoryRequest request, MySqlConnection conn)
{
    await using var cmd = new MySqlCommand("""
                                           SELECT IdOrder FROM OrderTable
                                                                            WHERE IdProduct = @idProduct AND Amount >= @amount
                                                                            AND CreatedAt <= @createdAt AND FulfilledAt IS NULL
                                           """, conn);
    cmd.Parameters.AddWithValue("@idProduct", request.IdProduct);
    cmd.Parameters.AddWithValue("@amount", request.Amount);
    cmd.Parameters.AddWithValue("@createdAt", request.CreatedAt);
    var reader = await cmd.ExecuteScalarAsync();
    return reader != null ? Convert.ToInt32(reader) : -1;
}

async Task UpdateOrderFulfilledAt(int orderId, MySqlConnection conn)
{
    await using var cmd = new MySqlCommand("UPDATE OrderTable SET FulfilledAt = NOW() WHERE IdOrder = @id", conn);
    cmd.Parameters.AddWithValue("@id", orderId);
    await cmd.ExecuteNonQueryAsync();
}

async Task<int> InsertProductWarehouse(InventoryRequest request, int orderId, MySqlConnection conn)
{
    await using var cmd = new MySqlCommand("""
                                           INSERT INTO Product_Warehouse
                                                                            (IdWarehouse, IdProduct, IdOrder, Amount, Price, CreatedAt)
                                                                            VALUES (@idWarehouse, @idProduct, @idOrder, @amount,
                                                                                    (SELECT Price * @amount FROM Product WHERE IdProduct = @idProduct),
                                                                                    NOW())
                                           """, conn);
    cmd.Parameters.AddWithValue("@idWarehouse", request.IdWarehouse);
    cmd.Parameters.AddWithValue("@idProduct", request.IdProduct);
    cmd.Parameters.AddWithValue("@idOrder", orderId);
    cmd.Parameters.AddWithValue("@amount", request.Amount);
    await cmd.ExecuteNonQueryAsync();
    return (int)cmd.LastInsertedId;
}

internal abstract record InventoryRequest(int IdProduct, int IdWarehouse, int Amount, DateTime CreatedAt);